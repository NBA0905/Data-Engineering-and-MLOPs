# Predicting chess blunders live: an ML pipeline on Lichess data

**Data Engineering & MLOps, final assignment**

David Camargo-Maddin (557784) & Bao Anh Nguyen (545104)

## 1. Summary

We built a pipeline that predicts, while a game is being played, whether the player about to move is about to blunder. The model learns from historical Lichess games that already carry Stockfish evaluations, so we can label millions of moves without running an engine ourselves. It is retrained every six hours on newly replayed games and scores the live Lichess TV feed in real time, without any engine in the live path.

Every required tool has a clear job:

| Requirement | How we meet it |
|---|---|
| Orchestrated by Airflow | DAG `train_blunder_model` runs every 6 hours: replay → bronze → parse → Spark → train → evaluate/promote. DAG `opening_book` loads the reference data |
| Kafka for ingestion | Redpanda (Kafka API). Topics `tv.moves` (live stream) and `games.archive` (replayed history). All game data enters through Kafka |
| Spark for cleaning and combining | A Spark job cleans the parsed moves (silver) and joins the opening book, then computes per-game window features (gold) |
| Data stored on RustFS | S3-compatible lake with `bronze`, `silver`, `gold` and `mlflow` buckets |
| Periodic (re)training tracked in MLflow | Every DAG run trains a challenger, logs parameters, metrics and the model, and promotes it to `champion` only if it beats the current champion |
| Dashboard | Streamlit with three pages: live board with blunder risk per player, data insights from gold, model history from MLflow |
| ≥ 2 related sources, ≥ 1 streaming | Lichess TV feed (streaming), Lichess monthly archive (replayed), eco.json opening book (external). Linked by position (FEN) and game id |

On the newest held-out games the model reaches a **PR-AUC of 0.181**, against **0.087** for a clock-only model and **0.070** for guessing the base rate: about 2× better than the clock baseline.

## 2. The problem and why we chose it

A blunder is a move that throws away a large part of the player's winning chances. We wanted a question that is not obvious, needs several data sources and has a real live component. Chess fits well:

- **Labels come for free.** Lichess publishes every rated game, and about 10% of them carry Stockfish evaluations per move. Two consecutive evaluations tell us exactly how much a move cost. We can train a real model in week one instead of collecting data for weeks first (our alternative idea, a Steam player-count project, would have needed exactly that).
- **There is a genuine live stream.** Lichess TV streams the featured game of each speed category, move by move.
- **The live side cannot use an engine.** Scoring every live move with Stockfish would be far too slow. So the model must predict from engine-free features only, which makes the problem harder and more interesting than just reading an evaluation.

**Label.** We convert the engine score (centipawns) to a win probability, `wp = 1 / (1 + e^(-0.00368208 · cp))`, the formula Lichess uses, and call a move a blunder when the mover's win probability drops by **at least 0.15**. Win probability matters because a 300-centipawn drop is a disaster in a level position but irrelevant when already a queen up. Mate scores are clamped to ±1000 centipawns so they do not distort the distribution. We chose the threshold by comparing against Lichess's own `??` annotations on a trial set:

| Win-probability drop | Moves labelled blunder | Lichess `??` caught | Labelled moves that are `??` |
|---|---|---|---|
| 0.10 | 10.9% | 90% | 44% |
| **0.15** | **7.1%** | **84%** | **64%** |
| 0.20 | 5.0% | 77% | 83% |
| 0.30 | 2.8% | 52% | 100% |

0.15 catches most real blunders while keeping the label reproducible (Lichess's marks are not a pure threshold). The `??` marks are kept for validation only, never as a feature or label.

## 3. Data sources and how they relate

| Source | Type | What we use | Link |
|---|---|---|---|
| **Lichess TV feed** `lichess.org/api/tv/{channel}/feed` | Streaming (NDJSON) | Live positions, last move and clocks for the bullet, blitz, rapid and classical channels | game id, FEN |
| **Lichess monthly archive** `database.lichess.org` | Static file, replayed as a stream | Training corpus: games with per-move `[%eval]` and `[%clk]` | game id, FEN |
| **eco.json opening book** (hayatbiralem/eco.json, pinned commit) | Static reference data from outside Lichess | 15,373 named opening positions with ECO code and name | FEN |

**The relationship.** Every position, from the archive or from the live feed, is looked up in the opening book on a normalised FEN key (piece placement, side to move and castling rights; the move counters are dropped because the same position can be reached in a different move order). This gives three features per move: whether the position is still known opening theory, how many moves ago the game left the book, and the ECO family of the last book position. The relationship is clearly visible in the data: **players blunder on 0.5% of moves while in book versus 7.5% once out of it.**

**Archive slice.** A monthly file is about 29 GB. We download only the first 3 GB with an HTTP range request. A zstd stream can be decoded from the start of a cut-off file, so the parser simply stops at the last complete game. Of the first 50,000 games, 10.7% carry evaluations, so the slice holds hundreds of thousands of usable games, far more than the project needs. Both team members use the same slice, verified by SHA-256.

**What the TV feed does not send.** The feed has no game id on move events, no time control, and joins games halfway. Our producer therefore remembers the current game per channel from the `featured` event, stamps it on every move, and asks the Lichess export API **once per game** for the time control and the moves played so far.

## 4. Architecture

There are **two lanes that only meet in two places**: RustFS (data and the opening book) and MLflow (the registered model).

- The **batch lane** replays history through Kafka into the lake, builds the feature table with Spark, trains a challenger and promotes it if it is better. Airflow schedules it; nothing in it is live.
- The **live lane** reads the TV feed, computes the same features for the position on the board right now, asks the champion model for a probability and writes it to Postgres for the dashboard. It runs as long-lived Docker services, not as Airflow tasks.

Everything runs in one `docker-compose.yml` on a laptop.

| Service | Port | Role |
|---|---|---|
| Redpanda + Console | 19092 · 8085 | Kafka API · topic browser |
| RustFS | 9000 · 9001 | S3 API · web console |
| Postgres + pgAdmin | 5432 · 5050 | Airflow and MLflow metadata, replay checkpoint, predictions |
| MLflow | 5000 | Tracking server and model registry |
| Airflow (api-server, scheduler, dag-processor) | 8080 | Orchestration of the batch lane |
| Spark master · worker · connect | 8088 · 8089 · 15002 (jobs UI 4040) | Spark cluster, used through Spark Connect |
| tv-producer · live-scorer · tv-lander | – | Live lane |
| Streamlit dashboard | 8501 | Dashboard |

## 5. Each step and why we built it this way

### 5.1 Ingestion with Kafka (Redpanda)

**What.** Two topics:

- `tv.moves`: one message per live move, keyed by game id, 4 partitions, 24-hour retention.
- `games.archive`: one message per replayed archive game (raw PGN plus batch number and sequence number), keyed by game id.

**Why Redpanda.** It implements the Kafka API, so producers and consumers use the standard `confluent-kafka` client unchanged. It runs as a single binary without ZooKeeper or a JVM, needs far less memory than Apache Kafka (which matters on a laptop running fourteen containers), and is what the course used.

**Why Kafka at all.** It decouples producers from consumers. The TV producer only ingests. The scorer and the raw-event lander read the same topic independently, each with their own consumer group. If the scorer is briefly slower (for example while loading a new model), moves wait in the topic instead of being lost.

**Keys and partitions.** The game id is the message key, so all moves of one game land in the same partition and stay in order, which the scorer needs because its features depend on earlier moves. Four partitions allow up to four scorers in one consumer group if the stream ever needs scaling.

**Retention.** Kafka is a buffer, not storage: `tv.moves` keeps messages for 24 hours. Long-term data lives in RustFS (raw live events in `bronze/tv/`).

### 5.2 The archive replay: "always send the next new data"

The assignment requires a simulation program that sends data to Kafka and continues where it stopped. Our **archive replay producer** (`ingest/replay_archive.py`) reads the next batch of annotated games from the archive file and publishes them to `games.archive`.

- **Checkpoint in Postgres.** The table `archive_checkpoint` stores the byte offset in the decompressed stream, the games scanned and sent, and the batch number. Each run resumes from that offset, so a game is never sent twice by design.
- **Backfill, then batches.** The first run sends 20,000 annotated games (enough for a meaningful first model); every run after that sends 5,000. Both are Airflow parameters, so a demo run can use a smaller batch.
- **At-least-once.** The checkpoint is saved only after Redpanda has confirmed every message. If a run fails halfway, the next run sends the same games again; duplicates are removed downstream in Spark.

Because each run adds new games, the periodic retraining actually sees new data.

### 5.3 Landing in the lake (RustFS)

**What.** RustFS is an S3-compatible object store, our data lake, organised in layers:

| Bucket | Contents | Written by |
|---|---|---|
| `bronze` | `archive/`: replayed games exactly as received from Kafka · `moves/`: one row per move with board features · `tv/`: raw live events · `eco/<commit>/`: eco.json as downloaded | land_archive, parse_bronze, tv-lander, opening_book DAG |
| `silver` | `moves/`: cleaned moves (bots and UltraBullet removed, each game once) | Spark |
| `gold` | `features/`: the model-ready feature table · `openings/`: the normalised opening book | Spark, opening_book DAG |
| `mlflow` | model files and plots | MLflow server |

**Why layers.** Bronze is kept raw, so any later step can be rerun from it after a bug fix or a new feature, without downloading or replaying anything again. Silver is the cleaned version, gold is what the model and the dashboard read. The opening book goes straight from bronze to gold: it is small static reference data whose cleaning step directly produces its final lookup form, so a silver copy would be identical.

**Landing from Kafka.** `ingest/land_archive.py` reads every message the consumer group has not landed yet, writes them as Parquet to `bronze/archive/`, and commits the Kafka offsets **only after** the upload succeeded. A failed run lands the same messages again into the same file name.

**Why Parquet.** Columnar and compressed: 3.9 million parsed moves take 210 MB, and readers fetch only the columns they need, which is what the training script and the dashboard do. It also stores column types, unlike CSV.

### 5.4 Parsing and board features (Python, before Spark)

`ingest/parse_bronze.py` turns each landed game into one row per move: ratings, time control, clocks, the engine evaluation before and after, the label, and **13 board features** computed with python-chess (material balance, legal moves, pieces attacked, pieces en prise, pinned pieces, attacks around the king, captures and checks available, and more).

**Why this is not in Spark.** These features need python-chess position by position. Running that row by row inside Spark would mean Python UDFs serialising tens of millions of positions, which is very slow, and it would require python-chess on the Spark workers. So Python does the parsing (with a few processes, in chunks of 2,000 games to keep memory flat), and Spark does the columnar work it is good at. Only files that have no moves file yet are parsed.

### 5.5 Cleaning and combining with Spark

**What.** `spark/build_features.py`, run from Airflow through **Spark Connect** against a standalone cluster (master, worker, connect server), reads `bronze/moves/` and `gold/openings/`:

1. **Cleaning → silver.** Drops bot games and UltraBullet (bots play differently, and UltraBullet never appears on TV), and keeps only the first copy of a game that was sent twice (Kafka delivers at least once).
2. **Combining → gold.** Joins every move with the opening book on the normalised FEN key, then computes the features that depend on earlier moves in the same game with **window functions** (partition by game, order by position):

| Feature | Spark technique |
|---|---|
| `plies_out_of_book`, `eco_family` | max / last book position over the game so far |
| `material_drawdown` | best material balance so far, per side |
| `material_change` | material four positions ago, looked up with a join on position − 4 |
| `prev_time_spent`, `recent_time_avg` | lag and 3-move average over each player's own moves |

**Why Spark Connect.** It is the setup from the course. Airflow tasks only need the small `pyspark-client` package and talk to the cluster over the network, so the Airflow image does not need Java or a full Spark install. The Spark UI shows every job, which helps during the demo.

**Correctness: one feature definition for both lanes.** The live scorer computes the same sequential features move by move with a small state object per game (`features.game.GameState`), while training uses the Spark windows. If the two disagreed, the model would silently get different inputs live than it was trained on. A **parity test** (`tests/test_spark_features.py`) generates games with a real opening, a missing evaluation, a bot game and a duplicate, computes the features both ways, and checks that all 26 are identical.

**Details worth mentioning.**

- `null.isin(...)` is null in Spark, not false, so the title filter uses `coalesce(title, "")`; without it, every untitled game would be dropped.
- The material lookback is a join on position − 4 rather than `lag(4)`, so a move without an evaluation (a missing row) does not shift the window.
- `spark.sql.shuffle.partitions` is set to 8 instead of 200, as in the course notebooks. With two worker cores, 200 partitions wrote around 200 small files per table, which slowed Spark down and caused read timeouts in training. Adaptive query execution then writes gold as about four files of 11 MB.
- The connect server has a larger driver heap, exits and restarts on out-of-memory instead of staying broken, and releases the worker's memory after 15 seconds idle, so the training task that follows has room.

**Result on our data:** 29,683 games and 1.93 million moves in silver and gold, built in 1 minute 45 seconds.

### 5.6 Orchestration with Airflow

**DAG `train_blunder_model`** (every 6 hours, `catchup=False`, `max_active_runs=1`):

```
replay_archive → land_in_bronze → parse_moves → spark_features → train_model → evaluate_model
```

**DAG `opening_book`** (`@once`): downloads eco.json from a pinned commit into `bronze/eco/`, then builds the lookup table into `gold/openings/`.

**Why this shape.**

- **Airflow only runs jobs with a start and an end.** The TV producer, the scorer and the lander never finish, so they are Docker services with `restart: unless-stopped`, not Airflow tasks.
- **Each task runs one of our scripts**, the same command we can run by hand (`python -m ...`). The DAG only decides the order and the parameters, which keeps the logic testable outside Airflow.
- **Every task is safe to rerun:** the checkpoint, the offset commit after upload, "skip files already parsed" and Spark's overwrite all make a retry harmless. On top of that, tasks retry automatically on transient errors (for example a Kafka coordinator that is not ready yet), and the Spark task has an execution timeout, so a hang fails instead of blocking the pipeline.
- **Every six hours with `catchup=False`.** Our laptops are not on day and night; this gives two or three retrains per working day without a storm of catch-up runs after the laptop was off. Parameters (`games`, `backfill`, `test_parts`, `train_parts`) can be changed when triggering by hand.
- **LocalExecutor instead of Celery.** One machine, so no Redis or worker containers are needed. It saves memory and is a standard Airflow setting. Airflow shares the Postgres server with MLflow and the predictions.

### 5.7 Training, tracking and the model registry (MLflow)

**Model.** `HistGradientBoostingClassifier` from scikit-learn on **26 engine-free features**: rating and rating gap, time control and increment, both clocks, time spent on the previous move, move number, material and piece count, check, legal moves, the three opening-book features, eight threat features and three game-history features. Gradient boosting handles mixed features and missing values (for example no previous move yet) without scaling. In the notebook, class weights did not improve PR-AUC and badly hurt calibration, so we train without them: the probabilities can be shown as they are.

**Evaluation.** Blunders are about 7% of moves, so accuracy is meaningless (always predicting "no blunder" is 93% accurate). We report **PR-AUC**, plus ROC-AUC and the Brier score, against two baselines logged in every run: the base rate and a clock-only model. The split is **by time**: the newest parts (2,000 games each, in archive order) are the test set and are never trained on in that run. A random split would leak games from the same period into training and flatter the metrics.

| On the newest games | PR-AUC |
|---|---|
| Guessing (base rate) | 0.070 |
| Clock only | 0.087 |
| **Our model** | **0.181** |

The exploratory notebook (60,000 games, test set of 11,854 games) found the same picture: 0.177 against 0.086, and the gain holds for every speed. The threat features add the most (+12% PR-AUC); the opening and history features add little on top, mostly because that information is already in material and clocks. Moves Lichess marked `??` receive the highest average predicted risk, then `?`, then `?!`, then unmarked moves, even though the model never saw those marks. Flagging the riskiest 5% of moves catches 17% of all blunders at 23% precision, 3.3× the base rate. It is a risk score, not a blunder detector, which is what a live indicator needs.

**Tracking.** Every run logs its parameters (model settings, training window, number of moves), metrics (test PR-AUC, ROC-AUC, Brier, base rate, clock-only PR-AUC, lift over clock) and the model itself, with its feature list as a tag. The MLflow server stores run data in Postgres and artifacts in the RustFS `mlflow` bucket; clients upload through the server, so they need no storage credentials.

**Registry and promotion.** Each training run registers a new version of model `blunder` with the alias **`challenger`**. `evaluate_model` then scores the challenger and the current **`champion`** on the **same** newest test games and moves the `champion` alias only if the challenger's PR-AUC is higher. The comparison is logged as its own run. The live scorer always loads `models:/blunder@champion`, so a better model reaches production without any manual step.

### 5.8 The live lane: scoring the TV feed

- **`tv-producer`** opens one streaming connection per channel (bullet, blitz, rapid, classical) in its own thread, stamps the current game on every move, adds the time control from one export call per game, and publishes to `tv.moves`. It reconnects automatically if Lichess drops the connection.
- **`live-scorer`** reads `tv.moves`, keeps one `GameState` per game (reset when a new game starts on a channel), builds the board from the FEN in each message, computes the 26 features with the **same `features` module** used for training, and asks the champion model for the probability that the player to move blunders. Each prediction is written to the `predictions` table in Postgres.
- **`tv-lander`** writes the raw TV events to `bronze/tv/`, so all data lands in the lake and can be reprocessed later.

**Why it keeps up, even with bullet.** Because no engine is involved, scoring a move takes milliseconds: one feature vector and a few hundred small trees. Most of the delay between a move on Lichess and the dashboard is the dashboard's one-second refresh.

**Edge cases handled.** Move events carry no game id (the producer stamps it). Games switch without warning (the `featured` event resets the state, and the dashboard always follows the newest game per channel). The export API can be one move ahead of the feed (the scorer trusts the FEN in the message). Lichess TV writes castling as king-takes-rook (`e1h1`), another reason to build the board from the FEN.

### 5.9 The dashboard (Streamlit)

Three pages:

- **Live.** The current game on the chosen channel, drawn with `chess.svg`, with a vertical risk bar for each player. A bar turns orange above 0.157 and red above 0.258, the levels that only the riskiest 10% and 1% of moves reach. A chart below shows both players' risk per move. Only this part refreshes, every second (`st.fragment`), straight from Postgres.
- **Data.** Blunder rate by time left on the clock, by moves since leaving the opening book, by speed and by rating, read from `gold/features` in RustFS (cached for ten minutes, only the needed columns).
- **Model.** The current champion, its PR-AUC next to both baselines, PR-AUC across every retrain, and a table of all finished training runs, read from MLflow.

## 6. Quality and robustness

| Measure | Where |
|---|---|
| Unit tests for the parser, opening book, features, replay producer and TV producer (45 tests) | `tests/` |
| Parity test: Spark features equal the live `GameState` features | `tests/test_spark_features.py` |
| At-least-once delivery with duplicates removed downstream | replay checkpoint after confirmation, offset commit after upload, Spark dedup |
| Every task rerunnable; retries and a Spark timeout | Airflow DAG |
| Fixed schemas so empty columns do not break Parquet files | parse and landing steps |
| Champion/challenger promotion on the same test set | `training/evaluate_model.py` |
| Reproducible inputs | pinned eco.json commit, archive slice verified by SHA-256, `uv.lock` |
| Containers restart themselves; processes are reaped (`init: true`) | `docker-compose.yml` |

Besides the required tools, the pipeline also includes the MLflow model registry with automatic promotion, several Airflow operator types (TaskFlow tasks, the bash decorator, typed `Param`s), Spark Connect on a standalone cluster, and a combination of three data sources: a live stream, a replayed archive and an external reference set.

## 7. Limitations and next steps

- **Spark reprocesses all of bronze on every run.** Simple and always correct, and fast enough now (under two minutes for 30,000 games), but it grows with the data. The next step is to process only new batches and append to silver and gold.
- **No live accuracy check.** We cannot know live whether a predicted risk turned into a blunder, because that needs an engine. A low-frequency DAG could fetch Lichess's analysis of finished TV games afterwards and compare it with the live predictions.
- **The lake keeps growing.** About 50 MB per run of 5,000 games. Fine for this project; in production we would add a retention rule for old bronze files.
- **One laptop.** Fourteen containers fit, but only just. We bring services up in stages while developing, keep the DAG paused unless we need runs, and run Spark with dynamic allocation.
- **Training/serving versions.** The model is trained inside the Airflow image; the live image must use the same scikit-learn version to load it. A shared base image would remove that risk.

## 8. Running it

```powershell
uv sync
docker compose up -d --build
```

Then, once: trigger the `opening_book` DAG, then unpause or trigger `train_blunder_model` (the first run does the 20,000-game backfill and creates the first champion). The live lane starts by itself; the dashboard is at http://localhost:8501. Setup details, including downloading the archive slice, are in the README.