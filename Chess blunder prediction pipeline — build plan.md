# Chess blunder prediction pipeline — build plan

Sep 30, 2026 · @Alan

## What we're building

A pipeline that predicts, live, whether the player about to move in an online chess game is about to blunder. The model is trained on millions of historical Lichess games that already carry Stockfish evaluations, and scores positions on a live feed as they are played.

We picked this over a Steam playerbase project for one practical reason: the Lichess archive already contains millions of labelled training examples, so we can train a real model in week one. A Steam project would require three weeks of self-collected polling before any model work could start, which blocks the whole assignment behind data collection.

## Data sources

Three sources, all free, none requiring authentication. The assignment asks for at least two different sources, at least one of them streaming, with a relationship between them. The TV feed is the streaming source, eco.json is the static source from outside Lichess, and FEN is the join key that links them.

| Source | Role | Endpoint or location | Join key |
| --- | --- | --- | --- |
| Lichess TV feed | Streaming | `lichess.org/api/tv/{channel}/feed` (NDJSON) | game id, FEN |
| Lichess monthly archive | Training corpus, replayed through Redpanda in batches | [database.lichess.org](https://database.lichess.org) (PGN, zstd) | game id, FEN |
| eco.json opening book | Static feature lookup | [hayatbiralem/eco.json](https://github.com/hayatbiralem/eco.json) (`ecoA.json` … `ecoE.json`, MIT) | FEN |

The TV feed covers the bullet, blitz, rapid and classical channels. One connection per channel gives six to eight simultaneous games. Each channel sends a `featured` event when a game starts, carrying both players' usernames and ratings, then a stream of `fen` events with the position, last move and both clocks.

What the feed actually sends (checked against a 30 second capture on Oct 28, sample in `data/samples/`):

- `featured`: `{"t":"featured","d":{"id","orientation","players":[{"color","user":{"name","id"},"rating","seconds"}],"fen"}}`
- `fen`: `{"t":"fen","d":{"fen","lm","wc","bc"}}`. `lm` is the last move in UCI notation, `wc` and `bc` are the remaining clocks in whole seconds.

Three consequences:

- **Move events carry no game id.** The producer must remember the current game per channel and stamp it on every message.
- **There is no time control or increment in the feed.**
- **The feed joins games midway** (the capture started at move 23), so the earlier moves are missing. Without them we cannot compute `plies_out_of_book` or the time spent on the previous move.

Fix for the last two: on each `featured` event, the producer calls `GET lichess.org/api/game/{id}` once. That returns the clock settings and the moves played so far. One call per game, not per move, so it stays out of the hot path.

The monthly archive is the training corpus. Roughly 6% of games carry `[%eval ...]` and `[%clk ...]` annotations per move. We need around 60,000 annotated games, which means reading about one million — roughly 1% of a single monthly file.

A monthly file is about 29 GB, but over the whole project we replay roughly 220k annotated games (about 4% of a month). We therefore download only the first 3 GB with an HTTP range request (`curl.exe -r 0-3221225471`). A zstd stream can be decoded from the start of a cut-off file, so the parser simply stops at the last complete game.

Measured on Oct 28: 10.7% of the first 50,000 games in the September 2026 file carry `[%eval`, higher than the 6% we assumed. Scanning cost per replayed batch is correspondingly lower.

The archive does not go straight to storage. An **archive replay producer** reads annotated games from the `.pgn.zst` file and publishes them to the `games.archive` topic in batches, so all game data is ingested through Redpanda. The producer stores its position (byte offset and game count) in a checkpoint table in Postgres and resumes from there on the next run, so every run sends the next new games, never the same ones again. Because the training set grows with every run, periodic retraining actually sees new data.

eco.json is the third source and makes the join requirement real. It holds 12,000+ named opening positions, each keyed by FEN, with ECO code, opening name and move sequence. Joining every position on FEN tells us whether the player is still in known opening theory, and if not, how many plies ago they left it. A player still in book is in known territory; one who left theory ten plies ago is improvising. The archive's PGN headers only give one opening name per game. eco.json gives us that information for every position, and the live feed carries no opening data at all.

Two practical notes on eco.json:

- **The FEN keys are full six-field FENs** (placement, side to move, castling, en passant, halfmove clock, move number). The move counters differ when the same position is reached by a different move order, so we join on a normalised key: the first three fields only. The normalisation function lives in the shared feature module so both lanes compute it identically.
- **The repository is archived** (read-only; active development moved to JeffML/eco.json). We pin a specific commit when downloading, so the lookup never changes underneath us.

The lookup is small (about 12k rows), so we load it once into RustFS during the batch lane. Spark joins it, and the live consumer loads it into memory at startup. No external API call at any point.

## Architecture

&#91;embedded content: pipeline architecture · two lanes, ten components\]

This is the part that makes the project click. There are two data paths, not one.

The batch lane replays history through Redpanda into RustFS, builds a feature table with Spark, trains a model and registers it. Nothing about it is live. The live lane reads the TV feed, computes the same features for the position on the board right now, asks the registered model for a probability and records the answer.

They touch in exactly two places: RustFS, where the training data, the opening book and the raw live events sit, and MLflow, where the trained model is registered and the live consumer picks it up.

RustFS layout:

| Layer | Path | Contents |
| --- | --- | --- |
| Bronze | `bronze/archive/`, `bronze/tv/`, `bronze/eco/` | Raw replayed games and raw TV feed events as received from Redpanda; eco.json files as downloaded |
| Silver | `silver/moves/` | Cleaned moves: bots and unrated games removed, deduplicated, labels computed |
| Gold | `gold/features/`, `gold/openings/` | Model-ready feature table; normalised opening book (FEN key, ECO code, name) |

Redpanda topics: `games.archive` (replayed archive games), `tv.moves` (live move events, keyed by game id).

## What each tool does

| Tool | Role in this project |
| --- | --- |
| Airflow | Schedules the batch lane only: replay the next archive batch, load the opening book, Spark features, train, evaluate, promote |
| Redpanda | Kafka-compatible ingestion for all game data: replayed archive games and live feed events. Decouples producers from consumers |
| Spark | Cleaning, combining (opening book join on FEN), columnar feature engineering and windowing within games |
| RustFS | S3-compatible data lake, bronze / silver / gold, for archive, opening book and live data |
| MLflow | Tracks runs (backend in Postgres, artifacts on RustFS), holds the model registry the live consumer reads from |
| Postgres | Airflow metadata, MLflow backend, prediction log, replay checkpoint |
| Streamlit | Dashboard, polling Postgres |

Clarifications that matter.

**Redpanda is our Kafka.** It implements the Kafka API, so producers and consumers use standard Kafka client libraries (`confluent-kafka` or `kafka-python`) unchanged. We used it during the bachelor, it runs as a single binary without ZooKeeper or a JVM, and it needs much less memory than Apache Kafka, which matters on our laptops. Run it in dev-container mode (`--smp 1 --memory 1G`) and add Redpanda Console so we can show topics and messages during the demo.

**Airflow does not run the live producer or consumer.** Those are long-running services in Docker Compose that stay up continuously. The archive replay producer is different: each run sends one batch and exits, so it is a normal Airflow task. Airflow handles scheduled jobs with a start and an end. Putting a never-ending stream reader inside a DAG is a common mistake and a grader will spot it.

**Spark does not parse PGN.** Python UDFs in Spark serialize row by row, and we would be paying that cost across tens of millions of plies. Parsing happens in plain Python with multiprocessing; Spark does the columnar work afterwards.

## The model

**Label.** The evaluation difference before and after a move, from the mover's perspective. We do not need the engine's best move — consecutive annotations give the loss directly.

Convert centipawns to win probability before thresholding:

```latex
wp = \frac{1}{1 + e^{-0.00368208 \cdot cp}}
```

A 300 centipawn drop while already winning a queen is not a blunder in any useful sense, and raw centipawns cannot tell the difference. A blunder is a win-probability drop past a chosen threshold. Mate scores appear as `[%eval #3]` rather than a number and must be clamped, or they distort the label distribution.

**Features.** Chosen so none require an engine at inference time: rating, rating gap, time control and increment, seconds remaining, seconds spent on the previous move, move number, material balance, piece count, in-check flag, legal move count as a complexity proxy, and three opening-book features from eco.json:

- `in_book`: the current position is a known opening position.
- `plies_out_of_book`: plies since the game was last in a known position (0 while in book).
- `eco_family`: the ECO letter (A–E) of the last book position reached.

The opening name is not a model feature, but the dashboard shows it.

Phase 2b added engine-free threat features (pieces attacked, en prise, pinned, king zone, captures and checks available, what the mover could win) and game-history features (recent material change, material below the best point, recent time use). See phase 2b.

**Algorithm.** `HistGradientBoostingClassifier` from scikit-learn. Fast, handles mixed features without scaling, good enough for this assignment. Phase 2 tested class weights and dropped them: same PR-AUC, badly calibrated probabilities.

**Evaluation.** Blunders are a few percent of moves, so accuracy is meaningless here. Report PR-AUC and calibration. Two baselines to beat: the base rate, and a model using clock remaining alone.

**Retraining and promotion.** Every DAG run logs parameters, metrics and the model to MLflow and registers a new version under the `challenger` alias. The split is by time: the newest replayed batch is the test set and is never trained on in that run. The new version and the current `champion` are both scored on that same batch. If the new version has the higher PR-AUC, the `champion` alias moves to it. The live consumer always loads `models:/blunder@champion`.

**Replay and retraining schedule.** Proposed defaults, all configurable as Airflow params:

| Setting | Value | Why |
| --- | --- | --- |
| First run (backfill) | 20,000 annotated games (≈1.4M moves) | Enough for a meaningful first model; the per-run batch alone is too small to start from |
| Batch per scheduled run | 5,000 annotated games (≈350k moves) | Each run grows the data by a visible amount, and the newest batch is a large enough test set for a stable PR-AUC |
| Schedule | Every 6 hours (`0 */6 * * *`), `catchup=False`, `max_active_runs=1` | Our laptops are not on 24/7. Six hours gives two or three retrains in a working day, so we can show several MLflow runs; no catch-up storm after the laptop has been off |
| Training window | The most recent 60,000 games | Keeps training time and memory flat as the data keeps growing |
| Demo override | Trigger manually with a batch of 1,000–2,000 | A full retrain fits within a live demo |

Rough cost per scheduled run, to be confirmed in phase 4: finding 5,000 annotated games means scanning about 85,000 raw games. Parsing those takes 10–30 seconds on a few cores, and the whole DAG should finish in about 5–10 minutes. One monthly file holds millions of annotated games, so we will not run out.

## Windows setup

We are both on Windows laptops with PowerShell, so these apply to both of us.

**PowerShell aliases `curl` to `Invoke-WebRequest`**, which buffers the whole response and never returns on a stream. Call the real binary by full name:

```
curl.exe -N https://lichess.org/api/tv/rapid/feed
```

**Docker memory.** WSL2 takes a default share of RAM and this stack wants more. Create `C:\Users\<you>\.wslconfig`:

```
[wsl2]
memory=10GB
processors=6
```

Then `wsl --shutdown` and restart Docker Desktop. Leave 4-6 GB for Windows itself.

**zstd.** Do not install the CLI. Use the `zstandard` Python package and stream-decompress in process, so the expanded PGN never touches disk:

```
import zstandard, io
with open("lichess_db_standard_rated_2026-08.pgn.zst", "rb") as fh:
    reader = zstandard.ZstdDecompressor().stream_reader(fh)
    for line in io.TextIOWrapper(reader, encoding="utf-8"):
        ...
```

**Other notes.** Keep the repo on the Windows filesystem and bind-mount into containers as normal. Use `pathlib.Path` everywhere rather than string concatenation, since Spark and Airflow tasks run inside Linux containers while notebooks run on Windows. Check free disk space before downloading an archive — tens of GB compressed, plus Docker images, plus Parquet output adds up fast. Start that download early and let it run in the background.

## Build order

Each phase works standalone, so neither of us is ever blocked waiting for the other. Phase 2 is the one to protect: it tells us whether the problem is learnable at all before we have built any infrastructure.

| Phase | What gets built | Depends on | Suggested owner |
| --- | --- | --- | --- |
| 0 | Setup: WSL limits, archive download, TV feed sample, repo layout | nothing | both |
| 1 | Python script: parse annotated games to Parquet. No Docker | 0 | A |
| 2 | Train a model in a notebook on that Parquet, write the shared feature module | 1, 7 | A |
| 3 | Redpanda up, TV feed producer writing events | 0 | B |
| 4 | RustFS, MLflow, Postgres, Airflow; archive replay producer with checkpoint; training DAG | 1, 2, 3 | A |
| 5 | Spark job for cleaning, opening book join, feature engineering and windowing | 1, 4, 7 | A |
| 6 | Live consumer loading the registered model and scoring; raw events to RustFS | 3, 4, 7 | B |
| 7 | eco.json opening book: download, normalise FEN keys, lookup table | 0 | B |
| 8 | Streamlit dashboard | 6 | B |
| 9 | End-to-end run, README, architecture diagram, grading rehearsal | all | both |

Phases 1, 3 and 7 can start at the same time once setup is done. Phase 7 is small, so B should do it first: phase 2 needs the opening-book features. The shared feature module (including FEN normalisation) is written during phase 2 and imported by both phase 5 and phase 6 — agree on its interface before either of us writes against it.

### Phase 0 — Setup (day 1, both)

- Create `.wslconfig` (see Windows setup), run `wsl --shutdown`, restart Docker Desktop.
- Check free disk space, then start the archive download in the background.
- Run `curl.exe -N https://lichess.org/api/tv/rapid/feed` for 30 seconds and save the output as a sample of the real event schema.
- Create the repo layout: `ingest/`, `features/` (shared module), `spark/`, `dags/`, `consumer/`, `dashboard/`, `notebooks/`, `docker-compose.yml`.

### Phase 1 — Parse the archive to Parquet (A, no Docker)

- Stream-decompress with `zstandard`. First pass: text-scan each game for `[%eval`. Second pass: parse the survivors with `python-chess`, using multiprocessing.
- Output one row per move: game id, ratings, time control, clock, eval before and after, FEN, normalised FEN key, move number.
- Clamp mate scores, convert eval to win probability, compute the drop.
- Done when: about 60k games are in Parquet and the blunder rate looks plausible (a few percent).

**Status (Oct 30): done.** `ingest/archive.py` (reader and parser, reused by the replay producer in phase 4) and `ingest/parse_archive.py` (CLI). Result: 60,000 annotated games out of 607,614 scanned, 3.87M move rows, 210 MB Parquet in `data/parquet/moves/`, 173 seconds on 8 workers. The first move of each game is skipped because Lichess does not annotate the starting position. Each row also carries `speed` (computed from the time control with Lichess's rule, so the live lane can compute it the same way) and `lichess_nag`, the `?!` / `?` / `??` mark Lichess's own analysis put on the move.

Choosing the blunder threshold, measured against Lichess's own `??` marks on the trial run (2,000 games):

| Win-probability drop | Moves labelled blunder | Lichess `??` moves caught | Labelled moves that are `??` |
| --- | --- | --- | --- |
| 0.10 | 10.9% | 90% | 44% |
| 0.15 | 7.1% | 84% | 64% |
| 0.20 | 5.0% | 77% | 83% |
| 0.30 | 2.8% | 52% | 100% |

Proposal: 0.15, which gives 6.9% blunders over the full 60k games. Lichess's `??` marks are not a pure threshold (some have drops below 0.05, likely mate-related), so we use our own reproducible label and keep `lichess_nag` for validation only.

First look at the signal: blunder rate is 8.6% with under 10 seconds on the clock versus 6.7% above 3 minutes, and 7.5% in bullet versus 6.6% in blitz. Clock alone is a weak predictor, so the clock-only baseline will be easy to beat, and the board features have to carry most of the model.

### Phase 2 — Notebook model (A). Go / no-go check

- Write `features/` with one function, e.g. `compute_features(board, clocks, meta, book) -> dict`, plus `fen_key(fen) -> str`. This is the interface both lanes import.
- Train `HistGradientBoostingClassifier` with class weights. Compare to the base rate and the clock-only baseline. Report PR-AUC and calibration.
- Done when: the model clearly beats the clock-only baseline.

**Status (Oct 30): done, go.** `ingest/build_features.py` replays every parsed game through `compute_features` (3.87M rows, about 2 minutes on 8 workers, `data/parquet/features/`), and `notebooks/blunder_model.ipynb` trains and evaluates. Test set: the newest 11,854 games (last 6 of 30 parts), 760k moves.

| Model | PR-AUC | ROC-AUC | Brier |
| --- | --- | --- | --- |
| Base rate | 0.069 | 0.500 | 0.064 |
| Clock only | 0.086 | 0.591 | 0.064 |
| All 15 features | **0.156** | 0.743 | 0.061 |
| All 15, class-weighted | 0.157 | 0.744 | 0.212 |

- The model beats clock only by 1.8× in PR-AUC, and by at least 1.78× in every speed. Flagging the riskiest 5% of moves catches 15% of blunders at 20% precision (2.9× the base rate).
- **Change from the plan: no class weights.** They don't improve PR-AUC and they ruin calibration (mean prediction 0.41 against a 6.9% blunder rate). The unweighted model is well calibrated, so the dashboard can show its probability directly.
- Opening features are redundant with move number, piece count and legal moves: removing all three leaves PR-AUC unchanged. We keep them (the join, the dashboard). Named-only versus with interpolated positions makes no difference; we keep the interpolated ones.
- Strongest features: material balance, mover rating, legal move count, plies out of book. More model capacity does not help (0.157 at most), so the ceiling is the feature set.
- DAG settings for phase 4: `HistGradientBoostingClassifier(max_iter=500, early_stopping=True, random_state=0)`, no class weights, `features.FEATURES` (26 features after phase 2b).

Interface agreed:

- `features/board.py`: `board_features(board)`. Material balance (mover's view), piece count, in check, legal move count, move number. Needs python-chess, so the batch lane computes these in the parser, not in Spark.
- `features/openings.py`: `book_features(fen_key, book)` gives `in_book` and the position's ECO letter.
- `features/game.py`: `GameState`, the per-game state for the sequential features (`plies_out_of_book`, `eco_family` of the last book position, `prev_time_spent` per colour). The live consumer keeps one per game id; Spark computes the same values with window functions.
- `features/__init__.py`: `compute_features(board, clocks, meta, state, book)`, the ordered `FEATURES` list (the training/serving contract), `BLUNDER_THRESHOLD = 0.15` and `is_scored()` (drops bot and UltraBullet games).
- Conventions: plies count positions (start = 0, python-chess `board.ply()`); the starting position counts as book; missing values stay `None` (HistGradientBoosting handles NaN natively); `time_spent` uses one shared formula `clock_before - clock_after + inc`.
- Only information available before the move is used: no evals, no clock after the move.

### Phase 2b — Threat and game-history features (A)

Phase 2 showed the feature set was the ceiling, so we added two groups of engine-free features. Both lanes get them through the shared module.

- **Threats** (in `board_features`): own pieces attacked; en prise (attacked and undefended, or attacked by a cheaper piece), their count and the value of the most valuable one; the same value for the opponent's pieces (what the mover could win); pinned pieces; attacked squares around the king; captures and checks available. A proxy for "the right move is hard to find".
- **History** (in `GameState`): material change over the last 4 plies, material below the mover's best point in the game, average time over the mover's last 3 moves. A proxy for "has the game already gone wrong". Earlier blunders themselves can't be used: the live lane has no evals to detect them.

**Status (Oct 30): done.** The feature table now takes about 6 minutes to build (the check and capture scan over every legal move is the slow part).

| Features | PR-AUC | ROC-AUC |
| --- | --- | --- |
| Phase 2 (15) | 0.156 | 0.743 |
| + threats (23) | 0.174 | 0.766 |
| + history (18) | 0.157 | 0.745 |
| All 26 | **0.177** | 0.769 |

- Threats give +12% PR-AUC. The strongest new feature is `opp_en_prise_value`: missing a chance to win material is also a win-probability drop. History adds almost nothing, because material balance already carries most of it. We keep both groups.
- Overall, 2.1× the clock-only baseline (at least 2.0× in every speed). Flagging the riskiest 5% of moves catches 17% of blunders at 23% precision (3.3× the base rate). Calibration still holds without class weights.
- For phase 5: the history features are Spark windows too: `lag` over 4 plies for material change, a running max for the drawdown, and a 3-row window over the mover's own moves for the time average.

### Phase 3 — Redpanda and the TV producer (B, in parallel with 1–2)

- Redpanda (dev-container mode) and Redpanda Console in Docker Compose.
- Producer with one connection per channel. Holds game metadata from each `featured` event in memory and stamps every move event with it. Handles game switches.
- Publishes to `tv.moves`, keyed by game id.

**Status (Oct 31): done.** Redpanda v25.1.1 (`--mode dev-container --smp 1`, Kafka on `localhost:19092`) and Console (`localhost:8085`) in `docker-compose.yml`. `ingest/tv_producer.py` runs one thread per channel (`uv run python -m ingest.tv_producer rapid blitz`). On each `featured` event it fetches the game once from `/game/export/{id}` for the time control and the moves so far, sends a `start` message, then stamps every `move` message with the game metadata. A 45 second run on rapid and blitz put 18 messages on `tv.moves` (1 partition, auto-created). Tests in `tests/test_tv_producer.py`.

Gaps found in the check, to fix before phase 6:

- The producer drops `user.title` from the `featured` players, but `is_scored()` needs both titles to skip bot games.
- It also drops the players' `seconds` from the `featured` event, so the consumer has no clock for the first move after joining a game.
- `moves_so_far` lags the live position: Lichess delays exports of ongoing games, and both captured games replayed to ply 50 while the start FEN was at ply 53. The consumer must trust the FEN and accept a small gap in the history features when it joins mid-game.
- No delivery callback, so failed sends go unnoticed. Redpanda runs without `--memory 1G`.

### Phase 4 — Storage, tracking and orchestration (A)

- Add RustFS (S3 API), Postgres, MLflow (Postgres backend, artifacts on RustFS) and Airflow with `LocalExecutor` to Compose.
- Archive replay producer: reads the next N annotated games from the `.pgn.zst`, publishes them to `games.archive`, saves the offset in a Postgres checkpoint table. Resumes from the checkpoint on the next run.
- A consumer task writes `games.archive` to `bronze/archive/` on RustFS.
- DAG `train_blunder_model` (every 6 hours, see the replay and retraining schedule): replay next batch → land in bronze → Spark features → train → evaluate on newest batch → register → promote `champion` if better.

**Status (Oct 31): storage, tracking and training done; Airflow, replay and DAG still to do.**

- Compose (B) adds RustFS (S3 API on `localhost:9000`, console on `9001`), Postgres 16 (databases `airflow`, `mlflow`, `chess`; `docker/postgres-init.sql` only runs when the volume is first created), pgAdmin (`localhost:5050`) and MLflow 3.17.2 (`localhost:5000`, Postgres backend, artifacts in the `mlflow` bucket, same version as `uv.lock`). `storage/create_buckets.py` creates `bronze`, `silver`, `gold` and `mlflow`; `storage/check_setup.py` checks MLflow logs to Postgres and RustFS.
- `training/train_model.py` trains with the phase 2 settings on the feature table, holds out the newest 6 parts, logs parameters, metrics (incl. the clock-only baseline and the lift over it) and the feature list as the `features` tag, registers the version and gives it the `challenger` alias.
- `training/evaluate_model.py` scores `@challenger` and `@champion` on the same held-out parts, logs the comparison as its own run (`evaluate v<n>`, tag `promoted`), moves `champion` if the challenger's PR-AUC is strictly higher (or there is no champion yet), and removes the `challenger` alias. These become the train and evaluate tasks of the DAG. The `champion` alias can still be moved by hand in the MLflow UI, for a rollback or the demo.
- **`blunder` v1 is champion**: PR-AUC 0.177, ROC-AUC 0.769, clock only 0.086 (2.1×), mean prediction 6.9% against a 7.1% blunder rate, the same as the notebook. A retrain with the same data gave an identical v2 (`random_state=0`), which the evaluation correctly did not promote on the tie.
- `features.model_input(rows)` turns feature dicts into the dtypes the model was trained on (booleans as 0/1, `eco_family` as a categorical over A–E). Training and the consumer both call it; scoring one dict at a time gives exactly the same probabilities as scoring a DataFrame.
- **MLflow artifacts from the host.** The server tells clients to download artifacts straight from RustFS via presigned links to `http://rustfs:9000`, a name that only resolves inside the Docker network. From Windows every model load stalled on retries. `storage.tracking.connect()` sets the tracking URI and switches the client to download and upload through the MLflow server; loading the champion now takes about 3 seconds. Every script that talks to MLflow from the host calls it; containers on `chess_network` don't need it.
- **Model serialisation.** MLflow 3.17 saves sklearn models with skops and refuses types that are not on its allow list. `SKOPS_TRUSTED` in `train_model.py` lists the three HistGradientBoosting internals it flagged. We keep skops rather than switching to cloudpickle, which would run arbitrary code on load.
- Still to do: Airflow (`LocalExecutor`) in Compose, the archive replay producer with its checkpoint table, landing `games.archive` in `bronze/archive/`, uploading the opening book to `bronze/eco/` and `gold/openings/`, and the `train_blunder_model` DAG.

### Phase 5 — Spark job (A)

- Read bronze, clean (drop bot and unrated games, deduplicate), write silver.
- Join `gold/openings/` on the normalised FEN key (the combining step).
- Window functions within each game: seconds spent on the previous move, `plies_out_of_book`, rolling features.
- Write gold features to RustFS. Runs in Spark local mode inside the Airflow task.

### Phase 6 — Live consumer (B)

- Consume `tv.moves`, load the opening book from `gold/openings/` into memory at startup, compute features with the same `features/` module, load `models:/blunder@champion`, write predictions to Postgres.
- Keep small per-game state (previous clock, last in-book ply) and reset it on a `featured` game switch.
- Write raw events to `bronze/tv/` on RustFS.
- Test that a known position produces identical feature vectors through the Spark path and the consumer path.

Ready from phase 4 (Oct 31): the champion is registered, so the consumer can be built against it now.

- Call `storage.tracking.connect()` before any MLflow call when running on the host, load the model with `mlflow.sklearn.load_model("models:/blunder@champion")`, and score with `model.predict_proba(model_input([features]))[0, 1]`.
- Check at startup that the champion run's `features` tag equals `features.FEATURES`.
- Handle the producer gaps listed under phase 3 (titles, starting clocks, the lag in `moves_so_far`).

**Status (Oct 31): done, except the Spark side of the parity test (waits for phase 5).**

- `consumer/live_game.py`: `LiveGame`, the per-game state of the live lane. Every `move` message gives the position after the move, so the consumer scores the move the player to move is about to make. The board always comes from the move message's FEN. The time spent on the previous move comes from two consecutive positions' clocks, using the parser's formula.
- **The `featured` FEN is not trustworthy.** Checked on the live feed (Oct 31): its move counters are reset (`0 1`, sometimes with the wrong side to move) and both players' `seconds` are the same value. Start messages are therefore only used to warm up a game, never scored, and the producer no longer sends their clocks.
- Producer gaps from phase 3 fixed: titles are stamped on every message, the start message carries the variant and the clocks so far (`/game/export?clocks=true`, centiseconds rounded to whole seconds like the feed; the export has one clock more than moves, which we drop), failed sends are logged through a delivery callback, and Redpanda runs with `--memory 1G`.
- Joining mid-game: the moves and clocks so far are replayed through `compute_features`, which gives the same state as the batch lane. When the export lags behind the live position, the missing plies' material is assumed unchanged and the time features catch up after one move per side. After a scorer restart, games already in progress continue without history (`plies_out_of_book` counts from the start of the game).
- `consumer/score_moves.py`: consumer group `live-scorer`, loads the book from `gold/openings/`, checks the champion's `features` tag against `features.FEATURES` at load, reloads when the alias moves, and writes to `live_predictions` (one row per game and ply, with the feature vector as `jsonb` for the dashboard). Standard variant only, bots and UltraBullet skipped.
- `consumer/land_tv.py`: consumer group `bronze-tv`, raw messages as JSON text plus filter columns, one Parquet file per partition every 5 minutes, offsets committed after the upload.
- Parity: `tests/test_live_game.py` sends one game through the batch lane (`parse_game` → `game_features`) and through `LiveGame` as feed messages, watched from the start and joined at several plies. All 26 features are identical at every ply. Once the Spark job exists, add its output for the same game to this test.
- End-to-end run (Oct 31, all four channels, in Docker): 91 positions scored in 90 seconds, mean probability 3–7% per channel, and 661 raw events landed in `bronze/tv/`.

### Phase 7 — Opening book lookup (B)

- Airflow task `load_openings`: download `ecoA.json` … `ecoE.json` from a pinned commit into `bronze/eco/`.
- Normalise each FEN key to its first three fields, deduplicate, and write `gold/openings/` (FEN key, ECO code, name) as Parquet.
- Sanity check: the starting position after `1. e4` and after `1. d4 d5 2. c4` both resolve to a named opening.

**Status (Oct 30): local version done** (done early because phase 2 needs it). `ingest/load_openings.py` downloads the six files from pinned commit `e397a1fa` to `data/raw/eco/e397a1fa/` and writes `data/parquet/openings/openings.parquet`; `features/openings.py` loads it as a lookup. The Airflow task and the RustFS upload come with phase 4. Details:

- 15,838 raw entries become 15,373 unique positions (12,105 named, 3,268 interpolated) after shortening the FEN keys.
- Every entry's move sequence is replayed with python-chess and checked against its key. All 15,838 pass, once move numbers written without a space (`1.e4`) are handled.
- When several entries share a position (transpositions), we keep the named entry over the interpolated one, then the shortest move sequence.
- We include `eco_interpolated.json`, and an `is_interpolated` column lets phase 2 compare with and without it.

Coverage on the 3.87M parsed moves: 9.0% of moves are in book (8.7% without interpolated). Games leave the book at ply 7 (median), and 90% have left by ply 11. Blunder rate is **0.53% in book versus 7.54% out of book**, so the opening features carry real signal.

### Phase 8 — Streamlit dashboard (B)

- Live board with blunder probability per move, current opening name, in-book or out-of-book indicator; channel selector (default rapid).
- Data insight: blunder rate by rating band, by clock remaining, and by plies out of book.
- Model page: MLflow metrics across retrains, current champion version.

### Phase 9 — Wrap-up (both)

- Full stack end to end from a clean `docker compose up`.
- README with setup steps and an architecture diagram.
- Rehearse the answers in "Decisions to defend at grading".

- [ ] Agree who takes A and who takes B
- [ ] Start the archive download in the background today
- [ ] Run the TV feed for 30 seconds and check the real event schema before writing the producer

## Decisions to defend at grading

Each of these is a deliberate choice with a reason. Writing the reason down now means we can answer without improvising later.

**Two-pass PGN parsing.** Text-scan for `[%eval` first, full-parse only the survivors. Full parsing runs at roughly 150-500 games per second per core, so parsing everything would take hours; parsing the annotated 6% takes minutes.

**No python-chess inside Spark UDFs.** Row-by-row serialization across tens of millions of plies would be brutal, and it would mean installing python-chess in the worker image. Parsing is plain Python, Spark does columnar work.

**No Stockfish in the live path.** Far too slow for a streaming consumer. We train offline on precomputed evaluations and infer online without an engine. If we want live accuracy tracking, a separate low-frequency DAG runs shallow-depth evaluation on a sample of positions.

**Producer enriches events with game metadata.** Ratings arrive once in the `featured` event, not in each move event. The producer holds game metadata in memory and stamps every published message with it, so consumers need no external lookups for metadata.

**One feature module imported by both lanes.** Training and serving skew is the real technical risk. If legal move count or the FEN key is computed differently in the Spark job and the live consumer, predictions degrade silently. We test that a known position produces identical vectors through both paths.

**Redpanda instead of Apache Kafka.** Same Kafka API and client libraries, no ZooKeeper or JVM, much lower memory use, and the tool we were taught with.

**Opening book from a static file, not an API.** eco.json is a pinned, versioned file loaded once into RustFS, so there is no external call in the consumer hot path, no rate limits and no failure mode during the demo.

**Opening book not sent through Redpanda.** Redpanda carries event data: games and moves that arrive over time. eco.json is 12k rows of reference data that changes never, so an Airflow task loads it straight into bronze. Pushing a fixed lookup table through a message queue adds nothing.

**Archive replayed through Redpanda with a checkpoint.** The assignment requires Kafka for ingestion and requires that each run of the simulation program sends the next new data. The replay producer satisfies both: it publishes the archive in batches and resumes from a stored offset. It also makes periodic retraining meaningful, because each run adds new games.

**Time-based test split and champion comparison.** Each retrain is tested on the newest batch it has not seen, and the challenger only replaces the champion if it scores better on that same batch. A random split would leak games from the same period into training and flatter the metrics.

**Raw live events kept on RustFS.** Predictions go to Postgres for the dashboard, but the raw TV feed is also written to bronze so all data lands in the lake and can be reprocessed later.

## Risks and open questions

**Infrastructure is the real risk, not the data.** Redpanda, Spark, Airflow, MLflow, RustFS, Postgres and Streamlit all running on Docker Desktop on a Windows laptop is the memory wall, and it will bite long before PGN parsing does. Mitigations: raise the WSL limit, run Redpanda in dev-container mode, run Spark in local mode rather than master and worker, use `LocalExecutor` instead of Celery, and bring services up in stages while developing rather than keeping the full stack hot.

**Stream volume is thin.** Eight TV channels produce maybe one to two events per second. If that looks unimpressive, `POST /api/stream/games-by-users` with a few hundred usernames harvested from the leaderboards gives a real firehose.

**Game transitions.** The feed sends a new `featured` event when a channel switches games. If the consumer assumes one continuous game, the dashboard will silently show a finished game during the demo, and the per-game state (previous clock, last in-book ply) will carry over into the wrong game.

**Class imbalance.** Blunders are a few percent of moves. Plan for stratified sampling and class weights from the start.

**Opening book coverage is shallow.** Most games leave eco.json within 10–20 plies, so `in_book` is false for most moves. `plies_out_of_book` carries the signal after that; check in phase 2 that it actually helps the model.

Open questions to settle between us:

- [ ] Confirm the replay defaults: 20k backfill, 5k per run, every 6 hours, 60k training window
- [x] Do we include `eco_interpolated.json` (gap-filling positions) or only the named openings in `ecoA` … `ecoE`? Yes, flagged with `is_interpolated` so phase 2 can compare
- [x] Which month of the archive? September 2026, first 3 GB (SHA-256 in the README)
- [ ] Which month of the archive do we use, and do we both work from the same slice?
- [x] Blunder threshold — what win-probability drop counts? 0.15 (`BLUNDER_THRESHOLD` in `features/`)
- [x] Do we exclude bot games and unrated players? Bots out (194 of 59,894 games, blunder rate 4.3% vs 6.9%), in training and live. Unrated is moot for training: the archive file holds rated games only. Live, casual games are scored. UltraBullet is excluded too (171 games, no TV channel)
- [ ] Which channel does the dashboard default to? Rapid or classical will look more responsive than bullet, given Streamlit's polling refresh.
