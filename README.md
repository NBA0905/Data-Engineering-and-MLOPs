# Data-Engineering-and-MLOPs
Project data engineering and mlops, Saxion 4th year, October 2026

Chess blunder prediction pipeline. See [the build plan](Chess%20blunder%20prediction%20pipeline%20—%20build%20plan.md) for the design.

## Repository layout

| Folder | Contents |
| --- | --- |
| `ingest/` | Archive parser, archive replay producer, TV feed producer, opening book loader |
| `features/` | Shared feature module, imported by both the Spark job and the live consumer |
| `spark/` | Spark jobs: cleaning, opening book join, feature engineering |
| `dags/` | Airflow DAGs |
| `consumer/` | Live consumer that scores TV feed moves |
| `dashboard/` | Streamlit dashboard |
| `notebooks/` | Exploration and the phase 2 model notebook |
| `tests/` | Tests, including the train/serve feature parity test |
| `data/` | Local data, not committed: `raw/` archives, `samples/` feed captures |

## Local setup

The project uses [uv](https://docs.astral.sh/uv/) for Python and dependencies. Dependencies are declared in `pyproject.toml` and pinned in `uv.lock`; do not use `pip install`.

1. Install uv (once per machine), then open a new terminal:

   ```powershell
   powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
   ```

2. From the repo root, create `.venv` with Python 3.12 and install everything from the lockfile:

   ```powershell
   uv sync
   ```

   uv downloads Python 3.12 itself if it is not installed.

3. Run things through uv, so the project environment is always used:

   ```powershell
   uv run python -m ingest.check_archive <path>
   uv run jupyter lab
   uv run pytest
   ```

Adding a dependency: `uv add <package>` (or `uv add --dev <package>` for notebook and test tools). Commit both `pyproject.toml` and `uv.lock`.

## Running everything end-to-end

From a fresh clone to live blunder predictions on the dashboard. The steps build on each other, so run them in order. The sections further down explain each part in detail.

### 1. Prerequisites

- Docker Desktop running, with plenty of memory for Docker: the Spark Connect driver and the Spark worker take 2 GB each and Redpanda 1 GB, on top of Airflow, MLflow and Postgres.
- uv and `uv sync` done, see [Local setup](#local-setup).
- The 3 GB archive in `data\raw\`, see [Downloading the game archive](#downloading-the-game-archive-3-gb). The training DAG reads it from there through the project mount.
- Windows only: nothing else on port 5432. A local PostgreSQL installation takes those connections first, see [Troubleshooting](#troubleshooting-on-windows).

### 2. Start the stack

```powershell
docker compose up -d --build
docker compose ps
```

The first start builds the images and Spark Connect downloads its S3 connector, which takes a few minutes. Wait until `airflow-apiserver` and `spark-master` show `healthy`. The live services start too, but `live-scorer` keeps restarting until step 4 has made a champion model. That is expected.

| UI | URL | Login |
| --- | --- | --- |
| Airflow | http://localhost:8080 | airflow / airflow |
| MLflow | http://localhost:5000 | |
| RustFS (data lake) | http://localhost:9001 | rustfsadmin / rustfsadmin |
| Redpanda Console | http://localhost:8085 | |
| pgAdmin | http://localhost:5050 | |
| Spark master | http://localhost:8088 | |

### 3. Opening book

In Airflow, unpause the `opening_book` DAG (DAGs are paused when they are created). It runs once by itself: it creates the buckets `bronze`, `silver`, `gold` and `mlflow`, and writes `gold/openings/openings.parquet`. Takes under a minute.

### 4. Train the first model

Unpause `train_blunder_model` and trigger it (▶). The first run replays 20,000 games, builds the features with Spark, trains a model and promotes it to `champion`, because there is no champion yet. For a quicker demo, trigger it with a smaller `backfill` param, but the model is then trained on fewer games.

When the run is green, MLflow shows model `blunder` with the `champion` alias. After that the DAG retrains every 6 hours on the next 5,000 games, and only promotes the new model if it scores better.

### 5. Live lane

Restart the scorer so it loads the champion straight away instead of waiting for its next restart:

```powershell
docker compose restart live-scorer
docker compose logs -f live-scorer
```

Within a minute the log shows lines like `[blitz] Iu3PrBcg ply 38 d7c5: white to move, blunder risk 5.1%`. The predictions go to the `live_predictions` table in Postgres (database `chess`), and the raw feed lands in `bronze/tv/` every 5 minutes.

### 6. Dashboard

```powershell
uv run streamlit run dashboard/app.py --server.port 8600
```

Open http://localhost:8600. **Live** shows the game on the chosen TV channel with the blunder risk of both players, **Data** shows where blunders happen in `gold/features`, and **Model** shows every retrain from MLflow.

### Stopping

`docker compose stop` stops everything and keeps all data. `docker compose down -v` also deletes the volumes (Postgres, RustFS, Redpanda), so you start again from step 2.

### Troubleshooting on Windows

- **`password authentication failed for user "airflow"`** from anything you run with `uv run`: a local PostgreSQL service is using port 5432. Containers are not affected, only scripts on the host. Stop it and keep it from starting at boot, in PowerShell as administrator (use your own version number):

  ```powershell
  Stop-Service postgresql-x64-16
  Set-Service postgresql-x64-16 -StartupType Manual
  ```

- **`Port 8501 is not available`** while nothing uses it: Windows reserves port ranges for Hyper-V/WSL, which Docker uses, and the range can include Streamlit's default port. `netsh interface ipv4 show excludedportrange protocol=tcp` lists them. Use a port outside those ranges, like `--server.port 8600` above.
- **Live page says "No predictions for … yet"**: check `docker compose logs live-scorer`. It needs a champion model (step 4) and the opening book (step 3).

## Downloading the game archive (3 GB)

Training data comes from the [Lichess open database](https://database.lichess.org/). A monthly file is about 29 GB, but the whole project needs only a few percent of one month. We therefore download just the **first 3 GB** of the September 2026 file with an HTTP range request. That slice holds millions of games, of which about 10% carry engine evaluations.

We both use the same month and the same slice, so our data is identical.

### 1. Check free disk space

You need 3 GB free on the drive that holds the repo, plus room for Docker images and Parquet output later.

```powershell
Get-PSDrive -PSProvider FileSystem
```

### 2. Download

Run from the repo root. In PowerShell, write `curl.exe`, not `curl`: plain `curl` is an alias for `Invoke-WebRequest`, which does not support this.

```powershell
New-Item -ItemType Directory -Force data\raw
curl.exe -r 0-3221225471 -o data\raw\lichess_db_standard_rated_2026-09.part.pgn.zst https://database.lichess.org/standard/lichess_db_standard_rated_2026-09.pgn.zst
```

`-r 0-3221225471` asks the server for bytes 0 up to 3 GiB only. The download takes about 10 minutes on a fast connection; add `--progress-bar` to see progress.

On macOS or Linux the same command works with `curl` and forward slashes.

### 3. If the download was interrupted

Resume from where it stopped instead of starting over. This requests only the missing bytes and appends them:

```powershell
$f = "data\raw\lichess_db_standard_rated_2026-09.part.pgn.zst"
$have = (Get-Item $f).Length
curl.exe -r "$have-3221225471" -o "$f.rest" https://database.lichess.org/standard/lichess_db_standard_rated_2026-09.pgn.zst
cmd /c copy /b "$f+$f.rest" "$f.tmp"
Move-Item -Force "$f.tmp" $f
Remove-Item "$f.rest"
```

The first two lines check how many bytes you already have and download only the rest into a separate file. `copy /b` then glues the two parts together byte for byte.

### 4. Verify

```powershell
uv run python -m ingest.check_archive data\raw\lichess_db_standard_rated_2026-09.part.pgn.zst
```

Expected output:

```
file size: 3,221,225,472 bytes (complete)
games read: 50,000, with evals: 5,331 (10.7%)
```

To confirm you have exactly the same bytes as the rest of the team, compare the checksum:

```powershell
(Get-FileHash data\raw\lichess_db_standard_rated_2026-09.part.pgn.zst -Algorithm SHA256).Hash
```

Expected: `5EE7E70DCDF82996C98CC1EDF733EFCD5152951AB49E2BEC2DCDEBA9FAB0E5F5`

The file is cut off mid-stream, so its very last game is incomplete. That is expected: the parser reads from the start and stops at the last complete game, and we will never get close to the end anyway.

### 5. Parse to Parquet (phase 1)

```powershell
uv run python -m ingest.parse_archive data\raw\lichess_db_standard_rated_2026-09.part.pgn.zst --games 60000
```

Writes one row per move to `data\parquet\moves\` (about 3.9M rows, 210 MB, roughly 3 minutes). Use `--games 2000 --out data\parquet\trial` for a quick test run.

## Opening book (eco.json)

```powershell
uv run python -m ingest.load_openings
```

Downloads the eco.json files from a pinned commit into `data\raw\eco\` and writes the lookup table `data\parquet\openings\openings.parquet` (15,373 positions). Takes a few seconds. Run it before `uv run pytest`, otherwise the opening tests are skipped.

## Blunder model notebook

Needs the parsed moves and the opening book from the two steps above. Build the feature table, once with the full opening book and once with named openings only (for the comparison in the notebook):

```powershell
uv run python -m ingest.build_features
uv run python -m ingest.build_features --no-interpolated --out data\parquet\features_named
```

Each takes about 6 minutes and writes 3.87M rows with 26 features. Then open `notebooks/blunder_model.ipynb` with `uv run jupyter lab` and run all cells (a few minutes).

## Training DAG (archive replay → bronze → Spark features → train → evaluate)

The batch lane runs in Airflow as the `train_blunder_model` DAG, every 6 hours. Run the `opening_book` DAG once first: the Spark step needs `gold/openings/`. The Spark services (`spark-master`, `spark-worker`, `spark-connect`) must be up; the DAG talks to Spark Connect on `spark-connect:15002`.

| Task | Script | What it does |
| --- | --- | --- |
| `replay_archive` | `ingest.replay_archive` | Sends the next annotated games from the archive to the `games.archive` topic and saves the position in the `archive_checkpoint` table (Postgres, database `chess`). The first run sends 20,000 games, every run after that 5,000. |
| `land_in_bronze` | `ingest.land_archive` | Writes every message on `games.archive` the `bronze-archive` consumer group has not landed yet to `bronze/archive/` as Parquet, then commits the offsets. |
| `parse_moves` | `ingest.parse_bronze` | Parses every new file in `bronze/archive/` into `bronze/moves/`, one row per move with the board features. Plain Python with multiprocessing, because PGN parsing and the board features need python-chess. |
| `spark_features` | `spark.build_features` | Spark: drops bot and UltraBullet games and duplicates into `silver/moves/`, joins the opening book on the FEN key and computes the per-game history features with window functions into `gold/features/`. Rebuilds both from all of `bronze/moves/` on every run. |
| `train_model` | `training.train_model` | Trains on `s3://gold/features`, newest 3 parts (6,000 games) held out, 27 parts before them as the training window, registers the `challenger`. |
| `evaluate_model` | `training.evaluate_model` | Scores challenger and champion on the same 3 parts, promotes the better one. |

Batch sizes, test parts and training window are DAG params, so a manual trigger can use a small batch for the demo. Each task runs the same command you can run by hand, for example:

```powershell
uv run python -m ingest.replay_archive data\raw\lichess_db_standard_rated_2026-09.part.pgn.zst --games 1000
uv run python -m ingest.land_archive
uv run python -m ingest.parse_bronze
uv run python -m spark.build_features
uv run python -m training.train_model --features s3://gold/features --test-parts 3 --train-parts 27
```

To replay the archive from the start again, delete its row from `archive_checkpoint` (pgAdmin, database `chess`).

## Live lane (TV feed → Redpanda → scorer → Postgres, bronze)

Three long-running services in Docker Compose, all from `docker/live.Dockerfile` (dependencies from `uv.lock`, code mounted at `/app`). Needs the `champion` model in MLflow and the opening book in `gold/openings/` (run the `opening_book` DAG once).

```powershell
docker compose up -d --build tv-producer live-scorer tv-lander
docker compose logs -f live-scorer
```

| Service | Script | What it does |
| --- | --- | --- |
| `tv-producer` | `ingest.tv_producer` | Reads the bullet, blitz, rapid and classical TV channels and sends a `start` message per game (players, titles, time control, variant, moves and clocks so far) and a `move` message per move to `tv.moves`, keyed by game id. |
| `live-scorer` | `consumer.score_moves` | Keeps one `consumer.live_game.LiveGame` per game, computes the features of every position with the shared `features` module and writes the `blunder@champion` probability for the move about to be played to `live_predictions` (Postgres, database `chess`). Checks every 5 minutes whether the champion alias moved and loads the new version. |
| `tv-lander` | `consumer.land_tv` | Writes the raw messages to `bronze/tv/date=YYYY-MM-DD/` as Parquet every 5 minutes, and once more on `docker compose stop`. |

Scoring and landing use separate consumer groups (`live-scorer`, `bronze-tv`), so one never holds up the other. The scorer stores a Kafka offset only after its prediction is written, and a prediction is unique per `(game_id, ply)`, so restarts neither lose nor duplicate rows. On its very first start the scorer begins at the newest message; `--from-start` scores what is already on the topic.

Outside Docker the same commands run with `uv run python -m ...`, but only if `localhost:5432` reaches the Docker Postgres, see [Troubleshooting on Windows](#troubleshooting-on-windows).
