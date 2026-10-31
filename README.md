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

## Training DAG (archive replay → bronze → features → train → evaluate)

The batch lane runs in Airflow as the `train_blunder_model` DAG, every 6 hours. Run the `opening_book` DAG once first: the features step needs `gold/openings/`.

| Task | Script | What it does |
| --- | --- | --- |
| `replay_archive` | `ingest.replay_archive` | Sends the next annotated games from the archive to the `games.archive` topic and saves the position in the `archive_checkpoint` table (Postgres, database `chess`). The first run sends 20,000 games, every run after that 5,000. |
| `land_in_bronze` | `ingest.land_archive` | Writes every message on `games.archive` the `bronze-archive` consumer group has not landed yet to `bronze/archive/` as Parquet, then commits the offsets. |
| `build_features` | `ingest.build_gold_features` | Parses new bronze files into `silver/moves/` and builds their features into `gold/features/`. Stand-in for the Spark job (phase 5). |
| `train_model` | `training.train_model` | Trains on `s3://gold/features`, newest 3 parts (6,000 games) held out, 27 parts before them as the training window, registers the `challenger`. |
| `evaluate_model` | `training.evaluate_model` | Scores challenger and champion on the same 3 parts, promotes the better one. |

Batch sizes, test parts and training window are DAG params, so a manual trigger can use a small batch for the demo. Each task runs the same command you can run by hand, for example:

```powershell
uv run python -m ingest.replay_archive data\raw\lichess_db_standard_rated_2026-09.part.pgn.zst --games 1000
uv run python -m ingest.land_archive
uv run python -m ingest.build_gold_features
uv run python -m training.train_model --features s3://gold/features --test-parts 3 --train-parts 27
```

To replay the archive from the start again, delete its row from `archive_checkpoint` (pgAdmin, database `chess`).
