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
