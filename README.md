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

## Local setup (Windows, PowerShell)

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Archive: we only use the first 3 GB of one monthly file, which holds far more annotated games than the project needs.

```powershell
curl.exe -r 0-3221225471 -o data\raw\lichess_db_standard_rated_2026-09.part.pgn.zst https://database.lichess.org/standard/lichess_db_standard_rated_2026-09.pgn.zst
```

The download is cut off mid-frame, so the last game in the file is incomplete. The parser stops cleanly at the end of the data.
