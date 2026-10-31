"""puts the eco.json opening book in the data lake.

bronze gets the raw json files exactly as downloaded, gold gets the cleaned lookup table
that spark and the live consumer both use. the book never changes (pinned commit), so this
runs once, after that only when you trigger it by hand.
"""

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from airflow.sdk import dag, task
from pendulum import datetime

from ingest.load_openings import COMMIT, FILES, build_rows, download
from storage import create_buckets
from storage.rustfs import get_client

# raw files go in a folder named after the pinned commit, so we always know which version it is
BRONZE_FOLDER = f"eco/{COMMIT[:8]}"


@dag(
    dag_id="opening_book",
    description="eco.json opening book: raw files to bronze, lookup table to gold",
    start_date=datetime(2026, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["chess", "batch"],
)
def opening_book():

    @task
    def make_buckets():
        """creates the buckets if they're not there yet, so the dag also works on a fresh setup."""
        create_buckets.main()

    @task
    def download_to_bronze():
        """downloads the raw json files and puts them in bronze untouched."""
        s3 = get_client()
        with tempfile.TemporaryDirectory() as tmp:
            for path in download(Path(tmp)):
                s3.upload_file(str(path), "bronze", f"{BRONZE_FOLDER}/{path.name}")
                print(f"uploaded {path.name} to bronze/{BRONZE_FOLDER}/")

    @task
    def build_lookup_in_gold():
        """reads the raw files back from bronze, cleans them and writes the lookup table to gold."""
        s3 = get_client()
        with tempfile.TemporaryDirectory() as tmp:
            paths = []
            for name in FILES:
                path = Path(tmp) / f"{name}.json"
                s3.download_file("bronze", f"{BRONZE_FOLDER}/{path.name}", str(path))
                paths.append(path)

            rows, failed = build_rows(paths)
            out = Path(tmp) / "openings.parquet"
            pq.write_table(pa.Table.from_pylist(rows), out)
            s3.upload_file(str(out), "gold", "openings/openings.parquet")

        print(f"{len(rows)} positions written to gold/openings/, {failed} entries skipped")

    make_buckets() >> download_to_bronze() >> build_lookup_in_gold()


opening_book()