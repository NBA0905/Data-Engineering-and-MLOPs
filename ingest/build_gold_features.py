"""Builds the feature table on RustFS from the landed archive games: bronze -> silver -> gold.

    uv run python -m ingest.build_gold_features

Stands in for the phase 5 Spark job. Every file in `bronze/archive/` without a feature file
yet is parsed with `ingest.archive.parse_game` into `silver/moves/`, then replayed through the
shared feature module (`ingest.build_features.game_features`) into `gold/features/`, under the
same file name. Games already in gold (a batch that was sent twice) are skipped.

Feature rows get `part = seq // PART_SIZE`: parts of 2,000 annotated games in archive order,
like the local parts, so training splits train and test by part in the same way.
"""

import argparse
import os
import tempfile
import time
from multiprocessing import Pool
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs

from ingest.archive import parse_game
from ingest.build_features import _init, game_features
from storage.rustfs import get_filesystem

BRONZE = "bronze/archive"
SILVER = "silver/moves"
GOLD = "gold/features"
BOOK = "gold/openings/openings.parquet"
PART_SIZE = 2_000


def _process(games: list[dict]) -> tuple[list[dict], list[dict]]:
    """Move rows and feature rows for a chunk of bronze games."""
    moves, features = [], []
    for game in games:
        rows = parse_game(game["pgn"])
        moves += [{**r, "batch": game["batch"], "seq": game["seq"]} for r in rows]
        features += [{**f, "batch": game["batch"], "part": game["seq"] // PART_SIZE} for f in game_features(rows)]
    return moves, features


def _names(lake: fs.FileSystem, folder: str) -> list[str]:
    infos = lake.get_file_info(fs.FileSelector(folder, allow_not_found=True))
    return sorted(i.base_name for i in infos if i.base_name.endswith(".parquet"))


def _write(rows: list[dict], key: str, lake: fs.FileSystem) -> None:
    table = pa.Table.from_pylist(rows)
    # A column that is empty in every row (no titled player in a small batch) comes out as type
    # null. Store it as text like in the other files, or the folder no longer reads as one table.
    for i, field in enumerate(table.schema):
        if pa.types.is_null(field.type):
            table = table.set_column(i, field.name, table.column(i).cast(pa.string()))
    pq.write_table(table, key, filesystem=lake)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=max(1, min(8, (os.cpu_count() or 2) - 2)))
    args = parser.parse_args(argv)

    lake = get_filesystem()
    done = set(_names(lake, GOLD))
    todo = [name for name in _names(lake, BRONZE) if name not in done]
    if not todo:
        print(f"no new files in {BRONZE}/")
        return
    seen = set(pq.read_table(GOLD, columns=["game_id"], filesystem=lake).column("game_id").to_pylist()) if done else set()

    start = time.perf_counter()
    with tempfile.TemporaryDirectory() as tmp:
        book = Path(tmp) / "openings.parquet"
        try:
            fs.copy_files(BOOK, str(book), source_filesystem=lake, destination_filesystem=fs.LocalFileSystem())
        except FileNotFoundError:
            raise SystemExit(f"no opening book at {BOOK}: run the opening_book DAG first") from None

        with Pool(args.workers, initializer=_init, initargs=(book, True)) as pool:
            for name in todo:
                games = []
                for game in pq.read_table(f"{BRONZE}/{name}", filesystem=lake).to_pylist():
                    if game["game_id"] not in seen:
                        seen.add(game["game_id"])
                        games.append(game)
                chunks = pool.map(_process, [games[i :: args.workers] for i in range(args.workers)])
                moves = [row for m, _ in chunks for row in m]
                features = [row for _, f in chunks for row in f]
                if features:  # a file of only duplicates writes nothing and is checked again next run
                    _write(moves, f"{SILVER}/{name}", lake)
                    _write(features, f"{GOLD}/{name}", lake)
                print(f"{name}: {len(games):,} new games, {len(moves):,} moves -> {SILVER}/, "
                      f"{len(features):,} feature rows -> {GOLD}/ ({time.perf_counter() - start:.0f}s)")


if __name__ == "__main__":
    main()
