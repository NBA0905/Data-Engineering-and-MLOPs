"""batch lane: parses the landed archive games into one row per move, with the board features.

run with: uv run python -m ingest.parse_bronze

this is the part spark can't do well: reading pgn and the board features need python-chess,
position by position, which is far too slow inside spark. so plain python does it here, with
a few processes, and the spark job (spark/build_features.py) does the cleaning and combining.

every file in bronze/archive/ that has no moves file yet gets one in bronze/moves/, same name.
"""

import argparse
import os
import time
from multiprocessing import Pool

import chess
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs

from features.board import board_features
from ingest.archive import parse_game
from storage.rustfs import get_filesystem

ARCHIVE = "bronze/archive"
MOVES = "bronze/moves"
CHUNK = 2000  # games parsed at a time, keeps memory low on big batches


def parse_games(games):
    """move rows plus board features for a list of landed games."""
    rows = []
    for game in games:
        for row in parse_game(game["pgn"]):
            board = chess.Board(row["fen_before"])
            rows.append({**row, **board_features(board), "batch": game["batch"], "seq": game["seq"]})
    return rows


def file_names(lake, folder):
    infos = lake.get_file_info(fs.FileSelector(folder, allow_not_found=True))
    return sorted(info.base_name for info in infos if info.base_name.endswith(".parquet"))


def fix_empty_columns(schema):
    """a column that is empty in every row (no titled players) gets type null, store it as text."""
    return pa.schema([pa.field(f.name, pa.string()) if pa.types.is_null(f.type) else f for f in schema])


def parse_file(name, lake, pool, workers):
    games = pq.read_table(f"{ARCHIVE}/{name}", columns=["game_id", "batch", "seq", "pgn"], filesystem=lake).to_pylist()
    writer = None
    total = 0
    for start in range(0, len(games), CHUNK):
        chunk = games[start:start + CHUNK]
        # split the chunk over the worker processes, every worker gets every n-th game
        results = pool.map(parse_games, [chunk[i::workers] for i in range(workers)])
        rows = [row for result in results for row in result]
        if not rows:
            continue
        if writer is None:
            # the first chunk decides the column types for the whole file
            schema = fix_empty_columns(pa.Table.from_pylist(rows).schema)
            writer = pq.ParquetWriter(f"{MOVES}/{name}", schema, filesystem=lake)
        writer.write_table(pa.Table.from_pylist(rows, schema=schema))
        total += len(rows)
    if writer is not None:
        writer.close()
    return len(games), total


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=max(1, min(8, (os.cpu_count() or 2) - 2)))
    args = parser.parse_args(argv)

    lake = get_filesystem()
    done = set(file_names(lake, MOVES))
    todo = [name for name in file_names(lake, ARCHIVE) if name not in done]
    if not todo:
        print(f"no new files in {ARCHIVE}/")
        return

    start = time.perf_counter()
    with Pool(args.workers) as pool:
        for name in todo:
            games, rows = parse_file(name, lake, pool, args.workers)
            print(f"{name}: {games:,} games -> {rows:,} moves in {MOVES}/ ({time.perf_counter() - start:.0f}s)")


if __name__ == "__main__":
    main()