"""Phase 1: parse annotated archive games into a Parquet dataset, one row per move.

    uv run python -m ingest.parse_archive data/raw/lichess_db_standard_rated_2026-09.part.pgn.zst --games 60000

Scanning runs in the main process; parsing runs in a worker pool, one batch at a time,
so the next batch is scanned while the previous one is being parsed.
"""

import argparse
import os
import time
from multiprocessing import Pool
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ingest.archive import has_evals, iter_game_texts, parse_game


def _parse_batch(texts: list[str]) -> list[dict]:
    return [row for text in texts for row in parse_game(text)]


def _annotated_batches(path: Path, games: int, batch_size: int):
    """Yield batches of annotated game texts until `games` have been collected."""
    batch: list[str] = []
    collected = scanned = 0
    for text in iter_game_texts(path):
        scanned += 1
        if not has_evals(text):
            continue
        batch.append(text)
        collected += 1
        if len(batch) == batch_size or collected == games:
            yield batch, scanned
            batch = []
        if collected == games:
            return
    if batch:
        yield batch, scanned


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path)
    parser.add_argument("--games", type=int, default=60_000, help="annotated games to parse")
    parser.add_argument("--out", type=Path, default=Path("data/parquet/moves"))
    parser.add_argument("--batch-size", type=int, default=2_000)
    parser.add_argument("--workers", type=int, default=max(1, min(8, (os.cpu_count() or 2) - 2)))
    args = parser.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    for old in args.out.glob("part-*.parquet"):
        old.unlink()

    start = time.perf_counter()
    games = rows = scanned = 0
    with Pool(args.workers) as pool:
        pending = None
        batches = _annotated_batches(args.archive, args.games, args.batch_size)
        for part, (batch, scanned_so_far) in enumerate(batches):
            submitted = pool.map_async(_parse_batch, [batch[i::args.workers] for i in range(args.workers)])
            if pending is not None:
                rows += _write(pending, args.out)
            pending = (part, submitted)
            games += len(batch)
            scanned = scanned_so_far
            print(f"scanned {scanned:,} games, collected {games:,} annotated ({time.perf_counter() - start:.0f}s)")
        if pending is not None:
            rows += _write(pending, args.out)

    elapsed = time.perf_counter() - start
    print(f"done: {games:,} annotated games of {scanned:,} scanned, {rows:,} move rows in {elapsed:.0f}s -> {args.out}")


def _write(pending, out: Path) -> int:
    part, result = pending
    rows = [row for chunk in result.get() for row in chunk]
    if rows:
        pq.write_table(pa.Table.from_pylist(rows), out / f"part-{part:05d}.parquet")
    return len(rows)


if __name__ == "__main__":
    main()
