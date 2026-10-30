"""Sanity check for the downloaded archive slice.

Decodes the first N games and reports how many carry engine evaluations.

    uv run python -m ingest.check_archive data/raw/lichess_db_standard_rated_2026-09.part.pgn.zst
"""

import argparse
import io
from pathlib import Path

import zstandard

EXPECTED_BYTES = 3 * 1024**3


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path)
    parser.add_argument("--games", type=int, default=50_000)
    args = parser.parse_args()

    size = args.path.stat().st_size
    print(f"file size: {size:,} bytes", "(complete)" if size == EXPECTED_BYTES else f"(expected {EXPECTED_BYTES:,})")

    games = evals = 0
    with args.path.open("rb") as fh:
        reader = zstandard.ZstdDecompressor().stream_reader(fh)
        for line in io.TextIOWrapper(reader, encoding="utf-8"):
            if line.startswith("[Event "):
                if games == args.games:
                    break
                games += 1
            elif line.startswith("1. ") and "[%eval" in line:
                evals += 1

    print(f"games read: {games:,}, with evals: {evals:,} ({evals / games:.1%})")


if __name__ == "__main__":
    main()
