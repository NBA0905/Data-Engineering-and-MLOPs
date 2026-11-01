"""Phase 2: turn the parsed moves into a feature table with the shared feature module.

    uv run python -m ingest.build_features

Each game is replayed through `compute_features` with its own GameState, the same code
path the live consumer uses. Reads data/parquet/moves/, writes one feature part per
moves part to data/parquet/features/ (the part number is kept: parts are in archive
order, so the notebook splits train and test by part).
"""

import argparse
import os
import time
from multiprocessing import Pool
from pathlib import Path

import chess
import pyarrow as pa
import pyarrow.parquet as pq

from features import FEATURES, GameState, compute_features
from features.clock import time_spent
from features.openings import load_book

# Columns carried over from the moves table: identifiers, filters, label and validation.
KEEP = ["game_id", "ply", "speed", "white_title", "black_title", "wp_drop", "lichess_nag"]

_book: dict[str, dict] = {}


def _init(book_path: Path, include_interpolated: bool) -> None:
    global _book
    _book = load_book(book_path, include_interpolated)


def game_features(rows: list[dict]) -> list[dict]:
    """Feature rows for one game, `rows` being its move rows in ply order."""
    state = GameState()
    # The parser skips ply 1 (no eval before it), but white's clock at ply 3 tells us
    # how long that move took, so prev_time_spent at ply 3 matches the live lane.
    if len(rows) > 1 and rows[0]["ply"] == 2 and rows[1]["ply"] == 3:
        r = rows[1]
        state.record_move(chess.WHITE, time_spent(r["tc_base"], r["clock_before"], r["tc_inc"]))
    out = []
    for r in rows:
        board = chess.Board(r["fen_before"])
        f = compute_features(board, {"mover": r["clock_before"], "opp": r["opp_clock"]}, r, state, _book)
        state.record_move(board.turn, r["time_spent"])
        out.append({**{k: r[k] for k in KEEP}, **f})
    return out


def _build_chunk(games: list[list[dict]]) -> list[dict]:
    return [row for rows in games for row in game_features(rows)]


def _games(path: Path) -> list[list[dict]]:
    by_game: dict[str, list[dict]] = {}
    for r in pq.read_table(path).to_pylist():
        by_game.setdefault(r["game_id"], []).append(r)
    return [sorted(rows, key=lambda r: r["ply"]) for rows in by_game.values()]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--moves", type=Path, default=Path("data/parquet/moves"))
    parser.add_argument("--book", type=Path, default=Path("data/parquet/openings/openings.parquet"))
    parser.add_argument("--out", type=Path, default=Path("data/parquet/features"))
    parser.add_argument("--no-interpolated", action="store_true", help="named openings only")
    parser.add_argument("--workers", type=int, default=max(1, min(8, (os.cpu_count() or 2) - 2)))
    args = parser.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    for old in args.out.glob("part-*.parquet"):
        old.unlink()

    start = time.perf_counter()
    total = 0
    with Pool(args.workers, initializer=_init, initargs=(args.book, not args.no_interpolated)) as pool:
        for path in sorted(args.moves.glob("part-*.parquet")):
            games = _games(path)
            chunks = pool.map(_build_chunk, [games[i :: args.workers] for i in range(args.workers)])
            rows = [row for chunk in chunks for row in chunk]
            part = int(path.stem.split("-")[1])
            table = pa.Table.from_pylist([{**row, "part": part} for row in rows])
            pq.write_table(table, args.out / path.name)
            total += len(rows)
            print(f"{path.name}: {len(games):,} games, {len(rows):,} rows ({time.perf_counter() - start:.0f}s)")

    print(f"done: {total:,} feature rows ({len(FEATURES)} features) in {time.perf_counter() - start:.0f}s -> {args.out}")


if __name__ == "__main__":
    main()
