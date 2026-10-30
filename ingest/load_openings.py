"""Phase 7: download the eco.json opening book and write a normalised lookup table.

    uv run python -m ingest.load_openings

Raw files land in data/raw/eco/<commit>/ (bronze), the lookup table in
data/parquet/openings/openings.parquet (gold). Each entry's move sequence is replayed
with python-chess to check that it really leads to the position in its key.
"""

import argparse
import json
import re
from pathlib import Path

import chess
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from features.fen import fen_key

# hayatbiralem/eco.json is archived; pin the commit so the book never changes under us.
COMMIT = "e397a1fa91ea6a281bff437f96a25a353051b2ce"
FILES = ["ecoA", "ecoB", "ecoC", "ecoD", "ecoE", "eco_interpolated"]
BASE_URL = f"https://raw.githubusercontent.com/hayatbiralem/eco.json/{COMMIT}"
MOVE_NUMBER = re.compile(r"\d+\.(\.\.)?")


def download(raw_dir: Path) -> list[Path]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for name in FILES:
        path = raw_dir / f"{name}.json"
        if not path.exists():
            resp = requests.get(f"{BASE_URL}/{name}.json", timeout=60)
            resp.raise_for_status()
            path.write_bytes(resp.content)
        paths.append(path)
    return paths


def move_list(moves: str) -> list[str]:
    """SAN moves from a string like '1. e4 e5 2. Nf3'; some entries omit the space ('1.e4')."""
    return MOVE_NUMBER.sub(" ", moves).split()


def replays_to(moves: list[str], key: str) -> bool:
    board = chess.Board()
    try:
        for san in moves:
            board.push_san(san)
    except ValueError:
        return False
    return fen_key(board.fen()) == key


def build_rows(paths: list[Path]) -> tuple[list[dict], int]:
    """One row per normalised position. Returns the rows and the number of entries that failed replay."""
    best: dict[str, dict] = {}
    failed = 0
    for path in paths:
        for fen, entry in json.loads(path.read_text(encoding="utf-8")).items():
            key = fen_key(fen)
            moves = move_list(entry.get("moves", ""))
            if not replays_to(moves, key):
                failed += 1
                continue
            row = {
                "fen_key": key,
                "eco": entry.get("eco"),
                "eco_family": (entry.get("eco") or "?")[0],
                "name": entry.get("name"),
                "moves": entry.get("moves"),
                "plies": len(moves),
                "src": entry.get("src"),
                "is_interpolated": entry.get("src") == "interpolated",
                "is_eco_root": bool(entry.get("isEcoRoot", False)),
            }
            # Same position reached by several move orders: prefer named over
            # interpolated entries, then the shortest move sequence.
            current = best.get(key)
            if current is None or _rank(row) < _rank(current):
                best[key] = row
    return sorted(best.values(), key=lambda r: (r["plies"], r["fen_key"])), failed


def _rank(row: dict) -> tuple:
    return (row["is_interpolated"], row["plies"], row["name"] or "")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw/eco") / COMMIT[:8])
    parser.add_argument("--out", type=Path, default=Path("data/parquet/openings/openings.parquet"))
    args = parser.parse_args()

    paths = download(args.raw_dir)
    rows, failed = build_rows(paths)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), args.out)

    interpolated = sum(r["is_interpolated"] for r in rows)
    print(f"{len(rows):,} positions ({len(rows) - interpolated:,} named, {interpolated:,} interpolated), "
          f"{failed} entries skipped because their moves do not reach their FEN -> {args.out}")


if __name__ == "__main__":
    main()
