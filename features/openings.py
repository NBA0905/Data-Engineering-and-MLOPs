"""Opening book lookup, shared by the batch lane and the live lane."""

from pathlib import Path

import pyarrow.parquet as pq


def load_book(path: Path, include_interpolated: bool = True) -> dict[str, dict]:
    """Map normalised FEN key to its opening entry (eco, eco_family, name, plies)."""
    rows = pq.read_table(path).to_pylist()
    return {
        r["fen_key"]: r
        for r in rows
        if include_interpolated or not r["is_interpolated"]
    }
