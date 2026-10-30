from pathlib import Path

import chess
import pytest

from features.fen import fen_key
from features.openings import load_book
from ingest.load_openings import _rank, move_list, replays_to

BOOK = Path("data/parquet/openings/openings.parquet")


def test_move_list_handles_both_number_styles():
    assert move_list("1. e4 e5 2. Nf3") == ["e4", "e5", "Nf3"]
    assert move_list("1.e4 e5 2.Nf3 Nc6") == ["e4", "e5", "Nf3", "Nc6"]
    assert move_list("1. f4  e5 2. fxe5") == ["f4", "e5", "fxe5"]


def test_replays_to():
    key = fen_key("rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1")
    assert replays_to(["e4"], key)
    assert not replays_to(["d4"], key)
    assert not replays_to(["e4", "e4"], key)  # illegal move


def test_named_entry_beats_interpolated_and_shorter_beats_longer():
    named = {"is_interpolated": False, "plies": 6, "name": "b"}
    interpolated = {"is_interpolated": True, "plies": 2, "name": "a"}
    longer = {"is_interpolated": False, "plies": 8, "name": "a"}
    assert sorted([interpolated, longer, named], key=_rank)[0] is named


@pytest.mark.skipif(not BOOK.exists(), reason="run `uv run python -m ingest.load_openings` first")
@pytest.mark.parametrize(
    ("moves", "eco", "name_part"),
    [
        (["e4"], "B00", "King's Pawn"),
        (["d4", "d5", "c4"], "D06", "Queen's Gambit"),
        (["e4", "c5"], "B20", "Sicilian"),
    ],
)
def test_known_openings_resolve(moves, eco, name_part):
    book = load_book(BOOK)
    board = chess.Board()
    for san in moves:
        board.push_san(san)
    entry = book[fen_key(board.fen())]
    assert entry["eco"] == eco
    assert name_part in entry["name"]
