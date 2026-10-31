import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import zstandard

from ingest.archive import game_id, iter_games
from ingest.build_gold_features import PART_SIZE, _process
from ingest.build_features import _init
from ingest.replay_archive import Checkpoint, next_games
from tests.test_archive import GAME


def game(gid: str, evals: bool = True) -> str:
    text = GAME.replace("abcd1234", gid)
    return text if evals else text.replace("[%eval", "[%x")


@pytest.fixture
def archive(tmp_path):
    """Six games, every other one annotated: a1 x1 a2 x2 a3 x3."""
    texts = [game(f"{kind}{i}", kind == "a") for i in (1, 2, 3) for kind in ("a", "x")]
    path = tmp_path / "test.pgn.zst"
    path.write_bytes(zstandard.ZstdCompressor().compress("".join(texts).encode()))
    return path


def test_game_id():
    assert game_id(GAME) == "abcd1234"


def test_resume_from_end_offset(archive):
    games = list(iter_games(archive))
    assert [game_id(text) for text, _ in games] == ["a1", "x1", "a2", "x2", "a3", "x3"]
    _, end = games[2]
    assert [game_id(text) for text, _ in iter_games(archive, end)] == ["x2", "a3", "x3"]


def test_next_games_continues_after_checkpoint(archive):
    first, after = next_games(archive, Checkpoint(), 2)
    assert [(m["game_id"], m["seq"], m["batch"]) for m in first] == [("a1", 0, 0), ("a2", 1, 0)]
    # stops right after the last annotated game: x2 is scanned again next time
    assert (after.games_scanned, after.games_sent, after.batches) == (3, 2, 1)

    second, final = next_games(archive, after, 2)
    assert [(m["game_id"], m["seq"], m["batch"]) for m in second] == [("a3", 2, 1)]
    assert (final.games_scanned, final.games_sent, final.batches) == (5, 3, 2)

    nothing, _ = next_games(archive, final, 2)
    assert nothing == []


def test_process_stamps_batch_and_part(tmp_path):
    book = tmp_path / "book.parquet"
    pq.write_table(pa.table({"fen_key": ["x"], "eco_family": ["A"], "is_interpolated": [False]}), book)
    _init(book, True)
    moves, features = _process([{"pgn": GAME, "batch": 4, "seq": PART_SIZE + 5}])
    assert len(moves) == len(features) == 6
    assert {m["seq"] for m in moves} == {PART_SIZE + 5}
    assert {(f["batch"], f["part"]) for f in features} == {(4, 1)}
