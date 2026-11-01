import pytest
import zstandard

from ingest.archive import game_id, iter_games
from ingest.parse_bronze import parse_games
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


def test_parse_games_stamps_batch_and_seq():
    # the parse step keeps each game's batch and seq, spark later turns seq into the part number
    rows = parse_games([{"pgn": GAME, "batch": 4, "seq": 2005}])
    assert len(rows) == 6
    assert {(r["batch"], r["seq"]) for r in rows} == {(4, 2005)}
    # and the board features are added to every move
    assert "legal_moves" in rows[0] and "material_balance" in rows[0]
