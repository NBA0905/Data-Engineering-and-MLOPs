"""Train/serve parity: one game through the batch lane and through the live consumer."""

import chess
import pytest

from consumer.live_game import LiveGame
from features.fen import fen_key
from ingest import build_features
from ingest.archive import parse_game

SANS = ["e4", "e5", "Nf3", "Nc6", "Bb5", "a6", "Ba4", "Nf6", "O-O", "Be7", "Re1", "b5", "Bb3", "d6",
        "c3", "O-O", "h3", "Nb8", "d4", "Nbd7", "Nbd2", "Bb7", "Bc2", "Re8", "Nf1", "Bf8", "Ng3", "g6",
        "a4", "c5", "d5", "c4", "Bg5", "h6", "Be3", "Nc5", "Qd2", "h5", "Bg5", "Be7", "Bxf6", "Bxf6"]
CLOCKS = [180, 180, 178, 177, 175, 171, 174, 166, 172, 160, 171, 158, 169, 150, 168, 149, 160, 141, 158,
          139, 157, 130, 150, 128, 149, 121, 148, 119, 140, 118, 133, 110, 131, 108, 130, 101, 128, 99,
          125, 95, 124, 94]
EVALS = [0.2 + 0.01 * i for i in range(len(SANS))]
META = {"game_id": "abcd1234", "channel": "blitz", "white_name": "alice", "black_name": "bob",
        "white_title": None, "black_title": None, "white_rating": 1500, "black_rating": 1600,
        "tc_base": 180, "tc_inc": 2, "variant": "standard"}


def boards() -> list[chess.Board]:
    """The position after every move, index = ply."""
    board = chess.Board()
    out = [board.copy()]
    for san in SANS:
        board.push_san(san)
        out.append(board.copy())
    return out


# A tiny opening book: the Ruy Lopez up to 3... a6, so the game leaves the book at ply 6.
BOOK = {fen_key(b.fen()): {"eco": "C70", "eco_family": "C", "name": "Ruy Lopez"} for b in boards()[1:7]}


def pgn() -> str:
    moves = []
    for i, (san, clk, ev) in enumerate(zip(SANS, CLOCKS, EVALS)):
        number = f"{i // 2 + 1}." if i % 2 == 0 else f"{i // 2 + 1}..."
        moves.append(f"{number} {san} {{ [%eval {ev:.2f}] [%clk 0:{clk // 60:02d}:{clk % 60:02d}] }}")
    return ('[Site "https://lichess.org/abcd1234"]\n[WhiteElo "1500"]\n[BlackElo "1600"]\n'
            '[TimeControl "180+2"]\n[Result "*"]\n\n' + " ".join(moves) + " *\n")


@pytest.fixture(scope="module")
def batch() -> dict[int, dict]:
    """Batch lane features by board ply (the archive row with ply n is the position at ply n - 1)."""
    build_features._book = BOOK
    rows = build_features.game_features(parse_game(pgn()))
    return {r["ply"] - 1: {k: r[k] for k in build_features.FEATURES} for r in rows}


def clocks_after(ply: int) -> tuple[int, int]:
    """Both clocks in the position at `ply`, as the TV feed sends them."""
    white = [CLOCKS[i] for i in range(0, ply, 2)]
    black = [CLOCKS[i] for i in range(1, ply, 2)]
    return (white[-1] if white else 180), (black[-1] if black else 180)


def live(game: LiveGame, plies) -> dict[int, dict]:
    out = {}
    for ply in plies:
        features = game.position(boards()[ply].fen(), *clocks_after(ply), BOOK)
        if features is not None:
            out[ply] = features
    return out


def test_watched_from_the_start_matches_the_batch_lane(batch):
    got = live(LiveGame(META), range(0, len(SANS)))
    for ply, expected in batch.items():
        assert got[ply] == expected, f"ply {ply}"


@pytest.mark.parametrize("joined_at", [1, 9, 20])
def test_joined_midway_with_full_history_matches_the_batch_lane(batch, joined_at):
    game = LiveGame(META)
    game.replay(SANS[:joined_at], CLOCKS[:joined_at], BOOK)
    got = live(game, range(joined_at, len(SANS)))
    for ply in range(max(joined_at, 1), len(SANS)):
        assert got[ply] == batch[ply], f"ply {ply}"


def test_lagging_history_leaves_a_gap_but_board_features_still_match(batch):
    game = LiveGame(META)
    game.replay(SANS[:17], CLOCKS[:17], BOOK)  # the export stops 3 moves before the live position
    got = live(game, range(20, len(SANS)))
    assert set(got) == set(range(20, len(SANS)))
    assert got[20]["prev_time_spent"] == batch[18]["prev_time_spent"]  # nothing newer is known
    for ply in range(22, len(SANS)):  # two moves later the time history has caught up again
        assert got[ply]["prev_time_spent"] == batch[ply]["prev_time_spent"]
    for ply in got:
        for name in ("material_balance", "legal_moves", "clock", "opp_clock", "plies_out_of_book", "eco_family"):
            assert got[ply][name] == batch[ply][name], f"{name} at ply {ply}"


def test_no_history_at_all_still_scores(batch):
    got = live(LiveGame(META), range(25, 30))
    assert set(got) == set(range(25, 30))
    assert got[25]["prev_time_spent"] is None
    assert got[25]["material_change"] == 0  # unknown history counts as unchanged
    assert got[26]["prev_time_spent"] is None  # this mover has not moved since we joined either
    assert got[27]["prev_time_spent"] == batch[27]["prev_time_spent"]  # from the feed clocks at ply 25 and 27


def test_a_position_is_scored_once():
    game = LiveGame(META)
    assert game.position(boards()[5].fen(), 175, 171, BOOK) is not None
    assert game.position(boards()[5].fen(), 175, 171, BOOK) is None
    assert game.position(boards()[4].fen(), 175, 177, BOOK) is None


def test_opening_is_the_last_book_position_reached():
    game = LiveGame(META)
    live(game, range(0, 12))
    assert game.opening["name"] == "Ruy Lopez"


@pytest.mark.parametrize("change, scored", [
    ({}, True),
    ({"black_title": "BOT"}, False),
    ({"variant": "chess960"}, False),
    ({"tc_base": 15, "tc_inc": 0}, False),  # UltraBullet
    ({"tc_base": None, "tc_inc": None}, True),  # no time control: the channel name is the speed
])
def test_which_games_are_scored(change, scored):
    assert LiveGame({**META, **change}).is_scored() == scored
