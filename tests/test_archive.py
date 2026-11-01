import pytest

from features.clock import speed
from features.fen import fen_key
from ingest.archive import CP_CLAMP, has_evals, parse_game, win_probability

GAME = """[Event "Rated Blitz game"]
[Site "https://lichess.org/abcd1234"]
[WhiteElo "1500"]
[BlackElo "1600"]
[BlackTitle "BOT"]
[TimeControl "180+2"]
[Result "1-0"]

1. e4 { [%eval 0.2] [%clk 0:03:00] } 1... e5 { [%eval 0.25] [%clk 0:03:01] } 2. Qh5 { [%eval 0.0] [%clk 0:02:55] } 2... Nc6 { [%eval 0.1] [%clk 0:02:50] } 3. Bc4 { [%eval 0.0] [%clk 0:02:50] } 3... Nf6?? { [%eval #1] [%clk 0:02:40] } 4. Qxf7# { [%clk 0:02:48] } 1-0
"""


@pytest.fixture(scope="module")
def rows():
    return parse_game(GAME)


def test_fen_key_drops_en_passant_and_counters():
    assert fen_key("rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1") == (
        "rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq"
    )


def test_has_evals():
    assert has_evals(GAME)
    assert not has_evals(GAME.replace("[%eval", "[%x"))


def test_one_row_per_move_except_first(rows):
    assert [r["ply"] for r in rows] == [2, 3, 4, 5, 6, 7]
    assert rows[0]["game_id"] == "abcd1234"
    assert rows[0]["black_title"] == "BOT"


def test_eval_before_comes_from_previous_ply_in_mover_view(rows):
    black_e5 = rows[0]
    assert black_e5["mover"] == "b"
    assert black_e5["cp_before"] == pytest.approx(-20)
    assert black_e5["cp_after"] == pytest.approx(-25)


def test_mate_is_clamped_and_labelled(rows):
    blunder = rows[4]
    assert blunder["move_uci"] == "g8f6"
    assert blunder["cp_after"] == -CP_CLAMP
    assert blunder["wp_drop"] > 0.4
    assert blunder["lichess_nag"] == "??"


def test_checkmating_move_without_eval_counts_as_won(rows):
    mate = rows[5]
    assert mate["move_uci"] == "h5f7"
    assert mate["cp_before"] == CP_CLAMP
    assert mate["cp_after"] == CP_CLAMP
    assert mate["wp_drop"] == pytest.approx(0)


def test_clocks(rows):
    white_qh5 = rows[1]
    assert white_qh5["clock_before"] == 180
    assert white_qh5["clock_after"] == 175
    assert white_qh5["time_spent"] == 7  # 180 - 175 + 2 increment
    assert white_qh5["opp_clock"] == 181


@pytest.mark.parametrize(
    ("base", "inc", "expected"),
    [(15, 0, "ultraBullet"), (60, 0, "bullet"), (120, 1, "bullet"), (180, 2, "blitz"), (600, 0, "rapid"), (1800, 20, "classical")],
)
def test_speed(base, inc, expected):
    assert speed(base, inc) == expected


def test_win_probability_is_symmetric():
    assert win_probability(0) == pytest.approx(0.5)
    assert win_probability(300) + win_probability(-300) == pytest.approx(1)
