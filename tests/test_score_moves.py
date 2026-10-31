from consumer.score_moves import Scorer
from tests.test_live_game import BOOK, CLOCKS, META, SANS, boards, clocks_after


class FakeChampion:
    version = 7

    def refresh(self):
        pass

    def score(self, features):
        return 0.25


def start(game_id="abcd1234", moves=10, **change):
    # the featured fen comes with its move counters reset, like on the real feed
    fen = boards()[moves].fen().rsplit(" ", 2)[0] + " 0 1"
    return {**META, **change, "game_id": game_id, "type": "start", "fen": fen,
            "moves_so_far": " ".join(SANS[:moves]), "clocks_so_far": CLOCKS[:moves], "received_at": 0.0}


def move(ply, game_id="abcd1234", **change):
    white, black = clocks_after(ply)
    return {**META, **change, "game_id": game_id, "type": "move", "fen": boards()[ply].fen(),
            "last_move": boards()[ply].peek().uci(), "white_clock": white, "black_clock": black,
            "received_at": 0.0}


def test_start_warms_up_and_the_next_move_is_scored():
    scorer = Scorer(BOOK, FakeChampion())
    assert scorer.predict(start()) is None
    row = scorer.predict(move(11))
    assert (row["ply"], row["mover"], row["blunder_prob"], row["model_version"]) == (11, "black", 0.25, 7)
    assert row["features"].adapted["prev_time_spent"] is not None  # known from the history
    assert row["opening"] == "Ruy Lopez"
    assert scorer.predict(move(11)) is None  # the same position again after a reconnect


def test_a_new_game_on_the_channel_forgets_the_old_one():
    scorer = Scorer(BOOK, FakeChampion())
    scorer.predict(start("game1"))
    scorer.predict(start("game2"))
    assert set(scorer.games) == {"game2"}


def test_bot_games_are_not_scored():
    scorer = Scorer(BOOK, FakeChampion())
    assert scorer.predict(start(black_title="BOT")) is None
    assert scorer.predict(move(11, black_title="BOT")) is None
