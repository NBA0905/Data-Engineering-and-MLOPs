"""parity test: the spark job must give the same features as the python path (GameState).

the live consumer uses GameState, training uses the spark job. if they disagree, the model gets
different numbers live than it was trained on and its predictions quietly get worse.

needs spark connect running (docker compose up) and the local opening book
(uv run python -m ingest.load_openings), otherwise the test is skipped.
"""

import random
import socket
from pathlib import Path

import chess
import chess.engine
import chess.pgn
import pandas as pd
import pyarrow as pa
import pytest

from features import FEATURES
from features.openings import load_book
from ingest import build_features as python_path
from ingest.parse_bronze import fix_empty_columns, parse_games

BOOK = Path("data/parquet/openings/openings.parquet")


def spark_is_running():
    try:
        socket.create_connection(("localhost", 15002), timeout=2).close()
        return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not (spark_is_running() and BOOK.exists()),
                                reason="needs spark connect on localhost:15002 and the local opening book")


def make_game(seed, skip_eval_at=None, black_title=None):
    """a random game with fake evals and clocks, starting with a real opening so the book is used."""
    rnd = random.Random(seed)
    game = chess.pgn.Game()
    game.headers["Site"] = f"https://lichess.org/test{seed:04d}"
    game.headers["WhiteElo"] = str(rnd.randint(1200, 2400))
    game.headers["BlackElo"] = str(rnd.randint(1200, 2400))
    game.headers["TimeControl"] = "180+2"
    if black_title:
        game.headers["BlackTitle"] = black_title

    board = chess.Board()
    node = game
    clocks = {chess.WHITE: 180.0, chess.BLACK: 180.0}
    opening = ["e4", "c5", "Nf3", "d6", "d4", "cxd4"]
    for i in range(rnd.randint(30, 70)):
        if i < len(opening):
            move = board.parse_san(opening[i])
        else:
            legal = list(board.legal_moves)
            if not legal:
                break
            move = rnd.choice(legal)
        mover = board.turn
        board.push(move)
        node = node.add_variation(move)
        clocks[mover] = max(1.0, clocks[mover] - rnd.randint(0, 10) + 2)
        if i != skip_eval_at:  # a move without eval leaves a gap, like in some real games
            node.set_eval(chess.engine.PovScore(chess.engine.Cp(rnd.randint(-400, 400)), chess.WHITE))
        node.set_clock(clocks[mover])
        if board.is_game_over():
            break
    return str(game) + "\n\n"


def test_spark_features_match_python_features():
    from pyspark.sql import SparkSession

    from spark.build_features import build_features, clean

    games = [{"game_id": f"test{s:04d}", "batch": 0, "seq": s,
              "pgn": make_game(s, skip_eval_at=15 if s % 3 == 0 else None, black_title="BOT" if s == 7 else None)}
             for s in range(10)]
    duplicate = dict(games[2], seq=99)  # the same game sent again later
    rows = parse_games(games + [duplicate])

    # python path: GameState per game, exactly like ingest/build_features.py
    python_path._book = load_book(BOOK)
    by_game = {}
    for row in parse_games(games):
        by_game.setdefault(row["game_id"], []).append(row)
    expected = []
    for game_rows in by_game.values():
        expected += python_path.game_features(sorted(game_rows, key=lambda r: r["ply"]))
    expected = pd.DataFrame(expected)
    expected = expected[expected.game_id != "test0007"]  # the bot game, spark drops it while cleaning
    expected = expected.set_index(["game_id", "ply"]).sort_index()

    # spark path
    spark = SparkSession.builder.remote("sc://localhost:15002").appName("parity-test").getOrCreate()
    moves_table = pa.Table.from_pylist(rows)
    moves = spark.createDataFrame(moves_table.cast(fix_empty_columns(moves_table.schema)))
    openings = spark.createDataFrame(pa.Table.from_pylist(list(python_path._book.values())))
    actual = build_features(clean(moves), openings).toPandas().set_index(["game_id", "ply"]).sort_index()

    assert list(actual.index) == list(expected.index)  # same moves, bot game and duplicate gone
    for name in FEATURES:
        if name == "eco_family":
            assert actual[name].fillna("-").tolist() == expected[name].fillna("-").tolist(), name
        else:
            pd.testing.assert_series_equal(actual[name].astype("float64"), expected[name].astype("float64"),
                                           check_names=False, obj=name)