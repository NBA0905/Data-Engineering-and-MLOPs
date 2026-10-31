"""dev helper: plays random games and writes made-up predictions, so the live page can be built and
tested without the live consumer. not part of the pipeline, the real predictions come from
the consumer. it writes to the same live_predictions table, with game ids starting with 'fake'.

run with: uv run python -m dashboard.fake_live
"""

import random
import time

import chess
import psycopg2
from psycopg2.extras import Json

from consumer.score_moves import CREATE_TABLE, INSERT
from dashboard.lake import CHESS_DB_URL


def play_game(cur, channel):
    board = chess.Board()
    game_id = f"fake{random.randint(1000, 9999)}"
    clocks = {chess.WHITE: 600.0, chess.BLACK: 600.0}
    print(f"new fake game {game_id} on {channel}")
    while not board.is_game_over() and board.ply() < 80:
        move = random.choice(list(board.legal_moves))
        clocks[board.turn] -= random.uniform(1, 15)
        board.push(move)
        mover = "white" if board.turn == chess.WHITE else "black"  # the player about to move
        in_book = board.ply() < 8
        cur.execute(INSERT, {
            "game_id": game_id, "ply": board.ply(), "channel": channel, "speed": channel,
            "fen": board.fen(), "last_move": move.uci(), "mover": mover,
            "white_name": "FakeWhite", "black_name": "FakeBlack", "white_rating": 2100, "black_rating": 2050,
            "white_clock": max(int(clocks[chess.WHITE]), 0), "black_clock": max(int(clocks[chess.BLACK]), 0),
            "eco": "B20" if in_book else None, "opening": "Sicilian Defense" if in_book else None,
            "in_book": in_book, "plies_out_of_book": max(board.ply() - 8, 0),
            "blunder_prob": random.betavariate(1.2, 12), "model_version": 1,
            "features": Json({}), "received_at": time.time(),
        })
        time.sleep(2)


def main():
    conn = psycopg2.connect(CHESS_DB_URL)
    conn.autocommit = True  # every insert is visible to the dashboard straight away
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE)
        while True:
            play_game(cur, "rapid")


if __name__ == "__main__":
    main()