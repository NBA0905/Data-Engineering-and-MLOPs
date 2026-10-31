"""dev helper: plays random games and writes made-up predictions, so the live page can be built and
tested before the live consumer exists. not part of the pipeline, the real predictions come from
the consumer.

run with: uv run python -m dashboard.fake_live
"""

import random
import time

import chess
import psycopg2

from dashboard.lake import CHESS_DB_URL

# the table the live consumer writes to (agreed with the consumer side)
CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS predictions (
    id            SERIAL PRIMARY KEY,
    created_at    TIMESTAMPTZ DEFAULT now(),
    channel       TEXT,
    game_id       TEXT,
    ply           INTEGER,
    fen           TEXT,
    last_move     TEXT,
    mover         TEXT,
    white_name    TEXT, white_rating INTEGER, white_clock REAL,
    black_name    TEXT, black_rating INTEGER, black_clock REAL,
    opening_name  TEXT,
    in_book       BOOLEAN,
    blunder_prob  REAL,
    model_version INTEGER
)
"""

INSERT = """
INSERT INTO predictions (channel, game_id, ply, fen, last_move, mover, white_name, white_rating, white_clock,
                         black_name, black_rating, black_clock, opening_name, in_book, blunder_prob, model_version)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def play_game(cur, channel):
    board = chess.Board()
    game_id = f"fake{random.randint(1000, 9999)}"
    clocks = {chess.WHITE: 600.0, chess.BLACK: 600.0}
    print(f"new fake game {game_id} on {channel}")
    while not board.is_game_over() and board.ply() < 80:
        move = random.choice(list(board.legal_moves))
        clocks[board.turn] -= random.uniform(1, 15)
        board.push(move)
        mover = "w" if board.turn == chess.WHITE else "b"  # the player about to move
        cur.execute(INSERT, (channel, game_id, board.ply(), board.fen(), move.uci(), mover,
                             "FakeWhite", 2100, max(clocks[chess.WHITE], 0), "FakeBlack", 2050, max(clocks[chess.BLACK], 0),
                             "Sicilian Defense" if board.ply() < 8 else None, board.ply() < 8,
                             random.betavariate(1.2, 12), 1))
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