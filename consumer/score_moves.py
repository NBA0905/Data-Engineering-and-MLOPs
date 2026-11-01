"""Live consumer: scores every position on the TV feed with the champion model.

    uv run python -m consumer.score_moves

Reads `tv.moves`, keeps one `LiveGame` per game id, computes the features with the shared
feature module and writes the blunder probability of the move about to be played to the
`live_predictions` table (Postgres, database `chess`), which the dashboard polls.

The opening book comes from `gold/openings/` on RustFS, the model from `models:/blunder@champion`.
Every few minutes the consumer checks whether the champion alias has moved (a retrain or a
manual rollback) and loads the new version. A long-running service in Docker Compose, not an
Airflow task.

Offsets are stored only after the prediction is written, and writes are idempotent per
(game id, ply), so a crash or restart never loses or doubles a prediction.
"""

import argparse
import json
import os
import tempfile
import time
from pathlib import Path

import mlflow.sklearn
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from mlflow import MlflowClient
from psycopg2.extras import Json
from pyarrow import fs

from consumer.live_game import COLORS, LiveGame
from features import FEATURES, model_input
from features.openings import load_book
from storage.rustfs import get_filesystem
from storage.tracking import connect
from training.train_model import CHAMPION, MODEL_NAME

TOPIC = "tv.moves"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
GROUP = "live-scorer"
BOOK = "gold/openings/openings.parquet"
DB_URL = os.environ.get("CHESS_DB_URL", "postgresql://airflow:airflow@localhost:5432/chess")
MODEL_CHECK_EVERY = 300  # seconds between checks whether the champion alias moved
GAME_TIMEOUT = 1800  # seconds without a message before a game is forgotten

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS live_predictions (
    game_id           text NOT NULL,
    ply               integer NOT NULL,     -- position, counted like python-chess board.ply()
    channel           text,
    speed             text,
    fen               text NOT NULL,        -- the position the mover is about to move in
    last_move         text NOT NULL,        -- the move that led to it (UCI)
    mover             text NOT NULL,        -- 'white' or 'black'
    white_name        text,
    black_name        text,
    white_rating      integer,
    black_rating      integer,
    white_clock       integer,              -- seconds
    black_clock       integer,
    eco               text,                 -- last opening book position reached
    opening           text,
    in_book           boolean NOT NULL,
    plies_out_of_book integer NOT NULL,
    blunder_prob      double precision NOT NULL,
    model_version     integer NOT NULL,
    features          jsonb NOT NULL,       -- the model input, for the dashboard and for debugging
    received_at       timestamptz,          -- when the producer read the event from lichess
    scored_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (game_id, ply)
);
CREATE INDEX IF NOT EXISTS live_predictions_scored_at ON live_predictions (scored_at);
"""

INSERT = """
INSERT INTO live_predictions (game_id, ply, channel, speed, fen, last_move, mover, white_name, black_name,
    white_rating, black_rating, white_clock, black_clock, eco, opening, in_book, plies_out_of_book,
    blunder_prob, model_version, features, received_at)
VALUES (%(game_id)s, %(ply)s, %(channel)s, %(speed)s, %(fen)s, %(last_move)s, %(mover)s, %(white_name)s,
    %(black_name)s, %(white_rating)s, %(black_rating)s, %(white_clock)s, %(black_clock)s, %(eco)s,
    %(opening)s, %(in_book)s, %(plies_out_of_book)s, %(blunder_prob)s, %(model_version)s, %(features)s,
    to_timestamp(%(received_at)s))
ON CONFLICT (game_id, ply) DO NOTHING
"""


class Champion:
    """The champion model, reloaded when the alias moves to another version."""

    def __init__(self):
        self.client = MlflowClient()
        self.version: int | None = None
        self.model = None
        self.checked_at = 0.0

    def refresh(self) -> None:
        if time.monotonic() - self.checked_at < MODEL_CHECK_EVERY and self.model is not None:
            return
        self.checked_at = time.monotonic()
        try:
            current = self.client.get_model_version_by_alias(MODEL_NAME, CHAMPION)
        except Exception as error:  # MLflow unreachable: keep scoring with the model we have
            if self.model is None:
                raise
            print(f"could not check the champion ({error}), keeping v{self.version}")
            return
        if int(current.version) == self.version:
            return
        # the model only makes sense on the features it was trained on, in that order
        trained_on = self.client.get_run(current.run_id).data.tags.get("features", "").split(",")
        if trained_on != FEATURES:
            message = f"{MODEL_NAME} v{current.version} was trained on other features than features.FEATURES"
            if self.model is None:
                raise SystemExit(message)
            print(f"{message}, keeping v{self.version}")
            return
        self.model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{current.version}")
        self.version = int(current.version)
        print(f"loaded {MODEL_NAME} v{self.version} ({CHAMPION})")

    def score(self, features: dict) -> float:
        return float(self.model.predict_proba(model_input([features]))[0, 1])


def read_book(path: str | None) -> dict[str, dict]:
    """The opening book from a local file, or from gold/ on RustFS."""
    if path:
        return load_book(Path(path))
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / "openings.parquet"
        try:
            fs.copy_files(BOOK, str(local), source_filesystem=get_filesystem(),
                          destination_filesystem=fs.LocalFileSystem())
        except FileNotFoundError:
            raise SystemExit(f"no opening book at {BOOK}: run the opening_book DAG first") from None
        return load_book(local)


class Scorer:
    def __init__(self, book: dict[str, dict], champion: Champion):
        self.book = book
        self.champion = champion
        self.games: dict[str, LiveGame] = {}
        self.channels: dict[str, str] = {}  # channel -> game id on it now
        self.seen_at: dict[str, float] = {}

    def game_for(self, message: dict) -> LiveGame:
        game_id, channel = message["game_id"], message.get("channel")
        if message["type"] == "start" and self.channels.get(channel) not in (None, game_id):
            self.forget(self.channels[channel])  # the channel switched games, the old one is over
        game = self.games.get(game_id)
        if game is None:
            # a move of a game we have no start for (we restarted mid-game) starts without history
            meta = {k: v for k, v in message.items() if k not in ("type", "fen", "last_move", "white_clock",
                    "black_clock", "moves_so_far", "clocks_so_far", "received_at")}
            game = self.games[game_id] = LiveGame(meta)
            if message["type"] == "start" and game.is_scored():
                game.replay(message.get("moves_so_far", "").split(), message.get("clocks_so_far", []), self.book)
        if channel:
            self.channels[channel] = game_id
        self.seen_at[game_id] = time.monotonic()
        return game

    def forget(self, game_id: str) -> None:
        self.games.pop(game_id, None)
        self.seen_at.pop(game_id, None)

    def forget_idle(self) -> None:
        now = time.monotonic()
        for game_id in [g for g, t in self.seen_at.items() if now - t > GAME_TIMEOUT]:
            self.forget(game_id)

    def predict(self, message: dict) -> dict | None:
        """The prediction row for one feed message, or None when there is nothing to score."""
        game = self.game_for(message)
        if message["type"] != "move" or not game.is_scored():
            return None  # a start message only warms the game up: its FEN has wrong move counters
        features = game.position(message["fen"], message.get("white_clock"), message.get("black_clock"), self.book)
        if features is None:
            return None
        self.champion.refresh()
        opening = game.opening or {}
        ply = game.last_ply
        return {
            **{k: game.meta.get(k) for k in ("game_id", "channel", "white_name", "black_name",
                                             "white_rating", "black_rating")},
            "ply": ply,
            "speed": game.speed,
            "fen": message["fen"],
            "last_move": message["last_move"],
            "mover": COLORS[ply % 2 == 0],
            "white_clock": message.get("white_clock"),
            "black_clock": message.get("black_clock"),
            "eco": opening.get("eco"),
            "opening": opening.get("name"),
            "in_book": features["in_book"],
            "plies_out_of_book": features["plies_out_of_book"],
            "blunder_prob": self.champion.score(features),
            "model_version": self.champion.version,
            "features": Json(features),
            "received_at": message.get("received_at"),
        }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--book", help="local openings.parquet instead of gold/openings/ on RustFS")
    parser.add_argument("--from-start", action="store_true",
                        help="on the very first run, score what is already on the topic instead of only new moves")
    args = parser.parse_args(argv)

    connect()
    champion = Champion()
    champion.refresh()
    book = read_book(args.book)
    print(f"opening book: {len(book):,} positions")
    scorer = Scorer(book, champion)

    conn = psycopg2.connect(DB_URL)
    with conn, conn.cursor() as cur:
        cur.execute(CREATE_TABLE)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": "earliest" if args.from_start else "latest",
        "enable.auto.offset.store": False,  # an offset is stored only once its prediction is written
    })
    consumer.subscribe([TOPIC])
    print(f"scoring {TOPIC} as consumer group {GROUP}")
    try:
        while True:
            message = consumer.poll(1.0)
            scorer.forget_idle()
            if message is None:
                continue
            if message.error() is not None:
                if message.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    continue  # the topic appears when the producer sends its first message
                raise KafkaException(message.error())
            row = scorer.predict(json.loads(message.value()))
            if row is not None:
                with conn, conn.cursor() as cur:
                    cur.execute(INSERT, row)
                print(f"[{row['channel']}] {row['game_id']} ply {row['ply']} {row['last_move']}: "
                      f"{row['mover']} to move, blunder risk {row['blunder_prob']:.1%}")
            consumer.store_offsets(message)
    except KeyboardInterrupt:
        print("stopping")
    finally:
        consumer.close()  # commits the stored offsets
        conn.close()


if __name__ == "__main__":
    main()
