"""Archive replay producer: sends the next annotated games from the archive to `games.archive`.

    uv run python -m ingest.replay_archive data/raw/lichess_db_standard_rated_2026-09.part.pgn.zst --games 5000

Every run carries on where the previous one stopped, so each run sends new games. The position
is kept in the `archive_checkpoint` table in Postgres (database `chess`), one row per archive
file. The first run, when there is no checkpoint yet, sends `--backfill` games instead.

Each message is one raw PGN game, keyed by game id, stamped with the run's `batch` number and
the game's `seq` (its number among all annotated games sent so far, in archive order). The
checkpoint is saved only after Redpanda has confirmed every message: if a run fails halfway,
the next run sends the same games again (at least once, duplicates are dropped downstream).
"""

import argparse
import json
import os
from dataclasses import astuple, dataclass
from pathlib import Path

import psycopg2
from confluent_kafka import Producer

from ingest.archive import game_id, has_evals, iter_games

TOPIC = "games.archive"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
DB_URL = os.environ.get("CHESS_DB_URL", "postgresql://airflow:airflow@localhost:5432/chess")

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS archive_checkpoint (
    archive       text PRIMARY KEY,     -- file name, the same on the laptop and in the container
    byte_offset   bigint NOT NULL,      -- decompressed offset where the next unread game starts
    games_scanned bigint NOT NULL,      -- games read so far, annotated or not
    games_sent    bigint NOT NULL,      -- annotated games sent so far, also the next game's seq
    batches       integer NOT NULL,     -- runs so far, also the next run's batch number
    updated_at    timestamptz NOT NULL DEFAULT now()
)
"""


@dataclass
class Checkpoint:
    byte_offset: int = 0
    games_scanned: int = 0
    games_sent: int = 0
    batches: int = 0


def load_checkpoint(conn, archive: str) -> Checkpoint | None:
    with conn, conn.cursor() as cur:
        cur.execute(CREATE_TABLE)
        cur.execute("SELECT byte_offset, games_scanned, games_sent, batches FROM archive_checkpoint "
                    "WHERE archive = %s", (archive,))
        row = cur.fetchone()
    return Checkpoint(*row) if row else None


def save_checkpoint(conn, archive: str, checkpoint: Checkpoint) -> None:
    with conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO archive_checkpoint (archive, byte_offset, games_scanned, games_sent, batches)
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (archive) DO UPDATE SET
                   byte_offset = EXCLUDED.byte_offset, games_scanned = EXCLUDED.games_scanned,
                   games_sent = EXCLUDED.games_sent, batches = EXCLUDED.batches, updated_at = now()""",
            (archive, *astuple(checkpoint)),
        )


def next_games(path: Path, checkpoint: Checkpoint, games: int) -> tuple[list[dict], Checkpoint]:
    """The next `games` annotated games after `checkpoint` as messages, and the checkpoint after them.

    The new checkpoint points just past the last annotated game, so games scanned after it
    (not annotated, or the archive ran out) are scanned again next time.
    """
    messages: list[dict] = []
    after = Checkpoint(checkpoint.byte_offset, checkpoint.games_scanned, checkpoint.games_sent, checkpoint.batches + 1)
    scanned = 0
    for text, end in iter_games(path, checkpoint.byte_offset):
        scanned += 1
        if not has_evals(text):
            continue
        messages.append({"game_id": game_id(text), "archive": path.name, "batch": checkpoint.batches,
                         "seq": checkpoint.games_sent + len(messages), "pgn": text})
        after.byte_offset, after.games_scanned = end, checkpoint.games_scanned + scanned
        if len(messages) == games:
            break
    after.games_sent += len(messages)
    return messages, after


def publish(messages: list[dict]) -> None:
    """Sends the messages and waits until Redpanda has confirmed all of them."""
    failed = []

    def on_delivery(error, _message):
        if error is not None:
            failed.append(error)

    producer = Producer({"bootstrap.servers": BOOTSTRAP, "compression.type": "zstd", "linger.ms": 50})
    for message in messages:
        # game id as key, like tv.moves: a game always lands in the same partition
        producer.produce(TOPIC, key=message["game_id"], value=json.dumps(message), on_delivery=on_delivery)
        producer.poll(0)
    unsent = producer.flush(60)
    if unsent or failed:
        raise RuntimeError(f"{unsent} messages not sent, {len(failed)} failed (first error: {failed[:1]})")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path)
    parser.add_argument("--games", type=int, default=5_000, help="annotated games per run")
    parser.add_argument("--backfill", type=int, default=20_000, help="annotated games on the first run")
    args = parser.parse_args(argv)

    conn = psycopg2.connect(DB_URL)
    try:
        checkpoint = load_checkpoint(conn, args.archive.name)
        games = args.games if checkpoint else args.backfill
        checkpoint = checkpoint or Checkpoint()

        messages, after = next_games(args.archive, checkpoint, games)
        if not messages:
            raise SystemExit(f"no annotated games left in {args.archive.name} after offset {checkpoint.byte_offset:,}")
        publish(messages)
        save_checkpoint(conn, args.archive.name, after)
    finally:
        conn.close()

    print(f"batch {checkpoint.batches}: sent {len(messages):,} annotated games to {TOPIC} "
          f"(seq {checkpoint.games_sent:,}-{after.games_sent - 1:,}), scanned {after.games_scanned - checkpoint.games_scanned:,} "
          f"games, next run starts at byte {after.byte_offset:,}")


if __name__ == "__main__":
    main()
