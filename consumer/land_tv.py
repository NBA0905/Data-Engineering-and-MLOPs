"""Lands the raw TV feed events from `tv.moves` in `bronze/tv/` on RustFS.

    uv run python -m consumer.land_tv

A long-running service next to the scorer, in its own consumer group, so landing and scoring
never hold each other up. Collects messages and every few minutes writes one Parquet file per
partition to `bronze/tv/date=YYYY-MM-DD/`, named after the first Kafka offset in it. Offsets
are committed only after the upload, so a crash lands the same messages again, into the same
file name.

Each row keeps the message exactly as received (`message`, JSON text), next to a few columns
to filter on. Start and move messages have different fields, so a fixed schema would drop some.
"""

import json
import os
import signal
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from storage.rustfs import get_filesystem

TOPIC = "tv.moves"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
GROUP = "bronze-tv"
BRONZE_FOLDER = "bronze/tv"
FLUSH_EVERY = int(os.environ.get("TV_FLUSH_SECONDS", 300))  # seconds between files

SCHEMA = pa.schema([
    ("game_id", pa.string()),
    ("channel", pa.string()),
    ("type", pa.string()),  # "start" or "move"
    ("received_at", pa.float64()),  # unix time the producer read the event from lichess
    ("message", pa.string()),
    ("kafka_partition", pa.int32()),
    ("kafka_offset", pa.int64()),
])

stopping = False


def stop(*_) -> None:
    global stopping
    stopping = True


def to_row(message) -> dict:
    value = message.value().decode()
    event = json.loads(value)
    return {
        "game_id": event.get("game_id"),
        "channel": event.get("channel"),
        "type": event.get("type"),
        "received_at": event.get("received_at"),
        "message": value,
        "kafka_partition": message.partition(),
        "kafka_offset": message.offset(),
    }


def flush(consumer: Consumer, rows: dict[int, list[dict]], lake) -> None:
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for p, part_rows in rows.items():
        if not part_rows:
            continue
        first, last = part_rows[0]["kafka_offset"], part_rows[-1]["kafka_offset"]
        key = f"{BRONZE_FOLDER}/date={date}/tv-p{p}-{first:012d}.parquet"
        pq.write_table(pa.Table.from_pylist(part_rows, schema=SCHEMA), key, filesystem=lake)
        consumer.commit(offsets=[TopicPartition(TOPIC, p, last + 1)], asynchronous=False)
        print(f"partition {p}: {len(part_rows):,} events -> {key}")
    rows.clear()


def main() -> None:
    signal.signal(signal.SIGTERM, stop)  # docker stop: land what we have before exiting
    signal.signal(signal.SIGINT, stop)
    lake = get_filesystem()
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": GROUP,
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    consumer.subscribe([TOPIC])
    print(f"landing {TOPIC} in {BRONZE_FOLDER}/ every {FLUSH_EVERY} s")
    rows: dict[int, list[dict]] = {}
    last_flush = time.monotonic()
    try:
        while not stopping:
            message = consumer.poll(1.0)
            if message is not None:
                if message.error() is None:
                    rows.setdefault(message.partition(), []).append(to_row(message))
                elif message.error().code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                    raise KafkaException(message.error())
            if time.monotonic() - last_flush >= FLUSH_EVERY:
                flush(consumer, rows, lake)
                last_flush = time.monotonic()
        flush(consumer, rows, lake)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
