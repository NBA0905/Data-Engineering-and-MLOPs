"""Lands the replayed archive games from `games.archive` in `bronze/archive/` on RustFS.

    uv run python -m ingest.land_archive

Reads every message the consumer group has not landed yet, up to the end of the topic as it
is when the run starts, and writes them as one Parquet file per partition, named after the
first Kafka offset in it. Offsets are committed only after the upload, so a failed run lands
the same messages again next time, into the same file name.
"""

import json
import time

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaException, TopicPartition

from ingest.replay_archive import BOOTSTRAP, TOPIC
from storage.rustfs import get_filesystem

GROUP = "bronze-archive"
BRONZE_FOLDER = "bronze/archive"
IDLE_TIMEOUT = 60  # seconds without a message before we give up waiting for the rest


def pending_partitions(consumer: Consumer) -> dict[int, tuple[int, int]]:
    """{partition: (first offset to read, end offset)} for partitions with unlanded messages."""
    topic = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC]
    if topic.error is not None:
        return {}  # the topic does not exist until the producer has run once
    committed = consumer.committed([TopicPartition(TOPIC, p) for p in topic.partitions], timeout=10)
    pending = {}
    for tp in committed:
        low, high = consumer.get_watermark_offsets(tp, timeout=10)
        first = tp.offset if tp.offset >= 0 else low  # never committed: start at the oldest message
        if first < high:
            pending[tp.partition] = (first, high)
    return pending


def consume(consumer: Consumer, pending: dict[int, tuple[int, int]]) -> dict[int, list[dict]]:
    """Reads each pending partition up to its end offset, one row per message."""
    consumer.assign([TopicPartition(TOPIC, p, first) for p, (first, _) in pending.items()])
    rows: dict[int, list[dict]] = {p: [] for p in pending}
    open_partitions = set(pending)
    last_message = time.monotonic()
    while open_partitions:
        message = consumer.poll(1.0)
        if message is None:
            if time.monotonic() - last_message > IDLE_TIMEOUT:
                raise TimeoutError(f"no messages for {IDLE_TIMEOUT}s, still waiting on partitions {open_partitions}")
            continue
        if message.error() is not None:
            raise KafkaException(message.error())
        last_message = time.monotonic()
        p, offset = message.partition(), message.offset()
        end = pending[p][1]
        if offset < end:  # later messages were sent after this run started, the next run lands them
            rows[p].append({**json.loads(message.value()), "kafka_partition": p, "kafka_offset": offset})
        if offset >= end - 1:
            open_partitions.discard(p)
    return rows


def main() -> None:
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": GROUP,
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    try:
        pending = pending_partitions(consumer)
        if not pending:
            print(f"nothing new on {TOPIC}")
            return
        lake = get_filesystem()
        for p, rows in consume(consumer, pending).items():
            first = pending[p][0]
            key = f"{BRONZE_FOLDER}/games-p{p}-{first:012d}.parquet"
            pq.write_table(pa.Table.from_pylist(rows), key, filesystem=lake)
            consumer.commit(offsets=[TopicPartition(TOPIC, p, pending[p][1])], asynchronous=False)
            batches = sorted({r["batch"] for r in rows})
            print(f"partition {p}: {len(rows):,} games (batch {', '.join(map(str, batches))}) -> {key}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
