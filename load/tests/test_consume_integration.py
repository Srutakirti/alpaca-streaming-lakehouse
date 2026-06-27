"""Integration test: real Kafka -> loader consume loop -> Iceberg.

Requires a live broker at $KAFKA_BROKER (default localhost:9092). Bring one up
with `make up` (docker Tansu) before running `-m integration`. The same test
validates a cloud broker by exporting KAFKA_BROKER=<vm_ip>:9092.
"""
import json
import logging
import os
import threading
import time
import uuid

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition

from load import subscriber
from load.subscriber import _Metrics, ensure_kafka_topic, run_consumer

pytestmark = pytest.mark.integration

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
logger = logging.getLogger("test-consume-integration")


def _produce_frames(broker: str, topic: str, frames: list[list]) -> None:
    producer = Producer({"bootstrap.servers": broker})
    producer.list_topics(timeout=10)  # force the TCP handshake (Tansu quirk)
    for frame in frames:
        producer.produce(topic, json.dumps(frame).encode("utf-8"))
    producer.flush(10)


def _wait_until(predicate, timeout: float = 30.0, interval: float = 0.2) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_kafka_to_iceberg_roundtrip(tmp_iceberg, make_frame):
    topic = f"test-bars-{uuid.uuid4().hex[:8]}"
    group = f"test-loader-{uuid.uuid4().hex[:8]}"
    ensure_kafka_topic(KAFKA_BROKER, topic)

    frames = [make_frame(n=2, symbol="AAPL"), make_frame(n=3, symbol="TSLA")]
    expected_records = sum(len(f) for f in frames)  # 5 bars across 2 messages
    _produce_frames(KAFKA_BROKER, topic, frames)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])

    metrics = _Metrics()
    stop = threading.Event()
    worker = threading.Thread(
        target=run_consumer,
        args=(consumer, tmp_iceberg, metrics, logger, stop),
        # Flush as soon as the full batch is buffered; short interval as backstop.
        kwargs={"batch_size": expected_records, "batch_interval": 1},
        daemon=True,
    )
    worker.start()
    try:
        assert _wait_until(lambda: metrics.records_appended >= expected_records), (
            f"only {metrics.records_appended}/{expected_records} records appended"
        )
    finally:
        stop.set()
        worker.join(timeout=10)

    # Rows landed in Iceberg with the right symbols.
    rows = tmp_iceberg.scan().to_arrow().to_pylist()
    assert len(rows) == expected_records
    assert {r["S"] for r in rows} == {"AAPL", "TSLA"}

    # At-least-once: offsets committed only after a successful append, so the
    # committed offset reaches the produced message count and lag is zero.
    committed = consumer.committed([TopicPartition(topic, 0)], timeout=10)[0]
    assert committed.offset == len(frames)
    consumer.close()


def test_reload_from_catalog_sees_committed_rows(tmp_iceberg, make_frame):
    """A fresh table load (as the frontend does) sees the consumer's committed snapshot."""
    topic = f"test-bars-{uuid.uuid4().hex[:8]}"
    group = f"test-loader-{uuid.uuid4().hex[:8]}"
    ensure_kafka_topic(KAFKA_BROKER, topic)

    frames = [make_frame(n=4, symbol="NVDA")]
    _produce_frames(KAFKA_BROKER, topic, frames)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    metrics = _Metrics()
    stop = threading.Event()
    worker = threading.Thread(
        target=run_consumer,
        args=(consumer, tmp_iceberg, metrics, logger, stop),
        kwargs={"batch_size": 4, "batch_interval": 1},
        daemon=True,
    )
    worker.start()
    try:
        assert _wait_until(lambda: metrics.records_appended >= 4)
    finally:
        stop.set()
        worker.join(timeout=10)
        consumer.close()

    # Independent reader via the same catalog/warehouse (env still monkeypatched).
    reloaded = subscriber.bootstrap_iceberg()
    assert len(reloaded.scan().to_arrow()) == 4
