import os
import json
import time
import signal
import logging
import threading
import sys
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional

import pyarrow as pa
from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import (
    NestedField, StringType, DoubleType, LongType,
)
import google.cloud.logging
from google.cloud.logging_v2.handlers import CloudLoggingHandler
from google.cloud.logging_v2.handlers.transports.background_thread import BackgroundThreadTransport

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "alpaca-bars")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "alpaca-iceberg-loader")
ICEBERG_CATALOG_URI = os.environ.get("ICEBERG_CATALOG_URI", "sqlite:///./warehouse/catalog.db")
ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "./warehouse")
ICEBERG_NAMESPACE = os.environ.get("ICEBERG_NAMESPACE", "alpaca")
ICEBERG_TABLE = os.environ.get("ICEBERG_TABLE", "bars")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 100))
BATCH_INTERVAL = int(os.environ.get("BATCH_INTERVAL", 300))
PORT = int(os.environ.get("PORT", 8080))
LOG_MODE = os.environ.get("LOG_MODE", "stdout")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
LOG_NAME = os.environ.get("LOG_NAME", "alpaca-loader")
METRICS_INTERVAL = int(os.environ.get("METRICS_INTERVAL", 10))

PYARROW_SCHEMA = pa.schema([
    pa.field("T", pa.string()),
    pa.field("S", pa.string()),
    pa.field("o", pa.float64()),
    pa.field("h", pa.float64()),
    pa.field("l", pa.float64()),
    pa.field("c", pa.float64()),
    pa.field("v", pa.int64()),
    pa.field("t", pa.string()),
])

ICEBERG_TABLE_SCHEMA = IcebergSchema(
    NestedField(1, "T", StringType(), required=False),
    NestedField(2, "S", StringType(), required=False),
    NestedField(3, "o", DoubleType(), required=False),
    NestedField(4, "h", DoubleType(), required=False),
    NestedField(5, "l", DoubleType(), required=False),
    NestedField(6, "c", DoubleType(), required=False),
    NestedField(7, "v", LongType(), required=False),
    NestedField(8, "t", StringType(), required=False),
)


# Iceberg schema field names, in order. Projection is derived from the schema so
# adding a field to PYARROW_SCHEMA automatically flows into project_frame().
SCHEMA_FIELDS = PYARROW_SCHEMA.names


def project_frame(batch: list) -> list:
    """Project a decoded Kafka frame onto the Iceberg schema.

    A frame is a JSON array of Alpaca bar objects. Each bar is reduced to the 8
    schema fields (T,S,o,h,l,c,v,t); extra Alpaca fields are dropped and any
    missing schema field becomes None.
    """
    return [{k: bar.get(k) for k in SCHEMA_FIELDS} for bar in batch]


def should_flush(num_records: int, elapsed: float, batch_size: int, batch_interval: float) -> bool:
    """Whether to flush: batch is full, or the interval elapsed with records buffered."""
    return num_records >= batch_size or (elapsed >= batch_interval and num_records > 0)


def setup_logging(name: str, mode: str = "stdout", level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.handlers.clear()

    if mode in ("cloud", "both") and PROJECT_ID:
        gcp_client = google.cloud.logging.Client()
        logger.addHandler(CloudLoggingHandler(gcp_client, name=name, transport=BackgroundThreadTransport))

    if mode in ("stdout", "both") or (mode == "cloud" and not PROJECT_ID):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
        logger.addHandler(handler)

    return logger


class _Metrics:
    def __init__(self):
        self.messages_consumed: int = 0
        self.records_appended: int = 0
        self.batches_flushed: int = 0
        self.last_flush_records: int = 0
        self.last_flush_duration_ms: float = 0.0
        self.iceberg_append_errors: int = 0
        self.consumer_lag: int = 0
        self.last_commit_ts: str = ""

    def snapshot(self) -> dict:
        return {
            "component": "alpaca-loader",
            "messages_consumed": self.messages_consumed,
            "records_appended": self.records_appended,
            "batches_flushed": self.batches_flushed,
            "last_flush_records": self.last_flush_records,
            "last_flush_duration_ms": round(self.last_flush_duration_ms, 1),
            "iceberg_append_errors": self.iceberg_append_errors,
            "consumer_lag": self.consumer_lag,
            "last_commit_ts": self.last_commit_ts,
        }


def _consumer_lag(consumer: Consumer, topic: str) -> int:
    try:
        md = consumer.list_topics(topic=topic, timeout=2)
        total_lag = 0
        for p in md.topics[topic].partitions:
            tp = TopicPartition(topic, p)
            lo, hi = consumer.get_watermark_offsets(tp, timeout=2, cached=False)
            committed = consumer.committed([tp], timeout=2)[0].offset
            pos = committed if committed >= 0 else lo
            total_lag += max(0, hi - pos)
        return total_lag
    except Exception:
        return -1


def _metrics_emitter(metrics: _Metrics, consumer: Consumer, logger: logging.Logger,
                     interval: int, stop: threading.Event, topic: str) -> None:
    while not stop.wait(interval):
        metrics.consumer_lag = _consumer_lag(consumer, topic)
        logger.info(metrics.snapshot())


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):
        pass


def start_health_server():
    server = HTTPServer(("", PORT), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()


def ensure_kafka_topic(broker: str, topic: str) -> None:
    admin = AdminClient({"bootstrap.servers": broker})
    fs = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    for t, f in fs.items():
        try:
            f.result()
        except Exception:
            pass  # TOPIC_ALREADY_EXISTS is fine


def bootstrap_iceberg() -> any:
    # Ensure local warehouse directory exists before SQLite tries to open it
    if ICEBERG_WAREHOUSE.startswith(("./", "/")) or not ICEBERG_WAREHOUSE.startswith("gs://"):
        os.makedirs(ICEBERG_WAREHOUSE, exist_ok=True)

    catalog_props = {
        "uri": ICEBERG_CATALOG_URI,
        "warehouse": ICEBERG_WAREHOUSE,
    }
    catalog = SqlCatalog("alpaca_catalog", **catalog_props)
    ns = (ICEBERG_NAMESPACE,)
    if ns not in catalog.list_namespaces():
        catalog.create_namespace(ns)
    full_name = f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE}"
    if not catalog.table_exists(full_name):
        catalog.create_table(full_name, schema=ICEBERG_TABLE_SCHEMA)
    return catalog.load_table(full_name)


def flush(records: list, iceberg_table, metrics: _Metrics, logger: logging.Logger) -> bool:
    if not records:
        return True
    t0 = time.monotonic()
    try:
        arrow_table = pa.Table.from_pylist(records, schema=PYARROW_SCHEMA)
        iceberg_table.append(arrow_table)
        metrics.last_flush_records = len(records)
        metrics.last_flush_duration_ms = (time.monotonic() - t0) * 1000
        metrics.batches_flushed += 1
        metrics.records_appended += len(records)
        logger.info(f"Flushed {len(records)} records to Iceberg ({metrics.last_flush_duration_ms:.0f}ms)")
        return True
    except Exception as e:
        metrics.iceberg_append_errors += 1
        logger.error(f"Iceberg append failed: {e}")
        return False


def run_consumer(consumer, iceberg_table, metrics: _Metrics, logger: logging.Logger,
                 stop: threading.Event, batch_size: int = BATCH_SIZE,
                 batch_interval: int = BATCH_INTERVAL) -> None:
    """Poll the consumer and append batches to Iceberg until `stop` is set.

    At-least-once delivery: offsets are committed only after a successful append.
    Does not close the consumer — the caller owns its lifecycle.
    """
    records: list = []
    last_flush = time.monotonic()

    try:
        while not stop.is_set():
            msg = consumer.poll(1.0)

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Kafka error: {msg.error()}")
            else:
                try:
                    batch = json.loads(msg.value().decode("utf-8"))
                    records.extend(project_frame(batch))
                    metrics.messages_consumed += 1
                except Exception as e:
                    logger.error(f"Failed to parse message: {e}")

            elapsed = time.monotonic() - last_flush
            if should_flush(len(records), elapsed, batch_size, batch_interval):
                if flush(records, iceberg_table, metrics, logger):
                    consumer.commit(asynchronous=False)
                    metrics.last_commit_ts = datetime.now(timezone.utc).isoformat()
                    records = []
                    last_flush = time.monotonic()

    finally:
        if records:
            if flush(records, iceberg_table, metrics, logger):
                consumer.commit(asynchronous=False)


def main():
    logger = setup_logging(LOG_NAME, LOG_MODE, LOG_LEVEL)
    start_health_server()
    logger.info(f"Health server on port {PORT}")

    ensure_kafka_topic(KAFKA_BROKER, KAFKA_TOPIC)
    logger.info(f"Kafka topic '{KAFKA_TOPIC}' ready on {KAFKA_BROKER}")

    iceberg_table = bootstrap_iceberg()
    logger.info(f"Iceberg table ready: {ICEBERG_NAMESPACE}.{ICEBERG_TABLE} @ {ICEBERG_WAREHOUSE}")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([KAFKA_TOPIC])
    logger.info(f"Subscribed to {KAFKA_TOPIC} as group '{KAFKA_GROUP_ID}'")

    metrics = _Metrics()
    stop = threading.Event()

    emitter = threading.Thread(
        target=_metrics_emitter,
        args=(metrics, consumer, logger, METRICS_INTERVAL, stop, KAFKA_TOPIC),
        daemon=True,
    )
    emitter.start()

    def handle_shutdown(*_):
        logger.info("Shutdown signal received")
        stop.set()

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    try:
        run_consumer(consumer, iceberg_table, metrics, logger, stop)
    finally:
        stop.set()
        consumer.close()
        logger.info(f"Final metrics: {metrics.snapshot()}")


if __name__ == "__main__":
    main()
