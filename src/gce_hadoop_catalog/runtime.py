"""Bounded local broker -> producer -> loader orchestration."""

from __future__ import annotations

import json
from datetime import datetime

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .config import LocalSettings
from .hadoop_catalog import HadoopCatalog
from .loader import Loader, LoadResult
from .models import MarketBar
from .tansu_sqlite import TansuSqlite


def ensure_topic(settings: LocalSettings) -> None:
    admin = AdminClient({"bootstrap.servers": settings.broker_url})
    futures = admin.create_topics(
        [NewTopic(settings.topic, num_partitions=1, replication_factor=1)]
    )
    for future in futures.values():
        try:
            future.result(10)
        except Exception as error:
            if "TOPIC_ALREADY_EXISTS" not in str(error):
                raise


def publish(settings: LocalSettings, bars: list[MarketBar]) -> None:
    producer = Producer({"bootstrap.servers": settings.broker_url})
    try:
        for bar in bars:
            producer.produce(settings.topic, json.dumps(_encode(bar.as_row())).encode("utf-8"))
        remaining = producer.flush(10)
        if remaining:
            raise TimeoutError(f"{remaining} messages were not delivered")
    finally:
        producer.flush(1)


def consume(settings: LocalSettings, expected: int) -> list[MarketBar]:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.broker_url,
            "group.id": "synthetic-local-loader",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    bars: list[MarketBar] = []
    try:
        consumer.subscribe([settings.topic])
        while len(bars) < expected:
            message = consumer.poll(5)
            if message is None:
                raise TimeoutError("timed out waiting for synthetic bars")
            if message.error():
                raise RuntimeError(str(message.error()))
            bars.append(_decode(json.loads(message.value())))
        consumer.commit(asynchronous=False)
        return bars
    finally:
        consumer.close()


def run_bounded(settings: LocalSettings, bars: list[MarketBar]) -> LoadResult:
    with TansuSqlite(settings):
        ensure_topic(settings)
        publish(settings, bars)
        consumed = consume(settings, len(bars))
        catalog = HadoopCatalog(settings)
        try:
            return Loader(catalog).load(consumed)
        finally:
            catalog.close()


def _encode(row: dict[str, object]) -> dict[str, object]:
    encoded = dict(row)
    encoded["event_time"] = encoded["event_time"].isoformat().replace("+00:00", "Z")
    return encoded


def _decode(row: dict[str, object]) -> MarketBar:
    return MarketBar(
        symbol=str(row["symbol"]),
        event_time=datetime.fromisoformat(str(row["event_time"]).replace("Z", "+00:00")),
        open=float(row["open"]),
        high=float(row["high"]),
        low=float(row["low"]),
        close=float(row["close"]),
        volume=int(row["volume"]),
    )
