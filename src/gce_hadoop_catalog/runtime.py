"""Alpaca-compatible synthetic producer helpers for a running local broker."""

from __future__ import annotations

import json
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .config import LocalSettings
from .models import MarketBar


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
        # Alpaca sends batches as JSON arrays; preserve that contract exactly.
        producer.produce(settings.topic, json.dumps([bar.as_alpaca() for bar in bars]).encode("utf-8"))
        remaining = producer.flush(10)
        if remaining:
            raise TimeoutError(f"{remaining} messages were not delivered")
    finally:
        producer.flush(1)
