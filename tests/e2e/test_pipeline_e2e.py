"""End-to-end pipeline test: synthetic generator -> Kafka -> loader -> Iceberg -> API.

Requires a live broker at $KAFKA_BROKER (default localhost:9092). Run with
`make e2e`, which starts the local Tansu broker first.
"""
import logging
import os
import subprocess
import sys
import threading
import time
import uuid

import pytest
from confluent_kafka import Consumer
from fastapi.testclient import TestClient

from load.subscriber import _Metrics, run_consumer

pytestmark = pytest.mark.e2e

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
logger = logging.getLogger("test-pipeline-e2e")


def _wait_until(predicate, timeout: float = 30.0, interval: float = 0.2) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def _run_generator(topic: str, symbols: list[str], seconds: float = 3.0) -> str:
    cmd = [
        sys.executable,
        "extract/helpers/synthetic_stock_generator.py",
        "--kafka",
        KAFKA_BROKER,
        "--topic",
        topic,
        "--symbols",
        *symbols,
        "--rate",
        "10",
        "--metrics-interval",
        "1",
        "--bar-interval",
        "60",
    ]
    proc = subprocess.Popen(
        cmd,
        cwd=os.getcwd(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        time.sleep(seconds)
        if proc.poll() is not None:
            output = proc.stdout.read() if proc.stdout else ""
            raise AssertionError(f"synthetic generator exited early ({proc.returncode}):\n{output}")
        proc.terminate()
        try:
            output, _ = proc.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            output, _ = proc.communicate(timeout=5)
            raise AssertionError(f"synthetic generator did not stop cleanly:\n{output}")
        if proc.returncode not in (0, -15):
            raise AssertionError(f"synthetic generator failed ({proc.returncode}):\n{output}")
        return output
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


def _point_frontend_at(iceberg_env, monkeypatch) -> None:
    from frontend.app import iceberg_client
    from frontend.app.routes import bars

    for key, value in iceberg_env.items():
        monkeypatch.setattr(iceberg_client, key, value)
    iceberg_client._catalog.cache_clear()
    bars._symbols_cache = ([], 0.0)


def test_synthetic_kafka_loader_iceberg_frontend_roundtrip(tmp_iceberg, iceberg_env, monkeypatch):
    topic = f"e2e-bars-{uuid.uuid4().hex[:8]}"
    group = f"e2e-loader-{uuid.uuid4().hex[:8]}"
    symbols = ["AAPL", "TSLA"]

    generator_output = _run_generator(topic, symbols)

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
        kwargs={"batch_size": len(symbols), "batch_interval": 1},
        daemon=True,
    )
    worker.start()
    try:
        assert _wait_until(lambda: metrics.records_appended >= len(symbols)), (
            f"loader appended {metrics.records_appended} records; generator output:\n{generator_output}"
        )
    finally:
        stop.set()
        worker.join(timeout=10)
        consumer.close()

    rows = tmp_iceberg.scan().to_arrow().to_pylist()
    assert len(rows) >= len(symbols)
    assert set(symbols).issubset({row["S"] for row in rows})

    from frontend.app import logging_client

    monkeypatch.setattr(logging_client, "get_last_extractor_metrics", lambda: None)
    monkeypatch.setattr(logging_client, "get_last_loader_metrics", lambda: None)
    _point_frontend_at(iceberg_env, monkeypatch)
    from frontend.app.main import app

    client = TestClient(app)
    symbols_resp = client.get("/api/symbols")
    assert symbols_resp.status_code == 200
    assert set(symbols).issubset(set(symbols_resp.json()))

    for symbol in symbols:
        bars_resp = client.get("/api/bars", params={"symbol": symbol, "limit": 5})
        assert bars_resp.status_code == 200
        bars = bars_resp.json()
        assert bars, f"no API bars returned for {symbol}"
        assert set(bars[0]) == {"t", "o", "h", "l", "c", "v"}

    status = client.get("/api/pipeline/status").json()
    assert status["iceberg"]["row_count"] >= len(symbols)
    assert status["iceberg"]["latest_record_t"] is not None
