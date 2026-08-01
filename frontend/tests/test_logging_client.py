import json
from datetime import datetime, timezone
from types import SimpleNamespace

from frontend.app import logging_client


def test_local_loader_metrics_fallback(tmp_path, monkeypatch):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    log_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S,%f")[:-3] + "Z"
    (log_dir / "loader.log").write_text(
        f"{log_ts} INFO: {{'component': 'alpaca-loader', "
        "'messages_consumed': 31, 'records_appended': 31, 'batches_flushed': 31, "
        "'last_flush_records': 1, 'last_flush_duration_ms': 1680.8, "
        "'iceberg_append_errors': 0, 'consumer_lag': 0, "
        "'last_commit_ts': '2026-07-13T14:55:01.577697+00:00'}\n"
    )
    monkeypatch.setattr(logging_client, "PROJECT_ID", "")
    monkeypatch.setattr(logging_client, "LOCAL_LOG_DIR", str(log_dir))

    row = logging_client.get_last_loader_metrics()

    assert row["last_flush_records"] == 1
    assert row["last_flush_duration_ms"] == 1680.8
    assert row["last_commit_ts"] == "2026-07-13T14:55:01.577697+00:00"


def test_local_extractor_metrics_fallback(tmp_path, monkeypatch):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    snapshot = {
        "component": "alpaca-extractor-rs",
        "connection_status": True,
        "messages_sent": 32,
        "queue_depth": 0,
    }
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "level": "INFO",
        "fields": {"message": "metrics", "snapshot": json.dumps(snapshot)},
    }
    (log_dir / "extractor.log").write_text(json.dumps(entry) + "\n")
    monkeypatch.setattr(logging_client, "PROJECT_ID", "")
    monkeypatch.setattr(logging_client, "LOCAL_LOG_DIR", str(log_dir))

    row = logging_client.get_last_extractor_metrics()

    assert row["connection_status"] is True
    assert row["messages_sent"] == 32


def test_local_metrics_timeseries_filters_non_metrics(tmp_path, monkeypatch):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    log_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S,%f")[:-3] + "Z"
    (log_dir / "loader.log").write_text(
        f"{log_ts} INFO: Flushed 1 records to Iceberg (1681ms)\n"
        f"{log_ts} INFO: {{'component': 'alpaca-loader', "
        "'consumer_lag': 0, 'batches_flushed': 31}\n"
    )
    monkeypatch.setattr(logging_client, "PROJECT_ID", "")
    monkeypatch.setattr(logging_client, "LOCAL_LOG_DIR", str(log_dir))

    rows = logging_client.get_metrics_timeseries("loader", minutes=1440)

    assert len(rows) == 1
    assert rows[0]["batches_flushed"] == 31


def test_gcp_extractor_metrics_falls_back_to_cloud_run_stdout(monkeypatch):
    metric_ts = datetime.now(timezone.utc)
    snapshot = {
        "component": "alpaca-extractor-rs",
        "connection_status": True,
        "messages_sent": 9,
        "messages_received": 9,
    }
    entry = SimpleNamespace(
        payload={"fields": {"message": "metrics", "snapshot": json.dumps(snapshot)}},
        timestamp=metric_ts,
    )

    class FakeClient:
        def list_entries(self, filter_, **_kwargs):
            if 'resource.type="cloud_run_job"' in filter_:
                return [entry]
            return []

    monkeypatch.setattr(logging_client, "PROJECT_ID", "project-123")
    monkeypatch.setattr(logging_client, "_client", lambda: FakeClient())

    row = logging_client.get_last_extractor_metrics()

    assert row["component"] == "alpaca-extractor-rs"
    assert row["messages_sent"] == 9
    assert row["_ts"] == metric_ts.isoformat()


def test_gcp_extractor_timeseries_falls_back_to_cloud_run_stdout(monkeypatch):
    metric_ts = datetime.now(timezone.utc)
    snapshot = {
        "component": "alpaca-extractor-rs",
        "connection_status": True,
        "messages_sent": 12,
    }
    entry = SimpleNamespace(
        payload={"fields": {"message": "metrics", "snapshot": json.dumps(snapshot)}},
        timestamp=metric_ts,
    )

    class FakeClient:
        def list_entries(self, filter_, **_kwargs):
            if 'resource.type="cloud_run_job"' in filter_:
                return [entry]
            return []

    monkeypatch.setattr(logging_client, "PROJECT_ID", "project-123")
    monkeypatch.setattr(logging_client, "_client", lambda: FakeClient())

    rows = logging_client.get_metrics_timeseries("extractor", minutes=15)

    assert len(rows) == 1
    assert rows[0]["messages_sent"] == 12
