"""Unit tests for _Metrics.snapshot()."""
from load.subscriber import _Metrics

EXPECTED_KEYS = {
    "component", "messages_consumed", "records_appended", "batches_flushed",
    "last_flush_records", "last_flush_duration_ms", "iceberg_append_errors",
    "consumer_lag", "last_commit_ts",
}


def test_fresh_snapshot_shape_and_defaults():
    snap = _Metrics().snapshot()

    assert set(snap) == EXPECTED_KEYS
    assert snap["component"] == "alpaca-loader"
    assert snap["messages_consumed"] == 0
    assert snap["records_appended"] == 0
    assert snap["consumer_lag"] == 0
    assert snap["last_commit_ts"] == ""


def test_snapshot_reflects_updates_and_rounds_duration():
    m = _Metrics()
    m.messages_consumed = 7
    m.records_appended = 250
    m.consumer_lag = 12
    m.last_flush_duration_ms = 12.345

    snap = m.snapshot()
    assert snap["messages_consumed"] == 7
    assert snap["records_appended"] == 250
    assert snap["consumer_lag"] == 12
    assert snap["last_flush_duration_ms"] == 12.3  # rounded to 1 dp
