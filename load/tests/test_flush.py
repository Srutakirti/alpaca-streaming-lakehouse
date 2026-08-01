"""Unit tests for flush(): append a batch of projected records to Iceberg."""
import logging

from load.subscriber import _Metrics, flush, project_frame

logger = logging.getLogger("test-flush")


def _read_back(table) -> list[dict]:
    rows = table.scan().to_arrow().to_pylist()
    return sorted(rows, key=lambda r: r["t"])


def test_flush_writes_records_and_reads_back_equal(tmp_iceberg, make_frame):
    records = project_frame(make_frame(n=5, symbol="AAPL"))
    metrics = _Metrics()

    assert flush(records, tmp_iceberg, metrics, logger) is True

    read = _read_back(tmp_iceberg)
    assert read == sorted(records, key=lambda r: r["t"])
    assert metrics.records_appended == 5
    assert metrics.batches_flushed == 1
    assert metrics.last_flush_records == 5
    assert metrics.iceberg_append_errors == 0
    assert metrics.latest_record_t == "2026-06-27T14:34:00Z"


def test_flush_empty_is_noop_and_returns_true(tmp_iceberg):
    metrics = _Metrics()

    assert flush([], tmp_iceberg, metrics, logger) is True

    assert _read_back(tmp_iceberg) == []
    assert metrics.batches_flushed == 0
    assert metrics.records_appended == 0
    assert metrics.latest_record_t == ""


def test_flush_append_error_is_caught_and_counted(make_frame):
    class _Boom:
        def append(self, _arrow):
            raise RuntimeError("iceberg unavailable")

    records = project_frame(make_frame(n=3))
    metrics = _Metrics()

    assert flush(records, _Boom(), metrics, logger) is False

    assert metrics.iceberg_append_errors == 1
    assert metrics.records_appended == 0
    assert metrics.batches_flushed == 0
    assert metrics.latest_record_t == ""


def test_flush_refreshes_and_retries_iceberg_commit_conflict(make_frame):
    class _StaleTable:
        def __init__(self):
            self.append_calls = 0
            self.refresh_calls = 0

        def append(self, _arrow):
            self.append_calls += 1
            if self.append_calls == 1:
                raise RuntimeError(
                    "Requirement failed: branch main has changed: expected id 1, found 2"
                )

        def refresh(self):
            self.refresh_calls += 1
            return self

        def schema(self):
            raise RuntimeError("default to string schema")

    records = project_frame(make_frame(n=3))
    metrics = _Metrics()
    table = _StaleTable()

    assert flush(records, table, metrics, logger) is True

    assert table.append_calls == 2
    assert table.refresh_calls == 1
    assert metrics.iceberg_append_errors == 1
    assert metrics.records_appended == 3
    assert metrics.batches_flushed == 1


def test_repeated_flushes_accumulate(tmp_iceberg, make_frame):
    metrics = _Metrics()

    flush(project_frame(make_frame(n=2, symbol="AAPL")), tmp_iceberg, metrics, logger)
    flush(project_frame(make_frame(n=3, symbol="TSLA")), tmp_iceberg, metrics, logger)

    assert metrics.records_appended == 5
    assert metrics.batches_flushed == 2
    assert len(_read_back(tmp_iceberg)) == 5
