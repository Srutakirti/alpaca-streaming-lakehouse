"""Unit tests for flush(): append a batch of projected records to Iceberg."""
import logging

import pytest

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


def test_flush_empty_is_noop_and_returns_true(tmp_iceberg):
    metrics = _Metrics()

    assert flush([], tmp_iceberg, metrics, logger) is True

    assert _read_back(tmp_iceberg) == []
    assert metrics.batches_flushed == 0
    assert metrics.records_appended == 0


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


def test_repeated_flushes_accumulate(tmp_iceberg, make_frame):
    metrics = _Metrics()

    flush(project_frame(make_frame(n=2, symbol="AAPL")), tmp_iceberg, metrics, logger)
    flush(project_frame(make_frame(n=3, symbol="TSLA")), tmp_iceberg, metrics, logger)

    assert metrics.records_appended == 5
    assert metrics.batches_flushed == 2
    assert len(_read_back(tmp_iceberg)) == 5
