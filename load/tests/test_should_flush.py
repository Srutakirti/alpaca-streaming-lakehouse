"""Unit tests for should_flush: the batch-flush decision.

Mirrors the production trigger: flush when full, OR interval elapsed with records.
"""
import pytest

from load.subscriber import should_flush

BATCH_SIZE = 100
BATCH_INTERVAL = 300


@pytest.mark.parametrize(
    "num_records, elapsed, expected",
    [
        # Size trigger: at/over batch size flushes regardless of elapsed time.
        (100, 0.0, True),
        (101, 0.0, True),
        (99, 0.0, False),
        # Interval trigger: only when records are buffered.
        (1, 300.0, True),
        (1, 301.0, True),
        (1, 299.9, False),
        # Empty buffer never flushes, even past the interval (no empty commits).
        (0, 0.0, False),
        (0, 10_000.0, False),
        # Both conditions true.
        (100, 300.0, True),
    ],
)
def test_should_flush_truth_table(num_records, elapsed, expected):
    assert should_flush(num_records, elapsed, BATCH_SIZE, BATCH_INTERVAL) is expected


def test_size_boundary_is_inclusive():
    assert should_flush(BATCH_SIZE, 0.0, BATCH_SIZE, BATCH_INTERVAL) is True
    assert should_flush(BATCH_SIZE - 1, 0.0, BATCH_SIZE, BATCH_INTERVAL) is False


def test_interval_boundary_is_inclusive():
    assert should_flush(1, BATCH_INTERVAL, BATCH_SIZE, BATCH_INTERVAL) is True
    assert should_flush(1, BATCH_INTERVAL - 0.1, BATCH_SIZE, BATCH_INTERVAL) is False
