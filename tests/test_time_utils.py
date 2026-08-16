from datetime import datetime, timezone

import pytest

from gce_hadoop_catalog.time_utils import window_end, window_start


def test_window_is_utc_and_aligned_to_five_minutes() -> None:
    value = datetime(2026, 1, 2, 14, 34, 59, tzinfo=timezone.utc)
    assert window_start(value) == datetime(2026, 1, 2, 14, 30, tzinfo=timezone.utc)
    assert window_end(value) == datetime(2026, 1, 2, 14, 35, tzinfo=timezone.utc)


def test_naive_timestamps_are_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        window_start(datetime(2026, 1, 2, 14, 30))
