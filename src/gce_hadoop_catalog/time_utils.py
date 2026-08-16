"""UTC-only timestamp and fixed commit-window helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .models import require_utc


def window_start(value: datetime, size_seconds: int = 300) -> datetime:
    if size_seconds <= 0:
        raise ValueError("window size must be positive")
    timestamp = require_utc(value)
    epoch = int(timestamp.timestamp())
    return datetime.fromtimestamp(epoch - (epoch % size_seconds), tz=timezone.utc)


def window_end(value: datetime, size_seconds: int = 300) -> datetime:
    return window_start(value, size_seconds) + timedelta(seconds=size_seconds)
