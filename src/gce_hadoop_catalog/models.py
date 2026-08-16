"""The intentionally small synthetic market-bar contract."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256


def require_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(timezone.utc)


def utc_isoformat(value: datetime) -> str:
    return require_utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class MarketBar:
    symbol: str
    event_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

    def __post_init__(self) -> None:
        if not self.symbol:
            raise ValueError("symbol is required")
        if self.volume < 0:
            raise ValueError("volume must not be negative")
        object.__setattr__(self, "event_time", require_utc(self.event_time))

    @property
    def event_id(self) -> str:
        identity = f"{self.symbol}|{utc_isoformat(self.event_time)}"
        return sha256(identity.encode("utf-8")).hexdigest()

    def as_row(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "symbol": self.symbol,
            "event_time": self.event_time,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }
