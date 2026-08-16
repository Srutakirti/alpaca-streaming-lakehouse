"""Finite deterministic synthetic bars; no market-data API is contacted."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable

from .models import MarketBar, require_utc

DEFAULT_SYMBOLS = ("AAPL", "MSFT")
BASE_PRICES = {"AAPL": 175.0, "MSFT": 380.0}


def generate_bars(
    *,
    start: datetime,
    periods: int,
    symbols: Iterable[str] = DEFAULT_SYMBOLS,
    interval: timedelta = timedelta(minutes=1),
) -> list[MarketBar]:
    """Generate a reproducible finite sequence ordered by time then symbol."""
    if periods <= 0:
        raise ValueError("periods must be positive")
    if interval.total_seconds() <= 0:
        raise ValueError("interval must be positive")
    start = require_utc(start)
    normalized_symbols = tuple(symbols)
    if not normalized_symbols:
        raise ValueError("at least one symbol is required")

    bars: list[MarketBar] = []
    for period in range(periods):
        event_time = start + period * interval
        for symbol_index, symbol in enumerate(normalized_symbols):
            base = BASE_PRICES.get(symbol, 100.0 + symbol_index)
            # Predictable movement keeps tests independent of random seeds.
            close = round(base + period * 0.25 + symbol_index * 0.1, 2)
            open_price = round(close - 0.05, 2)
            bars.append(
                MarketBar(
                    symbol=symbol,
                    event_time=event_time,
                    open=open_price,
                    high=round(close + 0.15, 2),
                    low=round(open_price - 0.1, 2),
                    close=close,
                    volume=1_000 + period * 10 + symbol_index,
                )
            )
    return bars


def default_start() -> datetime:
    return datetime(2026, 1, 2, 14, 30, tzinfo=timezone.utc)
