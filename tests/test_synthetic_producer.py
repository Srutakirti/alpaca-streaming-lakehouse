from datetime import datetime, timezone

from gce_hadoop_catalog.synthetic_producer import generate_bars


def test_producer_is_bounded_deterministic_and_utc() -> None:
    start = datetime(2026, 1, 2, 14, 30, tzinfo=timezone.utc)
    first = generate_bars(start=start, periods=3, symbols=("AAPL", "MSFT"))
    second = generate_bars(start=start, periods=3, symbols=("AAPL", "MSFT"))

    assert first == second
    assert len(first) == 6
    assert [bar.symbol for bar in first[:2]] == ["AAPL", "MSFT"]
    assert all(bar.event_time.tzinfo == timezone.utc for bar in first)
    assert len({bar.event_id for bar in first}) == len(first)
    assert first[0].as_alpaca() == {
        "T": "b", "S": "AAPL", "o": 174.95, "h": 175.15, "l": 174.85,
        "c": 175.0, "v": 1000, "t": "2026-01-02T14:30:00.000000Z", "n": 10, "vw": 175.0,
    }
