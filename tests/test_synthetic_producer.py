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
