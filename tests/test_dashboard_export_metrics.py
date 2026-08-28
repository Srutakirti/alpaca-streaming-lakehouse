import json
from datetime import UTC, datetime
from pathlib import Path

from dashboard.export_metrics import DashboardSettings, build_snapshot, fixed_log_filters, market_state


FIXTURE = Path(__file__).parent / "fixtures" / "dashboard" / "market_close.json"


def test_market_close_fixture_is_sanitized_and_reports_clean_shutdown() -> None:
    entries = json.loads(FIXTURE.read_text())
    snapshot = build_snapshot(
        entries,
        DashboardSettings(project_id="example-project"),
        datetime(2026, 8, 27, 21, 20, tzinfo=UTC),
    )

    assert snapshot["market"]["state"] == "closed"
    assert snapshot["health"] == {"status": "healthy", "reasons": []}
    assert snapshot["extractor"]["status"] == "clean_shutdown"
    assert snapshot["extractor"]["messages_sent"] == 4167
    assert snapshot["extractor"]["shutdown_reason"] == "idle_window"
    assert snapshot["loader"]["last_inserted"] == 1000
    assert "MESSAGE" not in json.dumps(snapshot)
    assert "_SYSTEMD_UNIT" not in json.dumps(snapshot)


def test_market_open_marks_stale_bar_and_commit_unhealthy() -> None:
    snapshot = build_snapshot(
        [],
        DashboardSettings(project_id="example-project"),
        datetime(2026, 8, 27, 15, 0, tzinfo=UTC),
    )

    assert snapshot["market"]["state"] == "market_open"
    assert snapshot["health"]["status"] == "unhealthy"
    assert set(snapshot["health"]["reasons"]) == {"missing_extractor_bar", "missing_loader_commit"}


def test_market_state_uses_new_york_daylight_saving_time() -> None:
    state, next_open = market_state(datetime(2026, 8, 27, 13, 29, tzinfo=UTC))

    assert state == "pre_open"
    assert next_open == datetime(2026, 8, 27, 13, 30, tzinfo=UTC)


def test_fixed_filters_are_bounded_and_do_not_accept_user_lql() -> None:
    filters = fixed_log_filters("example-project", datetime(2026, 8, 27, 0, 0, tzinfo=UTC))

    assert set(filters) == {"extractor", "loader"}
    assert "alpaca-extractor.service" in filters["extractor"]
    assert "iceberg-loader.service" in filters["loader"]
    assert "2026-08-27T00:00:00Z" in filters["loader"]
