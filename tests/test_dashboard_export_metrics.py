import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from dashboard.export_metrics import (
    DashboardSettings,
    build_snapshot,
    fixed_log_filters,
    market_state,
    read_iceberg_table_metadata,
    read_gcloud_logs,
    summarize_table_metadata,
)


FIXTURE = Path(__file__).parent / "fixtures" / "dashboard" / "market_close.json"
TABLE_FIXTURE = Path(__file__).parent / "fixtures" / "dashboard" / "table_metadata.json"


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
    assert "example-project" not in json.dumps(snapshot)
    assert snapshot["table"]["status"] == "unavailable"


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


def test_gcloud_reads_newest_entries_before_applying_its_limit(monkeypatch) -> None:
    commands = []

    def fake_run(command, **_kwargs):
        commands.append(command)
        return SimpleNamespace(stdout="[]")

    monkeypatch.setattr("dashboard.export_metrics.subprocess.run", fake_run)

    assert read_gcloud_logs("example-project", datetime(2026, 8, 27, tzinfo=UTC)) == []
    assert len(commands) == 2
    assert all("--order=desc" in command for command in commands)
    assert all("--limit=500" in command for command in commands)


def test_new_active_metrics_do_not_reuse_a_prior_session_shutdown() -> None:
    entries = json.loads(FIXTURE.read_text())
    entries.append(
        {
            "timestamp": "2026-08-28T16:59:27Z",
            "jsonPayload": {
                "_SYSTEMD_UNIT": "alpaca-extractor.service",
                "level": "INFO",
                "fields": {
                    "message": "metrics",
                    "snapshot": (
                        '{"connection_status":true,"delivery_failures":0,"errors":0,'
                        '"last_message_ts":"2026-08-28T16:59:00Z",'
                        '"messages_received":1934,"messages_sent":1934}'
                    ),
                },
            },
        }
    )

    snapshot = build_snapshot(
        entries,
        DashboardSettings(project_id="example-project"),
        datetime(2026, 8, 28, 17, 0, tzinfo=UTC),
    )

    assert snapshot["extractor"]["status"] == "observed"
    assert snapshot["extractor"]["final_metrics_at_utc"] is None
    assert snapshot["extractor"]["shutdown_reason"] is None
    assert snapshot["extractor"]["last_bar_utc"] == "2026-08-28T16:59:00Z"


def test_table_metadata_summary_exposes_only_safe_aggregate_metrics() -> None:
    table = summarize_table_metadata(json.loads(TABLE_FIXTURE.read_text()))

    assert table == {
        "status": "available",
        "reason": None,
        "last_metadata_update_utc": "2026-08-31T21:00:47Z",
        "current_snapshot_commit_utc": "2026-08-31T21:00:47Z",
        "latest_operation": "append",
        "latest_added_records": 19,
        "latest_added_data_files": 1,
        "latest_added_files_size_bytes": 3701,
        "total_records": 4623785,
        "total_data_files": 4530,
        "total_files_size_bytes": 899223658,
        "average_rows_per_data_file": 1020.7,
        "average_data_file_size_bytes": 198504.12,
        "total_delete_files": 0,
        "total_position_deletes": 0,
        "total_equality_deletes": 0,
        "snapshot_history_count": 1,
        "metadata_history_count": 2,
    }
    rendered = json.dumps(table)
    assert "private-table-uuid" not in rendered
    assert "private-bucket" not in rendered
    assert "manifest-list" not in rendered


def test_invalid_table_metadata_is_safe_and_does_not_break_pipeline_health() -> None:
    snapshot = build_snapshot(
        [],
        DashboardSettings(project_id="example-project"),
        datetime(2026, 8, 27, 15, 0, tzinfo=UTC),
        {"current-snapshot-id": 1, "snapshots": []},
    )

    assert snapshot["table"]["status"] == "unavailable"
    assert snapshot["table"]["reason"] == "missing_current_snapshot"
    assert snapshot["health"]["status"] == "unhealthy"


def test_iceberg_metadata_reader_uses_only_hint_and_current_metadata(monkeypatch) -> None:
    calls = []
    metadata = json.loads(TABLE_FIXTURE.read_text())

    def fake_storage_text(uri):
        calls.append(uri)
        return "4531\n" if uri.endswith("version-hint.text") else json.dumps(metadata)

    monkeypatch.setattr("dashboard.export_metrics._gcloud_storage_text", fake_storage_text)

    assert read_iceberg_table_metadata("gs://example-bucket/warehouse/table/metadata") == metadata
    assert calls == [
        "gs://example-bucket/warehouse/table/metadata/version-hint.text",
        "gs://example-bucket/warehouse/table/metadata/v4531.metadata.json",
    ]


def test_iceberg_metadata_reader_rejects_bad_hint_without_fetching_metadata(monkeypatch) -> None:
    calls = []

    def fake_storage_text(uri):
        calls.append(uri)
        return "v4531\n"

    monkeypatch.setattr("dashboard.export_metrics._gcloud_storage_text", fake_storage_text)

    try:
        read_iceberg_table_metadata("gs://example-bucket/warehouse/table/metadata")
    except ValueError as error:
        assert "version hint" in str(error)
    else:
        raise AssertionError("bad version hint was accepted")
    assert calls == ["gs://example-bucket/warehouse/table/metadata/version-hint.text"]
