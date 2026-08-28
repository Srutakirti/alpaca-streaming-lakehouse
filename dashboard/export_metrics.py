#!/usr/bin/env python3
"""Build a public-safe dashboard snapshot from fixed Cloud Logging queries."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo


EXTRACTOR_LOG = "gce_hadoop_catalog_extractor_json"
JOURNAL_LOG = "gce_hadoop_catalog_journal"
NEW_YORK = ZoneInfo("America/New_York")
COMMIT_PATTERN = re.compile(
    r"committed_at=(?P<committed_at>\S+) received=(?P<received>\d+) inserted=(?P<inserted>\d+)"
)


@dataclass(frozen=True)
class DashboardSettings:
    project_id: str
    bar_warning_minutes: int = 5
    bar_unhealthy_minutes: int = 10
    commit_warning_minutes: int = 10
    commit_unhealthy_minutes: int = 15
    history_limit: int = 48


def parse_utc(value: str) -> datetime:
    """Parse an RFC3339 timestamp into a UTC datetime."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def market_state(now: datetime) -> tuple[str, datetime]:
    """Return state and the next weekday 09:30 America/New_York opening in UTC."""
    local = now.astimezone(NEW_YORK)
    candidate = local.date()
    if local.weekday() >= 5:
        candidate += timedelta(days=7 - local.weekday())
        return "weekend", _opening_utc(candidate)

    pre_open = time(9, 30)
    close = time(16, 0)
    settled = time(17, 0)
    if local.time() < pre_open:
        return "pre_open", _opening_utc(candidate)
    if local.time() < close:
        return "market_open", _opening_utc(candidate + timedelta(days=1))
    if local.time() < settled:
        return "settling", _opening_utc(candidate + timedelta(days=1))
    return "closed", _opening_utc(candidate + timedelta(days=1))


def _opening_utc(candidate: datetime.date) -> datetime:
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return datetime.combine(candidate, time(9, 30), tzinfo=NEW_YORK).astimezone(UTC)


def fixed_log_filters(project_id: str, start: datetime) -> dict[str, str]:
    """Return the only Cloud Logging filters the public exporter may execute."""
    since = utc_text(start)
    return {
        "extractor": (
            f'logName="projects/{project_id}/logs/{EXTRACTOR_LOG}" AND '
            'jsonPayload._SYSTEMD_UNIT="alpaca-extractor.service" AND '
            f'timestamp >= "{since}"'
        ),
        "loader": (
            f'logName="projects/{project_id}/logs/{JOURNAL_LOG}" AND '
            'jsonPayload._SYSTEMD_UNIT="iceberg-loader.service" AND '
            f'timestamp >= "{since}"'
        ),
    }


def read_gcloud_logs(project_id: str, start: datetime) -> list[dict[str, Any]]:
    """Read bounded entries with fixed filters; callers cannot inject LQL."""
    entries: list[dict[str, Any]] = []
    for log_filter in fixed_log_filters(project_id, start).values():
        result = subprocess.run(
            [
                "gcloud",
                "logging",
                "read",
                log_filter,
                f"--project={project_id}",
                "--limit=500",
                "--order=asc",
                "--format=json",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        entries.extend(json.loads(result.stdout))
    return entries


def build_snapshot(
    entries: Iterable[dict[str, Any]], settings: DashboardSettings, now: datetime
) -> dict[str, Any]:
    """Build a public-safe metric document without forwarding raw log fields."""
    now = now.astimezone(UTC)
    state, next_open = market_state(now)
    extractor_events = []
    commits = []
    alerts = []

    for entry in entries:
        payload = entry.get("jsonPayload", {})
        unit = payload.get("_SYSTEMD_UNIT")
        entry_time = parse_utc(entry.get("timestamp", payload.get("timestamp", utc_text(now))))
        if unit == "alpaca-extractor.service":
            event = _extract_extractor_event(payload, entry_time)
            if event is not None:
                extractor_events.append(event)
            alert = _safe_alert("extractor", payload, entry_time)
            if alert is not None:
                alerts.append(alert)
        elif unit == "iceberg-loader.service":
            message = payload.get("MESSAGE", "")
            match = COMMIT_PATTERN.search(message)
            if match:
                commits.append(
                    {
                        "at_utc": utc_text(parse_utc(match.group("committed_at"))),
                        "received": int(match.group("received")),
                        "inserted": int(match.group("inserted")),
                    }
                )
            alert = _safe_alert("loader", payload, entry_time)
            if alert is not None:
                alerts.append(alert)

    extractor_events.sort(key=lambda event: event["at"])
    commits.sort(key=lambda commit: commit["at_utc"])
    alerts.sort(key=lambda alert: alert["at_utc"], reverse=True)

    extractor = _extractor_summary(extractor_events)
    loader = _loader_summary(commits, settings.history_limit)
    health = _health(state, now, extractor, loader, alerts, settings)
    return {
        "schema_version": 1,
        "generated_at_utc": utc_text(now),
        "market": {"state": state, "next_expected_open_utc": utc_text(next_open)},
        "health": health,
        "extractor": extractor,
        "loader": loader,
        "alerts": alerts[:20],
    }


def _extract_extractor_event(payload: dict[str, Any], entry_time: datetime) -> dict[str, Any] | None:
    fields = payload.get("fields")
    if not isinstance(fields, dict) or not isinstance(fields.get("message"), str):
        return None
    snapshot: dict[str, Any] = {}
    raw_snapshot = fields.get("snapshot")
    if isinstance(raw_snapshot, str):
        try:
            loaded = json.loads(raw_snapshot)
            if isinstance(loaded, dict):
                snapshot = loaded
        except json.JSONDecodeError:
            pass
    return {
        "at": entry_time,
        "message": fields["message"],
        "snapshot": snapshot,
        "idle_secs": fields.get("idle_secs"),
    }


def _safe_alert(component: str, payload: dict[str, Any], entry_time: datetime) -> dict[str, str] | None:
    level = str(payload.get("level", payload.get("severity", "INFO"))).upper()
    if level not in {"WARN", "WARNING", "ERROR", "CRITICAL"}:
        return None
    fields = payload.get("fields")
    message = fields.get("message", "") if isinstance(fields, dict) else payload.get("MESSAGE", "")
    if message == "no bar data within idle window; shutting down producer":
        return None
    return {
        "at_utc": utc_text(entry_time),
        "component": component,
        "severity": "ERROR" if level in {"ERROR", "CRITICAL"} else "WARNING",
        "code": _alert_code(component, str(message)),
    }


def _alert_code(component: str, message: str) -> str:
    message = message.lower()
    if "delivery" in message:
        return f"{component}_delivery_failure"
    if "authentication" in message or "authenticated" in message:
        return f"{component}_authentication"
    if "commit" in message:
        return f"{component}_commit_failure"
    return f"{component}_warning"


def _extractor_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    metrics = [event for event in events if event["message"] in {"metrics", "final metrics"}]
    latest = metrics[-1] if metrics else None
    final = next((event for event in reversed(events) if event["message"] == "final metrics"), None)
    shutdown = next(
        (event for event in reversed(events) if event["message"] == "no bar data within idle window; shutting down producer"),
        None,
    )
    snapshot = latest["snapshot"] if latest else {}
    return {
        "status": "clean_shutdown" if final else ("observed" if latest else "no_recent_session"),
        "last_event_utc": utc_text(events[-1]["at"]) if events else None,
        "last_bar_utc": snapshot.get("last_message_ts"),
        "messages_received": _integer(snapshot.get("messages_received")),
        "messages_sent": _integer(snapshot.get("messages_sent")),
        "delivery_failures": _integer(snapshot.get("delivery_failures")),
        "errors": _integer(snapshot.get("errors")),
        "final_metrics_at_utc": utc_text(final["at"]) if final else None,
        "shutdown_reason": "idle_window" if shutdown else None,
    }


def _loader_summary(commits: list[dict[str, Any]], limit: int) -> dict[str, Any]:
    latest = commits[-1] if commits else None
    return {
        "last_commit_utc": latest["at_utc"] if latest else None,
        "last_received": latest["received"] if latest else None,
        "last_inserted": latest["inserted"] if latest else None,
        "recent_commits": commits[-limit:],
    }


def _health(
    state: str,
    now: datetime,
    extractor: dict[str, Any],
    loader: dict[str, Any],
    alerts: list[dict[str, str]],
    settings: DashboardSettings,
) -> dict[str, Any]:
    reasons: list[str] = []
    status = "healthy"
    if state == "market_open":
        status = _apply_freshness(
            reasons, status, "extractor_bar", extractor["last_bar_utc"], now,
            settings.bar_warning_minutes, settings.bar_unhealthy_minutes,
        )
        status = _apply_freshness(
            reasons, status, "loader_commit", loader["last_commit_utc"], now,
            settings.commit_warning_minutes, settings.commit_unhealthy_minutes,
        )
    elif state == "settling" and extractor["final_metrics_at_utc"] is None:
        status = "warning"
        reasons.append("awaiting_clean_shutdown")
    elif state in {"closed", "weekend", "pre_open"} and extractor["status"] == "no_recent_session":
        status = "unknown"
        reasons.append("no_recent_session")
    if any(alert["severity"] == "ERROR" for alert in alerts):
        status = "unhealthy"
        reasons.append("recent_error")
    return {"status": status, "reasons": reasons}


def _apply_freshness(
    reasons: list[str], status: str, name: str, value: str | None, now: datetime,
    warning_minutes: int, unhealthy_minutes: int,
) -> str:
    if value is None:
        reasons.append(f"missing_{name}")
        return "unhealthy"
    age = now - parse_utc(value)
    if age > timedelta(minutes=unhealthy_minutes):
        reasons.append(f"stale_{name}")
        return "unhealthy"
    if age > timedelta(minutes=warning_minutes) and status != "unhealthy":
        reasons.append(f"aging_{name}")
        return "warning"
    return status


def _integer(value: Any) -> int | None:
    return value if isinstance(value, int) else None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default=None, help="GCP project for fixed live Cloud Logging queries")
    parser.add_argument("--input", type=Path, help="Saved Cloud Logging JSON entries for local testing")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--now", help="RFC3339 UTC timestamp for reproducible testing")
    parser.add_argument("--lookback-hours", type=int, default=36)
    args = parser.parse_args()
    if bool(args.project) == bool(args.input):
        parser.error("provide exactly one of --project or --input")
    now = parse_utc(args.now) if args.now else datetime.now(UTC)
    if args.input:
        entries = json.loads(args.input.read_text())
        project_id = "public-dashboard-fixture"
    else:
        project_id = args.project
        entries = read_gcloud_logs(project_id, now - timedelta(hours=args.lookback_hours))
    snapshot = build_snapshot(entries, DashboardSettings(project_id=project_id), now)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
