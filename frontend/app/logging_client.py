import os
import ast
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
from functools import lru_cache

import google.cloud.logging

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
LOG_NAME_EXTRACTOR = os.environ.get("LOG_NAME_EXTRACTOR", "alpaca-extractor")
LOG_NAME_LOADER = os.environ.get("LOG_NAME_LOADER", "alpaca-loader")
LOCAL_LOG_DIR = os.environ.get("LOCAL_PIPELINE_LOG_DIR", ".local-run/logs")


@lru_cache(maxsize=1)
def _client():
    return google.cloud.logging.Client(project=PROJECT_ID)


def _tail_component(component: str, minutes: int = 5, max_results: int = 1) -> Optional[dict]:
    """Return the most recent metrics snapshot for a component, or None."""
    if not PROJECT_ID:
        rows = _local_metrics(component, minutes=minutes)
        return rows[-1] if rows else None
    since = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
    filter_ = (
        f'logName="projects/{PROJECT_ID}/logs/{component}" '
        f'timestamp>="{since}"'
    )
    try:
        entries = list(_client().list_entries(
            filter_=filter_,
            order_by=google.cloud.logging.DESCENDING,
            max_results=max_results,
            page_size=max_results,
        ))
        if entries:
            payload = entries[0].payload
            if isinstance(payload, dict):
                return payload
    except Exception:
        pass
    if component == LOG_NAME_EXTRACTOR:
        return _tail_extractor_cloud_run_metrics(minutes=minutes, max_results=max_results)
    return None


def get_last_extractor_metrics() -> Optional[dict]:
    return _tail_component(LOG_NAME_EXTRACTOR, minutes=15, max_results=1)


def get_last_loader_metrics() -> Optional[dict]:
    return _tail_component(LOG_NAME_LOADER, minutes=15, max_results=1)


def get_metrics_timeseries(component: str, minutes: int = 60) -> list[dict]:
    """Return time-ordered list of metric snapshots for a component."""
    if not PROJECT_ID:
        log_name = LOG_NAME_EXTRACTOR if component == "extractor" else LOG_NAME_LOADER
        return _local_metrics(log_name, minutes=minutes)
    log_name = LOG_NAME_EXTRACTOR if component == "extractor" else LOG_NAME_LOADER
    since = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
    filter_ = (
        f'logName="projects/{PROJECT_ID}/logs/{log_name}" '
        f'timestamp>="{since}"'
    )
    results: list[dict] = []
    try:
        entries = list(_client().list_entries(
            filter_=filter_,
            order_by=google.cloud.logging.ASCENDING,
            max_results=500,
            page_size=500,
        ))
        for e in entries:
            if isinstance(e.payload, dict):
                row = dict(e.payload)
                row["_ts"] = e.timestamp.isoformat() if e.timestamp else None
                results.append(row)
        if results or component != "extractor":
            return results
        return _extractor_cloud_run_metrics_timeseries(minutes)
    except Exception:
        if component == "extractor":
            return _extractor_cloud_run_metrics_timeseries(minutes)
        return results


def _tail_extractor_cloud_run_metrics(minutes: int, max_results: int) -> Optional[dict]:
    rows = _extractor_cloud_run_metrics(minutes, order_by=google.cloud.logging.DESCENDING, max_results=max_results)
    return rows[0] if rows else None


def _extractor_cloud_run_metrics_timeseries(minutes: int) -> list[dict]:
    return _extractor_cloud_run_metrics(minutes, order_by=google.cloud.logging.ASCENDING, max_results=500)


def _extractor_cloud_run_metrics(minutes: int, order_by: str, max_results: int) -> list[dict]:
    since = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
    filter_ = (
        'resource.type="cloud_run_job" '
        'resource.labels.job_name="alpaca-extractor" '
        'jsonPayload.fields.message="metrics" '
        f'timestamp>="{since}"'
    )
    try:
        entries = list(_client().list_entries(
            filter_=filter_,
            order_by=order_by,
            max_results=max_results,
            page_size=max_results,
        ))
    except Exception:
        return []

    rows: list[dict] = []
    for entry in entries:
        row = _parse_extractor_payload(entry.payload)
        if not row:
            continue
        row["_ts"] = entry.timestamp.isoformat() if entry.timestamp else None
        rows.append(row)
    return rows


def _local_metrics(component: str, minutes: int) -> list[dict]:
    path = _local_log_path(component)
    if not path.exists():
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    rows: list[dict] = []
    try:
        for line in path.read_text(errors="replace").splitlines():
            row = _parse_local_metric_line(component, line)
            if not row:
                continue
            ts = _parse_ts(row.get("_ts"))
            if ts is None or ts >= cutoff:
                rows.append(row)
    except OSError:
        return []
    return rows


def _local_log_path(component: str) -> Path:
    if component == LOG_NAME_EXTRACTOR:
        return Path(LOCAL_LOG_DIR) / "extractor.log"
    if component == LOG_NAME_LOADER:
        return Path(LOCAL_LOG_DIR) / "loader.log"
    return Path(LOCAL_LOG_DIR) / f"{component}.log"


def _parse_local_metric_line(component: str, line: str) -> Optional[dict]:
    if component == LOG_NAME_EXTRACTOR:
        return _parse_extractor_log(line)
    if component == LOG_NAME_LOADER:
        return _parse_loader_log(line)
    return None


def _parse_extractor_log(line: str) -> Optional[dict]:
    try:
        entry = json.loads(line)
        row = _parse_extractor_payload(entry)
        if row is None:
            return None
        row["_ts"] = entry.get("timestamp")
        return row
    except (json.JSONDecodeError, TypeError):
        return None


def _parse_extractor_payload(payload) -> Optional[dict]:
    if not isinstance(payload, dict):
        return None
    try:
        fields = payload.get("fields", {})
        if fields.get("message") != "metrics":
            return None
        row = json.loads(fields["snapshot"])
        return row if isinstance(row, dict) else None
    except (json.JSONDecodeError, KeyError, TypeError):
        return None


def _parse_loader_log(line: str) -> Optional[dict]:
    marker = "INFO: "
    if marker not in line or "'component': 'alpaca-loader'" not in line:
        return None
    try:
        row = ast.literal_eval(line.split(marker, 1)[1])
        if not isinstance(row, dict):
            return None
        row["_ts"] = _loader_line_ts(line)
        return row
    except (ValueError, SyntaxError):
        return None


def _loader_line_ts(line: str) -> Optional[str]:
    try:
        prefix = line.split(" INFO:", 1)[0]
        prefix = prefix.removesuffix("Z")
        parsed = datetime.strptime(prefix, "%Y-%m-%d %H:%M:%S,%f")
        return parsed.replace(tzinfo=timezone.utc).isoformat()
    except (ValueError, IndexError):
        return None


def _parse_ts(value) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
