import os
from datetime import datetime, timezone, timedelta
from typing import Optional
from functools import lru_cache

import google.cloud.logging

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
LOG_NAME_EXTRACTOR = os.environ.get("LOG_NAME_EXTRACTOR", "alpaca-extractor")
LOG_NAME_LOADER = os.environ.get("LOG_NAME_LOADER", "alpaca-loader")


@lru_cache(maxsize=1)
def _client():
    return google.cloud.logging.Client(project=PROJECT_ID)


def _tail_component(component: str, minutes: int = 5, max_results: int = 1) -> Optional[dict]:
    """Return the most recent metrics snapshot for a component, or None."""
    if not PROJECT_ID:
        return None
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
    return None


def get_last_extractor_metrics() -> Optional[dict]:
    return _tail_component(LOG_NAME_EXTRACTOR, minutes=15, max_results=1)


def get_last_loader_metrics() -> Optional[dict]:
    return _tail_component(LOG_NAME_LOADER, minutes=15, max_results=1)


def get_metrics_timeseries(component: str, minutes: int = 60) -> list[dict]:
    """Return time-ordered list of metric snapshots for a component."""
    if not PROJECT_ID:
        return []
    log_name = LOG_NAME_EXTRACTOR if component == "extractor" else LOG_NAME_LOADER
    since = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
    filter_ = (
        f'logName="projects/{PROJECT_ID}/logs/{log_name}" '
        f'timestamp>="{since}"'
    )
    try:
        entries = list(_client().list_entries(
            filter_=filter_,
            order_by=google.cloud.logging.ASCENDING,
            max_results=500,
            page_size=500,
        ))
        results = []
        for e in entries:
            if isinstance(e.payload, dict):
                row = dict(e.payload)
                row["_ts"] = e.timestamp.isoformat() if e.timestamp else None
                results.append(row)
        return results
    except Exception:
        return []
