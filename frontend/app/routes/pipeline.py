from fastapi import APIRouter, Query

from frontend.app.iceberg_client import get_table
from frontend.app import logging_client

router = APIRouter()


def _iceberg_freshness() -> dict:
    try:
        table = get_table()
        snapshots = table.metadata.snapshots
        result: dict = {"snapshot_count": len(snapshots) if snapshots else 0}
        if snapshots:
            latest = max(snapshots, key=lambda s: s.timestamp_ms)
            result["latest_snapshot_ts"] = latest.timestamp_ms
        else:
            result["latest_snapshot_ts"] = None

        try:
            latest_record_t, row_count = _latest_record_t_from_file_stats(table)
            if latest_record_t and _is_valid_record_t(latest_record_t):
                result["latest_record_t"] = latest_record_t
                result["latest_record_t_source"] = "file_stats"
            else:
                latest_record_t = _latest_record_t_from_scan(table)
                if latest_record_t:
                    result["latest_record_t"] = latest_record_t
                result["latest_record_t_source"] = "max_t_scan"
            result["row_count"] = row_count
        except Exception as e:
            result["latest_record_t_source"] = "unavailable"
            result["latest_record_t_error"] = str(e)
        return result
    except Exception as e:
        return {"error": str(e)}


def _format_utc(value) -> str:
    from datetime import datetime, timezone

    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(value) if value is not None else ""


def _is_valid_record_t(value: str) -> bool:
    from datetime import datetime

    if not isinstance(value, str) or not value:
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def _latest_record_t_from_file_stats(table) -> tuple[str, int]:
    files = table.inspect.data_files()
    if len(files) == 0:
        return "", 0

    latest = ""
    row_count = 0
    for row in files.to_pylist():
        row_count += row.get("record_count") or 0
        metrics = row.get("readable_metrics") or {}
        t_metrics = metrics.get("t") or {}
        upper_bound = _format_utc(t_metrics.get("upper_bound"))
        if upper_bound and upper_bound > latest:
            latest = upper_bound
    return latest, row_count


def _latest_record_t_from_scan(table) -> str:
    arrow = table.scan(selected_fields=("t",)).to_arrow()
    if len(arrow) == 0:
        return ""

    latest = ""
    for value in arrow.column("t").to_pylist():
        formatted = _format_utc(value)
        if _is_valid_record_t(formatted) and formatted > latest:
            latest = formatted
    return latest


@router.get("/status")
async def pipeline_status() -> dict:
    extractor = logging_client.get_last_extractor_metrics()
    loader = logging_client.get_last_loader_metrics()
    freshness = _iceberg_freshness()
    return {
        "extractor": extractor,
        "loader": loader,
        "iceberg": freshness,
    }


@router.get("/metrics")
async def pipeline_metrics(
    component: str = Query("loader", pattern="^(loader|extractor)$"),
    minutes: int = Query(60, ge=1, le=1440),
) -> list[dict]:
    return logging_client.get_metrics_timeseries(component, minutes)
