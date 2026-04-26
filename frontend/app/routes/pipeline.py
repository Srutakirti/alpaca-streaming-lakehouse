from fastapi import APIRouter, Query

from frontend.app.iceberg_client import get_table
from frontend.app import logging_client

router = APIRouter()


def _iceberg_freshness() -> dict:
    try:
        table = get_table()
        snapshots = table.metadata.snapshots
        if not snapshots:
            return {"snapshot_count": 0, "latest_snapshot_ts": None}
        latest = max(snapshots, key=lambda s: s.timestamp_ms)
        return {
            "snapshot_count": len(snapshots),
            "latest_snapshot_ts": latest.timestamp_ms,
        }
    except Exception as e:
        return {"error": str(e)}


@router.get("/status")
def pipeline_status() -> dict:
    extractor = logging_client.get_last_extractor_metrics()
    loader = logging_client.get_last_loader_metrics()
    freshness = _iceberg_freshness()
    return {
        "extractor": extractor,
        "loader": loader,
        "iceberg": freshness,
    }


@router.get("/metrics")
def pipeline_metrics(
    component: str = Query("loader", pattern="^(loader|extractor)$"),
    minutes: int = Query(60, ge=1, le=1440),
) -> list[dict]:
    return logging_client.get_metrics_timeseries(component, minutes)
