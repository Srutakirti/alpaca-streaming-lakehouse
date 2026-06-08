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

        # max(t) over the data — end-to-end freshness vs. simulated/source clock.
        # TODO: for large warehouses, read manifest upper-bounds instead of scanning.
        try:
            arrow = table.scan(selected_fields=("t",)).to_arrow()
            if len(arrow) > 0:
                import pyarrow.compute as pc
                result["latest_record_t"] = pc.max(arrow.column("t")).as_py()
                result["row_count"] = len(arrow)
        except Exception as e:
            result["latest_record_t_error"] = str(e)
        return result
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
