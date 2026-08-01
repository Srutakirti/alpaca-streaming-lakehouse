import time
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Query
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThanOrEqual
from pyiceberg.types import TimestampType, TimestamptzType

from frontend.app.iceberg_client import get_table

router = APIRouter()

_symbols_cache: tuple[list[str], float] = ([], 0.0)
_SYMBOLS_TTL = 60.0


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _format_utc(value) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return value


def _table_uses_timestamp_t(table) -> bool:
    try:
        field_type = table.schema().find_field("t").field_type
        return isinstance(field_type, (TimestampType, TimestamptzType))
    except Exception:
        return False


@router.get("/symbols")
async def list_symbols() -> list[str]:
    global _symbols_cache
    symbols, cached_at = _symbols_cache
    if time.monotonic() - cached_at < _SYMBOLS_TTL and symbols:
        return symbols

    table = get_table()
    result = table.scan(selected_fields=("S",)).to_arrow()
    unique = sorted(result.column("S").unique().to_pylist())
    _symbols_cache = (unique, time.monotonic())
    return unique


@router.get("/bars")
async def get_bars(
    symbol: str = Query(...),
    from_ts: Optional[str] = Query(None, alias="from"),
    to_ts: Optional[str] = Query(None, alias="to"),
    limit: int = Query(5000, le=10000),
) -> list[dict]:
    table = get_table()
    timestamp_t = _table_uses_timestamp_t(table)

    row_filter = EqualTo("S", symbol)
    if from_ts:
        row_filter = And(row_filter, GreaterThanOrEqual("t", _parse_utc(from_ts) if timestamp_t else from_ts))
    if to_ts:
        row_filter = And(row_filter, LessThanOrEqual("t", _parse_utc(to_ts) if timestamp_t else to_ts))

    arrow = table.scan(
        row_filter=row_filter,
        selected_fields=("t", "o", "h", "l", "c", "v"),
        limit=limit,
    ).to_arrow()

    if len(arrow) == 0:
        return []

    rows = arrow.to_pydict()
    return [
        {"t": _format_utc(rows["t"][i]), "o": rows["o"][i], "h": rows["h"][i],
         "l": rows["l"][i], "c": rows["c"][i], "v": rows["v"][i]}
        for i in range(len(rows["t"]))
    ]
