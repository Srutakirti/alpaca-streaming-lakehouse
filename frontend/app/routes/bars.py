import time
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThanOrEqual

from frontend.app.iceberg_client import get_table

router = APIRouter()

_symbols_cache: tuple[list[str], float] = ([], 0.0)
_SYMBOLS_TTL = 60.0


@router.get("/symbols")
def list_symbols() -> list[str]:
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
def get_bars(
    symbol: str = Query(...),
    from_ts: Optional[str] = Query(None, alias="from"),
    to_ts: Optional[str] = Query(None, alias="to"),
    limit: int = Query(5000, le=10000),
) -> list[dict]:
    table = get_table()

    row_filter = EqualTo("S", symbol)
    if from_ts:
        row_filter = And(row_filter, GreaterThanOrEqual("t", from_ts))
    if to_ts:
        row_filter = And(row_filter, LessThanOrEqual("t", to_ts))

    arrow = table.scan(
        row_filter=row_filter,
        selected_fields=("t", "o", "h", "l", "c", "v"),
        limit=limit,
    ).to_arrow()

    if len(arrow) == 0:
        return []

    rows = arrow.to_pydict()
    return [
        {"t": rows["t"][i], "o": rows["o"][i], "h": rows["h"][i],
         "l": rows["l"][i], "c": rows["c"][i], "v": rows["v"][i]}
        for i in range(len(rows["t"]))
    ]
