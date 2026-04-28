#!/usr/bin/env python3
"""
Hit every backend endpoint the frontend uses and print the responses as ASCII
tables. Useful as a smoke test, a UTC-display validator, and a debug tool when
the dashboard "looks weird".

Endpoints (defined in frontend/app/routes/):
  GET /api/health
  GET /api/symbols                                  (bars.router)
  GET /api/bars?symbol=...&limit=...                (bars.router)
  GET /api/pipeline/status                          (pipeline.router)
  GET /api/pipeline/metrics?component=...&minutes=N (pipeline.router)

Run against the local FastAPI dev server:
    uv run --with requests python scripts/inspect_frontend_api.py

Run against the deployed Cloud Run frontend:
    uv run --with requests python scripts/inspect_frontend_api.py \\
        --base-url "$(terraform -chdir=terraform output -raw frontend_service_url)" \\
        --bearer "$(gcloud auth print-identity-token)"

Every timestamp column ends up in two forms in the output:
  - the raw value as the API returned it
  - a derived `*_utc_human` column normalized to "YYYY-MM-DD HH:MM:SS UTC"
so you can eyeball whether the frontend is rendering the same instant.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Any

try:
    import requests
except ImportError:
    print(
        "requests not installed. Re-run with:\n"
        "    uv run --with requests python scripts/inspect_frontend_api.py ...",
        file=sys.stderr,
    )
    sys.exit(1)


# Columns whose values are timestamps the dashboard renders as clock times.
# For each such column we emit a derived "<col>_utc_human" column.
TIMESTAMP_COLUMNS = {"_ts", "latest_record_t", "last_commit_ts", "t"}
# Columns that are unix-epoch milliseconds (PyIceberg snapshot ts).
EPOCH_MS_COLUMNS = {"latest_snapshot_ts"}


def to_utc_human(value: Any) -> str:
    if value is None or value == "":
        return ""
    try:
        if isinstance(value, (int, float)):
            # Heuristic: > 1e12 → milliseconds, else seconds.
            ts = float(value)
            d = datetime.fromtimestamp(ts / 1000.0 if ts > 1e12 else ts, tz=timezone.utc)
        else:
            s = str(value).replace("Z", "+00:00")
            d = datetime.fromisoformat(s).astimezone(timezone.utc)
        return d.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return f"<unparseable: {value!r}>"


def enrich_row(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    for k, v in list(row.items()):
        if k in TIMESTAMP_COLUMNS or k in EPOCH_MS_COLUMNS:
            out[f"{k}_utc_human"] = to_utc_human(v)
    return out


def print_table(rows: list[dict[str, Any]], max_rows: int = 200) -> None:
    if not rows:
        print("(no rows)")
        return
    cols: list[str] = []
    seen: set[str] = set()
    for r in rows:
        for k in r:
            if k not in seen:
                cols.append(k)
                seen.add(k)
    widths = [
        max(len(c), max(len(str(r.get(c, ""))) for r in rows))
        for c in cols
    ]
    header = "  ".join(c.ljust(w) for c, w in zip(cols, widths))
    sep = "  ".join("-" * w for w in widths)
    print(header)
    print(sep)
    for r in rows[:max_rows]:
        print("  ".join(str(r.get(c, "")).ljust(w) for c, w in zip(cols, widths)))
    if len(rows) > max_rows:
        print(f"... ({len(rows) - max_rows} more rows truncated)")


def print_kv(d: dict[str, Any]) -> None:
    if not d:
        print("(empty)")
        return
    enriched = enrich_row(d)
    width = max(len(k) for k in enriched)
    for k, v in enriched.items():
        printable = json.dumps(v) if isinstance(v, (dict, list)) else str(v)
        print(f"  {k.ljust(width)}  {printable}")


def section(title: str) -> None:
    print()
    print(f"=== {title} ===")


def get(session: requests.Session, base: str, path: str, params: dict | None = None) -> tuple[int, Any]:
    url = base.rstrip("/") + path
    try:
        r = session.get(url, params=params, timeout=30)
        return r.status_code, (r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text)
    except Exception as e:
        return 0, {"error": f"request failed: {e}"}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--base-url", default="http://localhost:8080",
                   help="Frontend base URL. Default: http://localhost:8080")
    p.add_argument("--bearer", default=None,
                   help="Optional Bearer token (e.g. `gcloud auth print-identity-token` for private Cloud Run)")
    p.add_argument("--minutes", type=int, default=60,
                   help="Lookback window for /api/pipeline/metrics (default 60)")
    p.add_argument("--symbol", default=None,
                   help="Symbol for /api/bars. Defaults to the first entry from /api/symbols.")
    p.add_argument("--limit", type=int, default=50,
                   help="Row cap for /api/bars (default 50)")
    p.add_argument("--max-rows", type=int, default=200,
                   help="Max rows per printed table (default 200)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    session = requests.Session()
    if args.bearer:
        session.headers["Authorization"] = f"Bearer {args.bearer}"

    print(f"Base URL: {args.base_url}")

    section("/api/health")
    code, health = get(session, args.base_url, "/api/health")
    print(f"HTTP {code}: {health}")
    if code != 200:
        print("Health check failed — aborting.", file=sys.stderr)
        return 1

    section("/api/symbols")
    code, symbols = get(session, args.base_url, "/api/symbols")
    print(f"HTTP {code}")
    if isinstance(symbols, list):
        print_table([{"symbol": s} for s in symbols], args.max_rows)
    else:
        print(symbols)

    section("/api/pipeline/status")
    code, status = get(session, args.base_url, "/api/pipeline/status")
    print(f"HTTP {code}")
    if isinstance(status, dict):
        for block in ("extractor", "loader", "iceberg"):
            print(f"\n-- {block} --")
            value = status.get(block)
            if value is None:
                print("(null — set GCP_PROJECT_ID on the frontend service to enable)")
            elif isinstance(value, dict):
                print_kv(value)
            else:
                print(value)
    else:
        print(status)

    for component in ("loader", "extractor"):
        section(f"/api/pipeline/metrics?component={component}&minutes={args.minutes}")
        code, metrics = get(
            session, args.base_url, "/api/pipeline/metrics",
            {"component": component, "minutes": args.minutes},
        )
        print(f"HTTP {code}")
        if isinstance(metrics, list) and metrics:
            print_table([enrich_row(r) for r in metrics], args.max_rows)
        elif isinstance(metrics, list):
            print("(no metrics in window)")
        else:
            print(metrics)

    chosen_symbol = args.symbol
    if not chosen_symbol and isinstance(symbols, list) and symbols:
        chosen_symbol = symbols[0]
    if chosen_symbol:
        section(f"/api/bars?symbol={chosen_symbol}&limit={args.limit}")
        code, bars = get(
            session, args.base_url, "/api/bars",
            {"symbol": chosen_symbol, "limit": args.limit},
        )
        print(f"HTTP {code}")
        if isinstance(bars, list) and bars:
            print_table([enrich_row(r) for r in bars], args.max_rows)
        elif isinstance(bars, list):
            print("(no bars in default range)")
        else:
            print(bars)
    else:
        section("/api/bars")
        print("Skipped — no symbol available (no --symbol given and /api/symbols returned empty).")

    return 0


if __name__ == "__main__":
    sys.exit(main())
