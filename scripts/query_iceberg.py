#!/usr/bin/env python3
"""
Ad-hoc SQL against the Iceberg `alpaca.bars` table.

Loads the table via PyIceberg (respecting ICEBERG_CATALOG_URI / ICEBERG_WAREHOUSE
env vars, with sensible local defaults), registers it as a DuckDB view named
`bars`, then runs either:

  - the `--sql` string,
  - a generated query for `--symbol` (latest N rows for that symbol), or
  - the default summary query (per-symbol counts, OHLC range, volume).

Usage:
    # Default summary query against local sqlite warehouse
    uv run --with duckdb --package load python scripts/query_iceberg.py

    # Custom SQL
    uv run --with duckdb --package load python scripts/query_iceberg.py \\
        --sql "SELECT S, COUNT(*) FROM bars GROUP BY S"

    # Latest 20 bars for AAPL
    uv run --with duckdb --package load python scripts/query_iceberg.py --symbol AAPL --limit 20

    # Run against production GCS warehouse + Cloud SQL catalog.
    # Requires: cloud-sql-proxy <conn> --port 5432  running in another shell.
    uv run --with duckdb --package load python scripts/query_iceberg.py --prod

DuckDB view `bars` columns:
  rec_type (string)  — record type, always 'b' for bar (renamed from Iceberg "T")
  S        (string)  — symbol
  o,h,l,c  (double)  — open/high/low/close
  v        (bigint)  — volume
  t        (string)  — bar timestamp (ISO-8601 with trailing Z)
"""
import argparse
import os
import subprocess
import sys
import time

from pyiceberg.catalog.sql import SqlCatalog


DEFAULT_SUMMARY_QUERY = """
SELECT
    S                                         AS symbol,
    COUNT(*)                                  AS n_rows,
    MIN(t)                                    AS first_t,
    MAX(t)                                    AS last_t,
    ROUND(AVG(c)::DOUBLE, 2)                  AS avg_close,
    ROUND(MIN(l)::DOUBLE, 2)                  AS min_low,
    ROUND(MAX(h)::DOUBLE, 2)                  AS max_high,
    SUM(v)                                    AS total_volume
FROM bars
GROUP BY S
ORDER BY symbol
"""


def gcloud(*args: str) -> str:
    return subprocess.check_output(["gcloud", *args], text=True).strip()


def prod_settings() -> tuple[str, str]:
    """Return (catalog_uri, warehouse) for the production stack.

    Assumes cloud-sql-proxy is forwarding the Cloud SQL `alpaca-iceberg-catalog`
    instance to 127.0.0.1:5432.
    """
    project = os.environ.get("GCP_PROJECT_ID") or gcloud("config", "get-value", "project")
    password = gcloud("secrets", "versions", "access", "latest", "--secret=ICEBERG_DB_PASSWORD")
    catalog_uri = f"postgresql+psycopg2://iceberg:{password}@127.0.0.1:5432/iceberg"
    warehouse = f"gs://{project}-alpaca-iceberg-warehouse/"
    return catalog_uri, warehouse


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--sql", help="SQL query to run (overrides --symbol). DuckDB dialect; the table is `bars`.")
    p.add_argument("--symbol", help="Convenience: latest N bars for this symbol, ordered by t desc.")
    p.add_argument("--limit", type=int, default=20, help="Row cap for --symbol mode (default 20). Ignored when --sql is given.")
    p.add_argument("--prod", action="store_true",
                   help="Use production Cloud SQL catalog + GCS warehouse. Needs cloud-sql-proxy on :5432.")
    p.add_argument("--max-print", type=int, default=200, help="Max rows to print (default 200).")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    try:
        import duckdb
    except ImportError:
        print(
            "duckdb not installed. Re-run with:\n"
            "    uv run --with duckdb --package load python scripts/query_iceberg.py ...",
            file=sys.stderr,
        )
        return 1

    if args.prod:
        catalog_uri, warehouse = prod_settings()
    else:
        catalog_uri = os.environ.get("ICEBERG_CATALOG_URI", "sqlite:///./warehouse/catalog.db")
        warehouse = os.environ.get("ICEBERG_WAREHOUSE", "./warehouse")

    catalog_name = os.environ.get("ICEBERG_CATALOG_NAME", "alpaca_catalog")
    namespace = os.environ.get("ICEBERG_NAMESPACE", "alpaca")
    table_name = os.environ.get("ICEBERG_TABLE", "bars")

    catalog = SqlCatalog(catalog_name, uri=catalog_uri, warehouse=warehouse)
    table = catalog.load_table(f"{namespace}.{table_name}")

    # Mask password when echoing the catalog URI.
    safe_uri = catalog_uri
    if "@" in safe_uri and "://" in safe_uri:
        scheme, rest = safe_uri.split("://", 1)
        creds, host = rest.split("@", 1)
        if ":" in creds:
            user = creds.split(":", 1)[0]
            safe_uri = f"{scheme}://{user}:***@{host}"
    print(f"Catalog : {catalog_name} @ {safe_uri}")
    print(f"Table   : {namespace}.{table_name}  (snapshots={len(table.metadata.snapshots)})")

    t0 = time.monotonic()
    arrow = table.scan().to_arrow()
    scan_s = time.monotonic() - t0
    print(f"Scan    : {len(arrow)} rows in {scan_s:.2f}s")

    # DuckDB matches identifiers case-insensitively; Iceberg's "T" (record type)
    # and "t" (timestamp) collide on register. Rename "T" so "t" survives.
    if "T" in arrow.column_names:
        arrow = arrow.rename_columns(
            ["rec_type" if c == "T" else c for c in arrow.column_names]
        )

    if args.sql:
        query = args.sql
    elif args.symbol:
        query = (
            f"SELECT * FROM bars WHERE S = '{args.symbol}' "
            f"ORDER BY t DESC LIMIT {args.limit}"
        )
    else:
        query = DEFAULT_SUMMARY_QUERY

    con = duckdb.connect()
    con.register("bars", arrow)

    print(f"\nQuery   : {query.strip()}\n")
    t0 = time.monotonic()
    result = con.sql(query).to_arrow_table()
    query_s = time.monotonic() - t0

    rows = result.to_pylist()
    if not rows:
        print("(no rows)")
    else:
        cols = result.column_names
        widths = [
            max(len(c), max(len(str(r.get(c, ""))) for r in rows)) for c in cols
        ]
        sep = "  ".join("-" * w for w in widths)
        print("  ".join(c.ljust(w) for c, w in zip(cols, widths)))
        print(sep)
        for r in rows[:args.max_print]:
            print("  ".join(str(r.get(c, "")).ljust(w) for c, w in zip(cols, widths)))
        if len(rows) > args.max_print:
            print(f"... ({len(rows) - args.max_print} more rows truncated)")

    print(f"\nReturned {len(rows)} row(s) in {query_s*1000:.1f}ms.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
