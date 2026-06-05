#!/usr/bin/env python3
"""
Delete synthetic test rows from the Iceberg `alpaca.bars` table by symbol.

Synthetic E2E validation injects bars through the live topic → loader → the
production Iceberg table, so those rows must be cleaned up afterward. The
synthetic generator is run with distinctive symbols (default ZZSYNTH1/ZZSYNTH2)
that never collide with real tickers, so they can be removed with a targeted
row-level delete.

Loads the table via PyIceberg (respecting ICEBERG_CATALOG_URI /
ICEBERG_WAREHOUSE, same as scripts/query_iceberg.py) and calls
`table.delete(delete_filter=In("S", symbols))`, which commits a new snapshot
with the matching rows removed.

Usage:
    # Local sqlite warehouse, default ZZSYNTH* symbols, with confirmation
    uv run --package load python scripts/delete_synthetic.py

    # Count only, do not delete
    uv run --package load python scripts/delete_synthetic.py --dry-run

    # Custom symbols, no prompt
    uv run --package load python scripts/delete_synthetic.py --symbols ZZSYNTH1 FOO --yes

    # Production Cloud SQL catalog + GCS warehouse.
    # Requires: cloud-sql-proxy <conn> --port 5432  running in another shell.
    uv run --package load python scripts/delete_synthetic.py --prod --yes
"""
import argparse
import os
import subprocess
import sys

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import In

DEFAULT_SYMBOLS = ["ZZSYNTH1", "ZZSYNTH2"]


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
    p.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS,
                   help=f"Symbols (S column) to delete. Default: {' '.join(DEFAULT_SYMBOLS)}")
    p.add_argument("--prod", action="store_true",
                   help="Use production Cloud SQL catalog + GCS warehouse. Needs cloud-sql-proxy on :5432.")
    p.add_argument("--dry-run", action="store_true", help="Count matching rows but do not delete.")
    p.add_argument("--yes", action="store_true", help="Skip the confirmation prompt.")
    return p.parse_args()


def main() -> int:
    args = parse_args()

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

    symbols = set(args.symbols)
    row_filter = In("S", symbols)

    print(f"Catalog : {catalog_name} @ {safe_uri}")
    print(f"Table   : {namespace}.{table_name}")
    print(f"Symbols : {sorted(symbols)}")

    # Count matches first so the operator sees the blast radius.
    matched = len(table.scan(row_filter=row_filter).to_arrow())
    print(f"Matched : {matched} row(s)")

    if matched == 0:
        print("Nothing to delete.")
        return 0

    if args.dry_run:
        print("Dry run — no rows deleted.")
        return 0

    if not args.yes:
        reply = input(f"Delete {matched} row(s) for {sorted(symbols)}? [y/N] ").strip().lower()
        if reply not in ("y", "yes"):
            print("Aborted.")
            return 1

    table.delete(delete_filter=row_filter)

    remaining = len(table.scan(row_filter=row_filter).to_arrow())
    print(f"Deleted. Remaining matching rows: {remaining}")
    return 0 if remaining == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
