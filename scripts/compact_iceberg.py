#!/usr/bin/env python3
"""
Iceberg snapshot compaction.

Expires old snapshots from a PyIceberg table. PyIceberg 0.11 doesn't expose
manifest rewrite, but expiring snapshots collapses the manifest list a planner
must walk on every scan, which is the dominant cost on small frequent commits.

Examples:

  # Keep only the current snapshot (most aggressive; default).
  uv run --package load python scripts/compact_iceberg.py

  # Keep the last 5 snapshots.
  uv run --package load python scripts/compact_iceberg.py --keep-last 5

  # Expire anything older than 1 hour.
  uv run --package load python scripts/compact_iceberg.py --older-than-hours 1

  # Dry run.
  uv run --package load python scripts/compact_iceberg.py --keep-last 1 --dry-run

  # Override catalog/warehouse via flags or env (ICEBERG_CATALOG_URI, ICEBERG_WAREHOUSE).
  uv run --package load python scripts/compact_iceberg.py \\
    --catalog-uri sqlite:///./warehouse/catalog.db \\
    --warehouse ./warehouse \\
    --namespace alpaca --table bars --keep-last 1

NOTE: stop concurrent writers (the loader) before running, especially on SQLite.
"""
import argparse
import os
import sys
import time
from datetime import datetime, timezone, timedelta

from pyiceberg.catalog.sql import SqlCatalog


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Expire old Iceberg snapshots to reduce planner cost.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--catalog-name",
        default=os.environ.get("ICEBERG_CATALOG_NAME", "alpaca_catalog"),
        help="SqlCatalog name (must match the writer's; env: ICEBERG_CATALOG_NAME).",
    )
    p.add_argument(
        "--catalog-uri",
        default=os.environ.get("ICEBERG_CATALOG_URI", "sqlite:///./warehouse/catalog.db"),
        help="Iceberg catalog URI (env: ICEBERG_CATALOG_URI).",
    )
    p.add_argument(
        "--warehouse",
        default=os.environ.get("ICEBERG_WAREHOUSE", "./warehouse"),
        help="Iceberg warehouse path (env: ICEBERG_WAREHOUSE).",
    )
    p.add_argument(
        "--namespace",
        default=os.environ.get("ICEBERG_NAMESPACE", "alpaca"),
        help="Iceberg namespace (env: ICEBERG_NAMESPACE).",
    )
    p.add_argument(
        "--table",
        default=os.environ.get("ICEBERG_TABLE", "bars"),
        help="Iceberg table (env: ICEBERG_TABLE).",
    )
    grp = p.add_mutually_exclusive_group()
    grp.add_argument(
        "--keep-last",
        type=int,
        help="Keep only the N most recent snapshots; expire the rest.",
    )
    grp.add_argument(
        "--older-than-hours",
        type=float,
        help="Expire snapshots older than this many hours.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be expired and exit without committing.",
    )
    args = p.parse_args()
    if args.keep_last is None and args.older_than_hours is None:
        args.keep_last = 1
    return args


def main() -> int:
    args = parse_args()

    if args.warehouse.startswith(("./", "/")) and not args.warehouse.startswith("gs://"):
        os.makedirs(args.warehouse, exist_ok=True)

    catalog = SqlCatalog(args.catalog_name, uri=args.catalog_uri, warehouse=args.warehouse)
    table_name = f"{args.namespace}.{args.table}"
    table = catalog.load_table(table_name)

    snapshots = list(table.metadata.snapshots)
    snapshots.sort(key=lambda s: s.timestamp_ms)
    print(f"Table {table_name}: {len(snapshots)} snapshots")

    if not snapshots:
        print("No snapshots to expire.")
        return 0

    current = table.current_snapshot()
    current_id = current.snapshot_id if current else None

    if args.older_than_hours is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=args.older_than_hours)
        cutoff_ms = int(cutoff.timestamp() * 1000)
        candidates = [s for s in snapshots if s.timestamp_ms < cutoff_ms]
        print(
            f"Mode: older_than={args.older_than_hours}h "
            f"(cutoff {cutoff.isoformat()}) → {len(candidates)} candidates"
        )
    else:
        keep = max(1, args.keep_last)
        candidates = snapshots[:-keep] if len(snapshots) > keep else []
        print(f"Mode: keep_last={keep} → {len(candidates)} candidates")

    to_expire = [s.snapshot_id for s in candidates if s.snapshot_id != current_id]
    print(
        f"Excluding current snapshot ({current_id}); will expire {len(to_expire)} snapshots."
    )

    if not to_expire:
        print("Nothing to expire.")
        return 0

    if args.dry_run:
        print("--dry-run set; no changes committed.")
        return 0

    t0 = time.monotonic()
    table.maintenance.expire_snapshots().by_ids(to_expire).commit()
    elapsed = time.monotonic() - t0

    table = catalog.load_table(table_name)
    after = list(table.metadata.snapshots)
    print(
        f"Expired in {elapsed:.1f}s. Snapshots: {len(snapshots)} → {len(after)} "
        f"(removed {len(snapshots) - len(after)})."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
