#!/usr/bin/env python3
"""Run one guarded Iceberg data-file compaction in a Dataproc Spark batch."""

from __future__ import annotations

import argparse
import json
import re
from datetime import UTC, datetime
from typing import Any


IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,1023}$")
GCS_OBJECT_URI = re.compile(r"^gs://[a-z0-9._-]+/[A-Za-z0-9._/-]+\.json$")


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def qualified_table(catalog: str, namespace: str, table: str) -> str:
    for label, value in (("catalog", catalog), ("namespace", namespace), ("table", table)):
        if not IDENTIFIER.fullmatch(value):
            raise ValueError(f"invalid {label}")
    return f"{catalog}.{namespace}.{table}"


def snapshot_metrics(spark: Any, table: str) -> dict[str, Any]:
    snapshot = spark.sql(
        f"SELECT snapshot_id, committed_at, summary FROM {table}.snapshots "
        "ORDER BY committed_at DESC LIMIT 1"
    ).first()
    if snapshot is None:
        raise RuntimeError("table has no current snapshot")
    summary = dict(snapshot.summary or {})
    file_count = spark.sql(f"SELECT COUNT(*) AS count FROM {table}.files").first().count
    total_records = summary.get("total-records")
    if total_records is None:
        raise RuntimeError("current snapshot does not report total-records")
    return {
        "snapshot_id": str(snapshot.snapshot_id),
        "committed_at_utc": snapshot.committed_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "total_records": int(total_records),
        "data_file_count": int(file_count),
    }


def write_receipt(spark: Any, uri: str, receipt: dict[str, Any]) -> None:
    if not GCS_OBJECT_URI.fullmatch(uri):
        raise ValueError("receipt URI must be a gs:// object ending in .json")
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(uri)
    output = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).create(path, True)
    try:
        output.write(jvm.java.lang.String(json.dumps(receipt, sort_keys=True)).getBytes("UTF-8"))
    finally:
        output.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="hadoop")
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--expected-snapshot-id", required=True)
    parser.add_argument("--target-file-size-bytes", type=int, default=134_217_728)
    parser.add_argument("--receipt-uri", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    if args.target_file_size_bytes < 8 * 1024 * 1024:
        parser.error("target-file-size-bytes must be at least 8 MiB")
    return args


def main() -> None:
    from pyspark.sql import SparkSession

    args = parse_args()
    table = qualified_table(args.catalog, args.namespace, args.table)
    spark = SparkSession.builder.appName(f"iceberg-compaction-{args.run_id}").getOrCreate()
    started_at = utc_now()
    try:
        before = snapshot_metrics(spark, table)
        if before["snapshot_id"] != args.expected_snapshot_id:
            raise RuntimeError(
                "table changed after preflight: "
                f"expected {args.expected_snapshot_id}, found {before['snapshot_id']}"
            )

        rewrite = spark.sql(
            f"CALL {args.catalog}.system.rewrite_data_files("
            f"table => '{args.namespace}.{args.table}', "
            "options => map("
            f"'target-file-size-bytes', '{args.target_file_size_bytes}'"
            "))"
        ).first().asDict(recursive=True)
        manifests = spark.sql(
            f"CALL {args.catalog}.system.rewrite_manifests(table => '{args.namespace}.{args.table}')"
        ).collect()
        after = snapshot_metrics(spark, table)
        failed_files = int(rewrite.get("failed_data_files_count", 0))
        if failed_files:
            raise RuntimeError(f"rewrite_data_files reported {failed_files} failed data files")
        if after["snapshot_id"] == before["snapshot_id"]:
            raise RuntimeError("compaction did not create a new snapshot")
        if after["total_records"] != before["total_records"]:
            raise RuntimeError("total-records changed during compaction")
        if after["data_file_count"] >= before["data_file_count"]:
            raise RuntimeError("data-file count did not decrease")

        write_receipt(
            spark,
            args.receipt_uri,
            {
                "status": "succeeded",
                "run_id": args.run_id,
                "table": f"{args.namespace}.{args.table}",
                "started_at_utc": started_at,
                "finished_at_utc": utc_now(),
                "pre": before,
                "post": after,
                "rewrite_data_files": rewrite,
                "rewrite_manifests_result_count": len(manifests),
            },
        )
        print(f"compaction_succeeded receipt_uri={args.receipt_uri}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
