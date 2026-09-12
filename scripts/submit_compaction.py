#!/usr/bin/env python3
"""Preflight and submit one guarded, ad-hoc Dataproc Iceberg compaction batch."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Callable


IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,1023}$")
PROJECT = re.compile(r"^[a-z][a-z0-9-]{4,61}[a-z0-9]$")
GCS_DIRECTORY = re.compile(r"^gs://[a-z0-9._-]+(?:/[A-Za-z0-9._/-]+)?/$")


@dataclass(frozen=True)
class Snapshot:
    metadata_version: str
    snapshot_id: str


@dataclass(frozen=True)
class Settings:
    project: str
    region: str
    warehouse: str
    namespace: str
    table: str
    service_account: str
    staging_bucket: str
    receipt_directory: str
    quiet_window_seconds: int
    check_interval_seconds: int
    target_file_size_bytes: int
    executor_instances: int
    runtime_version: str
    iceberg_runtime_package: str

    @classmethod
    def from_environment(cls) -> "Settings":
        def required(name: str) -> str:
            value = os.environ.get(name, "").strip()
            if not value:
                raise ValueError(f"{name} is required")
            return value

        def integer(name: str, default: int, minimum: int) -> int:
            value = int(os.environ.get(name, str(default)))
            if value < minimum:
                raise ValueError(f"{name} must be at least {minimum}")
            return value

        settings = cls(
            project=required("GCP_PROJECT"),
            region=os.environ.get("DATAPROC_REGION", "us-east1"),
            warehouse=required("ICEBERG_WAREHOUSE").rstrip("/"),
            namespace=required("ICEBERG_NAMESPACE"),
            table=required("ICEBERG_TABLE"),
            service_account=required("COMPACTION_SERVICE_ACCOUNT"),
            staging_bucket=required("COMPACTION_STAGING_BUCKET").rstrip("/"),
            receipt_directory=required("COMPACTION_RECEIPT_DIRECTORY"),
            quiet_window_seconds=integer("COMPACTION_QUIET_WINDOW_SECONDS", 900, 0),
            check_interval_seconds=integer("COMPACTION_CHECK_INTERVAL_SECONDS", 60, 1),
            target_file_size_bytes=integer("COMPACTION_TARGET_FILE_SIZE_BYTES", 134_217_728, 8 * 1024 * 1024),
            executor_instances=integer("COMPACTION_EXECUTOR_INSTANCES", 2, 2),
            runtime_version=os.environ.get("COMPACTION_RUNTIME_VERSION", "2.3").strip(),
            iceberg_runtime_package=os.environ.get(
                "COMPACTION_ICEBERG_RUNTIME_PACKAGE", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2"
            ),
        )
        if not PROJECT.fullmatch(settings.project):
            raise ValueError("GCP_PROJECT is invalid")
        if not GCS_DIRECTORY.fullmatch(f"{settings.warehouse}/"):
            raise ValueError("ICEBERG_WAREHOUSE must be a gs:// directory")
        if not GCS_DIRECTORY.fullmatch(settings.receipt_directory):
            raise ValueError("COMPACTION_RECEIPT_DIRECTORY must be a gs:// directory ending in /")
        if not GCS_DIRECTORY.fullmatch(f"{settings.staging_bucket}/"):
            raise ValueError("COMPACTION_STAGING_BUCKET must be a gs:// bucket or directory")
        if not re.fullmatch(r"[0-9]+\.[0-9]+", settings.runtime_version):
            raise ValueError("COMPACTION_RUNTIME_VERSION must be a major.minor version, such as 2.3")
        for name, value in (("ICEBERG_NAMESPACE", settings.namespace), ("ICEBERG_TABLE", settings.table)):
            if not IDENTIFIER.fullmatch(value):
                raise ValueError(f"{name} is invalid")
        return settings


def run(command: list[str]) -> str:
    return subprocess.run(command, check=True, capture_output=True, text=True).stdout


def read_snapshot(settings: Settings, command: Callable[[list[str]], str] = run) -> Snapshot:
    metadata = f"{settings.warehouse}/{settings.namespace}/{settings.table}/metadata"
    version = command(["gcloud", "storage", "cat", f"{metadata}/version-hint.text"]).strip()
    if not re.fullmatch(r"[1-9][0-9]*", version):
        raise RuntimeError("Iceberg version-hint.text is invalid")
    document = json.loads(command(["gcloud", "storage", "cat", f"{metadata}/v{version}.metadata.json"]))
    snapshot_id = document.get("current-snapshot-id")
    if snapshot_id is None:
        raise RuntimeError("Iceberg table has no current snapshot")
    return Snapshot(metadata_version=version, snapshot_id=str(snapshot_id))


def require_quiet_snapshot(
    settings: Settings,
    run_now: bool,
    read: Callable[[], Snapshot],
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> Snapshot:
    expected = read()
    if run_now:
        return expected
    deadline = clock() + settings.quiet_window_seconds
    while clock() < deadline:
        sleep(min(settings.check_interval_seconds, max(0, deadline - clock())))
        observed = read()
        if observed != expected:
            raise RuntimeError(
                "table changed during quiet window: "
                f"metadata v{expected.metadata_version}/snapshot {expected.snapshot_id} -> "
                f"v{observed.metadata_version}/snapshot {observed.snapshot_id}"
            )
    return expected


def batch_command(settings: Settings, expected: Snapshot, run_id: str) -> list[str]:
    properties = "^#^" + "#".join(
        [
            "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.hadoop=org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.hadoop.type=hadoop",
            f"spark.sql.catalog.hadoop.warehouse={settings.warehouse}",
            f"spark.jars.packages={settings.iceberg_runtime_package}",
            "spark.dynamicAllocation.enabled=false",
            f"spark.executor.instances={settings.executor_instances}",
            "dataproc.diagnostics.enabled=false",
        ]
    )
    receipt_uri = f"{settings.receipt_directory.rstrip('/')}/{run_id}.json"
    staging_bucket_name = settings.staging_bucket.removeprefix("gs://").rstrip("/")
    return [
        "gcloud", "dataproc", "batches", "submit", "pyspark", "iceberg-maintenance/compact_table.py",
        f"--project={settings.project}", f"--region={settings.region}", f"--batch={run_id}",
        f"--version={settings.runtime_version}",
        f"--service-account={settings.service_account}",
        # Dataproc uploads the local PySpark file and resolved dependencies to the deps bucket.
        # Passing only --staging-bucket leaves that upload location unset in the gcloud CLI.
        f"--deps-bucket={staging_bucket_name}", f"--staging-bucket={staging_bucket_name}",
        f"--properties={properties}", "--",
        "--namespace", settings.namespace, "--table", settings.table,
        "--expected-snapshot-id", expected.snapshot_id,
        "--target-file-size-bytes", str(settings.target_file_size_bytes),
        "--receipt-uri", receipt_uri, "--run-id", run_id,
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-now", action="store_true", help="Skip the quiet wait but retain both snapshot fences.")
    parser.add_argument("--confirm", action="store_true", help="Actually submit the Dataproc batch after preflight.")
    args = parser.parse_args()
    settings = Settings.from_environment()
    expected = require_quiet_snapshot(settings, args.run_now, lambda: read_snapshot(settings))
    mode = "run_now" if args.run_now else f"quiet_{settings.quiet_window_seconds}s"
    print(f"preflight_passed mode={mode} metadata_version={expected.metadata_version} snapshot_id={expected.snapshot_id}")
    if not args.confirm:
        print("dry_run=passed add --confirm to submit the batch")
        return
    run_id = f"iceberg-compact-{datetime.now(UTC):%Y%m%dt%H%M%Sz}-{uuid.uuid4().hex[:8]}"
    command = batch_command(settings, expected, run_id)
    print(f"submitting_batch={run_id}")
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
