from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "submit_compaction.py"
SPEC = importlib.util.spec_from_file_location("submit_compaction", SCRIPT)
assert SPEC and SPEC.loader
submit_compaction = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = submit_compaction
SPEC.loader.exec_module(submit_compaction)


def settings() -> object:
    return submit_compaction.Settings(
        project="example-project",
        region="us-east1",
        warehouse="gs://example-warehouse/warehouse",
        namespace="alpaca_candidate",
        table="bars_direct",
        service_account="iceberg-maintenance@example-project.iam.gserviceaccount.com",
        staging_bucket="gs://example-staging",
        receipt_directory="gs://example-staging/receipts/",
        quiet_window_seconds=120,
        check_interval_seconds=60,
        target_file_size_bytes=134_217_728,
        executor_instances=2,
        disk_size_gib=250,
        runtime_version="2.3",
        iceberg_runtime_package="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2",
    )


def test_run_now_reads_one_snapshot_without_waiting() -> None:
    calls = []

    result = submit_compaction.require_quiet_snapshot(
        settings(), True, lambda: calls.append(1) or submit_compaction.Snapshot("42", "123")
    )

    assert result == submit_compaction.Snapshot("42", "123")
    assert calls == [1]


def test_quiet_window_rejects_changed_metadata_or_snapshot() -> None:
    snapshots = iter(
        [submit_compaction.Snapshot("42", "123"), submit_compaction.Snapshot("43", "124")]
    )
    moments = iter([0.0, 0.0, 60.0])

    try:
        submit_compaction.require_quiet_snapshot(
            settings(), False, lambda: next(snapshots), sleep=lambda _seconds: None, clock=lambda: next(moments)
        )
    except RuntimeError as error:
        assert "table changed during quiet window" in str(error)
    else:
        raise AssertionError("changed snapshot was accepted")


def test_batch_command_keeps_expected_snapshot_and_fixed_executor_cap() -> None:
    command = submit_compaction.batch_command(settings(), submit_compaction.Snapshot("42", "123"), "run-1")

    assert "--deps-bucket=example-staging" in command
    assert "--staging-bucket=example-staging" in command
    assert "--version=2.3" in command
    assert "--expected-snapshot-id" in command
    assert command[command.index("--expected-snapshot-id") + 1] == "123"
    properties = next(value for value in command if value.startswith("--properties="))
    assert "spark.executor.instances=2" in properties
    assert "spark.dynamicAllocation.enabled=false" in properties
    assert "spark.dataproc.driver.disk.size=250g" in properties
    assert "spark.dataproc.executor.disk.size=250g" in properties
    assert "dataproc.diagnostics.enabled=false" in properties
