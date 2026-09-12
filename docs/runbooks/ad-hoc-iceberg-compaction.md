# Ad-hoc Iceberg compaction

This runbook prepares one manually submitted Dataproc Serverless Spark batch to compact
`alpaca_candidate.bars_direct`. It does not create a persistent cluster and it does not
schedule itself.

The job rewrites small Parquet files and manifests. It never runs `expire_snapshots` or
`remove_orphan_files`.

## One-time preparation

Apply the reviewed Terraform maintenance resources first. They create a dedicated
maintenance service account and a seven-day-lifecycle staging/receipt bucket. They do
not start a Spark batch.

The person submitting a batch needs Dataproc batch-submission permission and permission
to act as `iceberg-maintenance`; the batch itself uses only that service account.

Set these non-secret values in the shell used for submission:

```bash
export GCP_PROJECT=project-66783f65-9c3e-4880-9a3
export DATAPROC_REGION=us-east1
export ICEBERG_WAREHOUSE=gs://YOUR_WAREHOUSE_BUCKET/warehouse
export ICEBERG_NAMESPACE=alpaca_candidate
export ICEBERG_TABLE=bars_direct
export COMPACTION_SERVICE_ACCOUNT=iceberg-maintenance@YOUR_PROJECT.iam.gserviceaccount.com
export COMPACTION_STAGING_BUCKET=gs://YOUR_MAINTENANCE_BUCKET
export COMPACTION_RECEIPT_DIRECTORY=gs://YOUR_MAINTENANCE_BUCKET/receipts/
export COMPACTION_RUNTIME_VERSION=2.3
```

## Normal quiet-window submission

Normal mode samples Iceberg's metadata version and current snapshot every minute for
15 minutes. Any table update aborts before a Spark batch is submitted.

```bash
export COMPACTION_QUIET_WINDOW_SECONDS=900
export COMPACTION_CHECK_INTERVAL_SECONDS=60

# Preflight only: no Dataproc batch is submitted.
uv run python scripts/submit_compaction.py

# Submit after a successful preflight.
uv run python scripts/submit_compaction.py --confirm
```

Both values are configurable. For example, 30 minutes sampled every two minutes:

```bash
COMPACTION_QUIET_WINDOW_SECONDS=1800 \
COMPACTION_CHECK_INTERVAL_SECONDS=120 \
uv run python scripts/submit_compaction.py --confirm
```

## Immediate mode

`--run-now` skips the wait but keeps two concurrency fences:

1. The submitter reads the current snapshot immediately before submission.
2. The Spark batch re-reads it before it rewrites data and aborts if it changed.

```bash
uv run python scripts/submit_compaction.py --run-now --confirm
```

Use this only after market close or on an off day. No `systemctl stop` is required;
the snapshot fence detects a loader append that occurs during the preflight interval.

## Cost controls

Defaults are intentionally bounded and can be changed per run:

```bash
export COMPACTION_TARGET_FILE_SIZE_BYTES=134217728  # 128 MiB
export COMPACTION_EXECUTOR_INSTANCES=2             # fixed upper cost shape
```

The submitter pins Dataproc runtime `2.3` and Iceberg runtime `1.9.2`, uses the
standard Serverless Spark tier, disables dynamic allocation, and limits the batch to
two executors. It also disables post-failure auto diagnostics so a failed ad-hoc run
releases its minimum 12-vCPU allocation promptly; driver output and Cloud Logging
remain available. The batch ends after one compaction and leaves no compute running.

## Success criteria

The batch succeeds only when it writes a receipt with all of the following:

- `pre.snapshot_id` differs from `post.snapshot_id`;
- `pre.total_records` equals `post.total_records`;
- `post.data_file_count` is lower than `pre.data_file_count`;
- Iceberg reports zero failed rewritten files.

Read the receipt after completion:

```bash
gcloud storage cat "gs://YOUR_MAINTENANCE_BUCKET/receipts/RUN_ID.json"
```

The optional VM `TableInspector` is not a compaction success requirement. The next
loader append is the operational integration check after reviewing the receipt.
