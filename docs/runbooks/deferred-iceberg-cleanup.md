# Deferred Iceberg snapshot and orphan cleanup

Status: planning only. This cleanup is not implemented, scheduled, or authorized to
run. The existing ad-hoc compaction job deliberately performs neither operation.

## Objective

Add a cleanup mode to the existing Dataproc Serverless maintenance program so the
table can release metadata and files retained after frequent loader commits and file
compaction. Reuse the maintenance service account, staging bucket, HadoopCatalog
configuration, snapshot fence, bounded Spark resources, and receipt format already
used by ad-hoc compaction.

## Operations

Run the operations in this order:

1. `expire_snapshots` removes snapshots older than the retention boundary while
   preserving a configured minimum number of recent snapshots. Files still referenced
   by any retained snapshot remain protected. Expired snapshots can no longer be used
   for time travel or rollback.
2. `remove_orphan_files` lists the table location and deletes sufficiently old files
   that are not referenced by any valid table metadata. This catches abandoned data,
   manifests, manifest lists, and metadata files that snapshot expiration cannot
   safely associate with the table.

Do not treat compaction as cleanup. `rewrite_data_files` and `rewrite_manifests`
optimize the current snapshot while older snapshots continue to reference historical
files.

## Proposed interface

Keep one reusable maintenance entry point with explicit modes rather than separate
infrastructure:

```text
compact  -> rewrite_data_files -> rewrite_manifests -> validate -> receipt
cleanup  -> expire_snapshots   -> remove_orphan_files -> validate -> receipt
```

Proposed cleanup configuration:

```bash
export CLEANUP_SNAPSHOT_RETENTION_DAYS=7
export CLEANUP_RETAIN_LAST_SNAPSHOTS=100
export CLEANUP_ORPHAN_MIN_AGE_DAYS=3
export CLEANUP_STREAM_RESULTS=true
```

All values must remain configurable. A preview/dry-run report must be available before
deletion, and execution must require an explicit confirmation flag.

## Safety requirements

- Use the same quiet-window preflight and job-start snapshot fence as compaction.
- Run after market hours or on an off day.
- Never expire the current snapshot or snapshots referenced by branches or tags.
- Retain both an age boundary and a minimum snapshot count.
- Never set orphan age below 24 hours; default to three days to protect in-flight or
  recently failed writes.
- Use streamed deletion results so thousands of paths are not collected in the Spark
  driver.
- Record the exact retention boundary, pre/post snapshot counts, and every Iceberg
  deletion counter in the receipt.
- Stop after snapshot expiration if its validation fails; do not continue to orphan
  deletion.

## Validation and receipt

The job should fail unless all of these checks pass:

- the current snapshot ID and total row count remain unchanged;
- the current table is readable after each operation;
- all retained snapshots satisfy the age/count policy;
- Iceberg reports the counts of deleted data files, delete files, manifests, manifest
  lists, and statistics files;
- orphan cleanup uses the configured minimum age and reports its deleted paths/count;
- a JSON receipt is written to the maintenance bucket and read back successfully.

After cleanup, start the VM pipeline and confirm one normal loader append succeeds.
The public dashboard should continue reading the latest metadata; only time travel to
expired snapshots is intentionally removed.

## Implementation checkpoints

1. Add a cleanup-only module and unit tests for retention calculations and command
   generation.
2. Add a preview mode that reads table history and reports eligible snapshots/files
   without deletion.
3. Run preview against the production table and review the proposed deletion counts.
4. Run one confirmed cleanup batch with conservative retention values.
5. Validate its receipt, current table row count, dashboard metadata read, and next
   loader append before considering any schedule.

No recurring schedule should be added until at least one manual cleanup has completed
and its storage reduction, runtime, and cost have been reviewed.

## Related operational follow-up

- [x] Enable `tansu.service` and `iceberg-loader.service` to start at VM boot.
- [x] Keep `alpaca-extractor.timer` enabled while leaving
  `alpaca-extractor.service` unscheduled directly.
- [ ] Perform a controlled VM stop/start validation. Confirm that Tansu becomes
  ready, `tansu-topic.service` succeeds through the loader dependency chain, the
  loader rejoins its Kafka consumer group at the committed offset, and the
  extractor remains governed by its market-open timer.

`tansu-topic.service` and `alpaca-extractor.service` are static units and do not
need to be enabled directly.

## References

- [Apache Iceberg maintenance](https://iceberg.apache.org/docs/nightly/maintenance/)
- [Apache Iceberg 1.9.2 Spark procedures](https://iceberg.apache.org/docs/1.9.2/spark-procedures/)
