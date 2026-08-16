# Clean-Slate GCE Hadoop Pipeline

## Operating rule

Every checkpoint follows this cycle:

```text
Implement one checkpoint
-> run its agreed validation
-> commit the checkpoint
-> report evidence
-> wait for explicit approval
-> begin the next checkpoint
```

Work does not proceed automatically past a checkpoint.

## Checkpoints

### Checkpoint 0: Plan-only commit

- Create the orphan branch `architecture/gce-hadoop-catalog`.
- Add this root-level plan file and make one documentation commit.
- Wait for validation before any implementation work.

### Checkpoint 1: Local synthetic foundation

- Add the minimum clean-slate code: Tansu SQLite setup, an Alpaca WebSocket-compatible bounded synthetic producer, and a long-lived Java Iceberg-core loader.
- The loader uses Java `HadoopCatalog` directly; Spark is a read-only validation client, never the loader runtime.
- Flush one batch when either five minutes elapse or the configured record threshold is reached.
- Protect the single VM writer with an OS `flock`; Cloud Run and all other components are read-only catalog clients.
- Validate locally, commit, and wait for approval.

### Checkpoint 2: Isolated cloud infrastructure

- Create a new e2-micro VM, GCS warehouse, IAM, and private scale-to-zero UI.
- Do not use Cloud SQL or modify existing resources.
- Validate, commit, and wait for approval.

### Checkpoint 3: Synthetic cloud acceptance

- Run synthetic data through VM Tansu SQLite, loader, GCS HadoopCatalog, and private UI.
- Validate UTC behavior, five-minute commits, restart recovery, Spark reads, and maintenance.
- Commit the evidence and wait for approval.

### Checkpoint 4: Limited real-data smoke test

- Enable only a small Alpaca symbol set after approval.
- Validate one market session, commit, and wait for approval.

### Checkpoint 5: Production activation

- Enable `*` and the weekday schedule after approval.
- Keep the UI private and scaled to zero.

## Standards

- UTC is universal for data, logs, APIs, UI, filenames, and operational output.
- The market timer alone uses `America/New_York` for daylight-saving alignment; its emitted events remain UTC.
- Kafka frames use the Alpaca bar WebSocket contract (`T`, `S`, `o`, `h`, `l`, `c`, `v`, `t`, `n`, `vw`).
- The raw Iceberg table is append-only; it retains Kafka replays with a deterministic `payload_hash` so read views can deduplicate without discarding source deliveries.
- The HadoopCatalog warehouse has exactly one writer: the long-lived loader on the VM. `flock` prevents duplicate local loader processes; all Cloud Run identities are read-only.
- Reference the legacy implementation directly from `feat/tansu-iceberg-pipeline` at `e55c880`; do not keep a gitignored legacy copy.
- Keep the old stack unchanged and paused until approved live validation of the new stack completes.
