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

- Add the minimum clean-slate code: HadoopCatalog adapter, Tansu SQLite setup, bounded synthetic producer, loader, and tests.
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
- Reference the legacy implementation directly from `feat/tansu-iceberg-pipeline` at `e55c880`; do not keep a gitignored legacy copy.
- Keep the old stack unchanged and paused until approved live validation of the new stack completes.
