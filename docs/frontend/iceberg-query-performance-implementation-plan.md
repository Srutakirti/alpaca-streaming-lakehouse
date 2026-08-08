# Iceberg Query Performance Implementation Plan

This is the concrete plan to follow when implementing the first round of Iceberg
query performance improvements for the local dashboard.

The goal is to reduce expensive Iceberg work on the Monitoring page while still
showing both:

1. operational freshness from the loader, and
2. Iceberg table-derived freshness from metadata/file statistics.

No implementation has been started from this document yet.

## Goals

- Batch local appends to roughly 5 minutes to reduce snapshot and small-file
  churn.
- Stop using a full Iceberg row scan for Monitoring page data freshness.
- Add `latest_record_t` to loader metrics.
- Make `/api/pipeline/status` expose both operational latest record time and
  Iceberg-derived latest record time.
- Update Monitoring UI to show both freshness sources.
- Keep Iceberg as the durable data source while making health/freshness more
  metrics-driven.

## Non-goals for this first pass

- Do not migrate `t` from string to timestamp yet.
- Do not add Iceberg partition evolution yet.
- Do not introduce Postgres or another serving store yet.
- Do not rewrite existing data files.
- Do not remove Iceberg-backed chart reads.

## 1. Batch appends to 5 minutes

The loader currently flushes when either condition is true:

```text
records >= BATCH_SIZE
OR elapsed >= BATCH_INTERVAL and records > 0
```

For local FAKEPACA testing, one-row/minute appends create too many snapshots and
metadata files. Change the local runner defaults to prefer 5-minute flushing.

Proposed local defaults:

```bash
BATCH_INTERVAL=300
BATCH_SIZE=1000
```

Meaning:

- Low-volume streams flush about every 5 minutes.
- High-volume streams can still flush earlier if they hit `BATCH_SIZE`.
- Iceberg freshness may lag by up to 5 minutes, which is acceptable for the
  durable lake table.

Tradeoff:

| Benefit | Cost |
|---------|------|
| fewer snapshots | less immediate Iceberg visibility |
| fewer metadata files | Monitoring must distinguish loader freshness from table freshness |
| faster planning over time | UI bars may update less frequently from Iceberg |

## 2. Add `latest_record_t` to loader metrics

The loader already has every record in memory before append. During each flush,
it should compute the max `t` in the records that were successfully appended.

Proposed `_Metrics` additions:

```python
self.latest_record_t: str = ""
```

Proposed snapshot field:

```json
{
  "latest_record_t": "2026-07-13T15:00:00Z"
}
```

Update only after a successful append. If append fails, do not advance
`latest_record_t`.

Important ordering:

```text
decode Kafka message
buffer projected records
flush to Iceberg succeeds
commit Kafka offset succeeds
update last_commit_ts
update latest_record_t
emit metrics
```

The exact code may update `latest_record_t` immediately after append and before
offset commit, but the desired external meaning is: latest record known to have
landed in Iceberg and reached the successful loader path.

Because `t` is currently a UTC ISO string, `max(record["t"])` is valid as long as
all values remain in `YYYY-MM-DDTHH:MM:SSZ` or equivalent zero-padded UTC ISO
format.

## 3. Change `/api/pipeline/status` to use loader `latest_record_t`

Today `/api/pipeline/status` computes data freshness by scanning Iceberg:

```text
table.scan(selected_fields=("t",)).to_arrow()
max(t)
```

That should stop being the primary Monitoring source.

New status shape should distinguish sources explicitly:

```json
{
  "extractor": {...},
  "loader": {
    "latest_record_t": "2026-07-13T15:00:00Z",
    "last_commit_ts": "2026-07-13T15:00:02.087545+00:00"
  },
  "iceberg": {
    "snapshot_count": 123,
    "latest_snapshot_ts": 1783954861547,
    "latest_record_t": "2026-07-13T15:00:00Z",
    "latest_record_t_source": "file_stats"
  }
}
```

Monitoring should calculate operational data lag from:

```text
status.loader.latest_record_t
```

This answers:

```text
What is the latest bar timestamp the loader says it successfully landed?
```

Iceberg table-derived data lag should come from:

```text
status.iceberg.latest_record_t
```

This answers:

```text
What is the latest bar timestamp visible from the Iceberg table metadata?
```

If loader metrics are unavailable, the UI can fall back to Iceberg-derived
freshness, but it should label the source clearly.

### Metrics source portability

This must remain easy to switch between local development and GCP deployment.

The UI should not know whether loader metrics came from local log files or GCP
Cloud Logging. The stable contract is `/api/pipeline/status`, not the underlying
log source.

Expected behavior:

| Environment | Metrics source | UI/API contract |
|-------------|----------------|-----------------|
| Local laptop, `GCP_PROJECT_ID` unset | `.local-run/logs/loader.log` via `logging_client.py` fallback | `loader.latest_record_t` |
| GCP, `GCP_PROJECT_ID` set | Cloud Logging structured entries | `loader.latest_record_t` |

Implementation rule:

```text
loader emits the same metrics fields everywhere
frontend/app/logging_client.py normalizes local logs and Cloud Logging into the
same dict shape
Monitoring UI reads only /api/pipeline/status
```

So switching from local logs to GCP Cloud Logging should be an environment
configuration change, not a React/UI change.

## 4. Get Iceberg `max(t)` from file stats

Instead of scanning the `t` column, use Iceberg metadata/file statistics.

Conceptual logic:

```text
latest_record_t = max(data_file.upper_bounds["t"] for all live data files)
```

Why this should work:

- Iceberg manifests track data files.
- Data files usually include per-column lower and upper bounds.
- For append-only bars, the max file-level upper bound for `t` is the table max.
- Because `t` is currently a consistently formatted UTC ISO string, lexicographic
  max matches timestamp max.

Caveats:

- We need to confirm the exact PyIceberg APIs available in this repo version.
- Some files may not have usable bounds.
- `t` is a string today; timestamp type would be cleaner later.
- Deletes/upserts would require more care later. For the current append-only
  pipeline, file upper bounds are sufficient.

Fallback strategy:

```text
try file stats
if unavailable:
  return latest_record_t_error or source="unavailable"
do not silently scan full table on every status request
```

Avoiding the fallback scan is intentional. If file stats are unavailable, the UI
should show that table-derived latest record time is unavailable rather than
making the status endpoint expensive again.

## 5. Display both freshness values in Monitoring

The Monitoring UI should display both #3 and #5 from the earlier discussion:

### A. Operational Data Lag

Source:

```text
loader.latest_record_t
```

Suggested label:

```text
Loader Data Lag
```

Meaning:

```text
now - latest loader-confirmed record timestamp
```

This is the primary operational freshness signal.

### B. Iceberg Data Lag

Source:

```text
iceberg.latest_record_t
```

Suggested label:

```text
Iceberg Data Lag
```

Meaning:

```text
now - latest record timestamp derived from Iceberg file stats
```

This validates what the table metadata says is visible in Iceberg.

### Existing Iceberg Freshness remains separate

Keep the existing Iceberg snapshot freshness card:

```text
now - iceberg.latest_snapshot_ts
```

This is not the same as data lag. It answers:

```text
When did the Iceberg table last commit a snapshot?
```

After the change, Monitoring may have three related cards:

| Card | Source | Meaning |
|------|--------|---------|
| Loader Data Lag | loader metrics | latest bar timestamp successfully handled by loader |
| Iceberg Data Lag | Iceberg file stats | latest bar timestamp visible in Iceberg metadata |
| Iceberg Freshness | Iceberg snapshot metadata | latest snapshot commit time |

## 6. API compatibility notes

Current UI uses:

```text
iceberg.latest_record_t
```

After this change, avoid breaking all callers at once. Options:

1. Keep `iceberg.latest_record_t`, but change its implementation from row scan
   to file stats.
2. Add `loader.latest_record_t` and update the UI to prefer it for operational
   lag.
3. Add source fields:

```json
{
  "loader": {
    "latest_record_t": "...",
    "latest_record_t_source": "loader_metrics"
  },
  "iceberg": {
    "latest_record_t": "...",
    "latest_record_t_source": "file_stats"
  }
}
```

This keeps the payload readable when debugging.

## 7. Testing plan

Loader tests:

- `_Metrics.snapshot()` includes `latest_record_t`.
- Successful flush advances `latest_record_t`.
- Failed flush does not advance `latest_record_t`.
- Empty flush does not change `latest_record_t`.

Frontend API tests:

- `/api/pipeline/status` includes loader `latest_record_t` when local/GCP metrics
  provide it.
- Iceberg freshness no longer scans rows for `max(t)`.
- Iceberg file-stats helper returns latest `t` when bounds exist.
- If file stats are unavailable, API returns an explicit unavailable/error field
  instead of scanning all rows.

UI tests/manual checks:

- Monitoring shows Loader Data Lag.
- Monitoring shows Iceberg Data Lag.
- Existing Iceberg Freshness still shows snapshot recency.
- UTC display remains consistent.

E2E/manual:

- Start FAKEPACA stream.
- Confirm loader metrics include `latest_record_t`.
- Confirm `/api/pipeline/status` exposes both loader and Iceberg latest record
  timestamps.
- Confirm Iceberg snapshots are created every ~5 minutes under low-volume fake
  stream.

## 8. Implementation order

1. Change local run defaults to `BATCH_INTERVAL=300` and a larger `BATCH_SIZE`.
2. Add `latest_record_t` to loader metrics.
3. Update tests for loader metrics.
4. Update `/api/pipeline/status` to expose loader latest record time.
5. Implement Iceberg file-stats latest-record helper.
6. Remove the full table `max(t)` row scan from the status path.
7. Update Monitoring UI to show Loader Data Lag and Iceberg Data Lag.
8. Update docs:
   - `docs/frontend/monitoring-datapoints.md`
   - `docs/frontend/iceberg-query-performance.md`
9. Run unit tests, frontend build, and local FAKEPACA smoke test.

## Timestamp and partitioning implementation note

PyIceberg 0.11.1 supports creating a `timestamptz` field and a partition spec
like:

```text
day(t), identity(S)
```

However, local append testing showed that writing timestamp data requires the
optional `pyiceberg-core` extra:

```text
pyiceberg_core needs to be installed. pip install "pyiceberg[pyiceberg-core]"
```

So the code path is gated behind:

```bash
ICEBERG_T_TYPE=timestamp
```

Until the runtime dependency is added and verified, the default remains the
existing string `t` table for compatibility with current local warehouses.

Expected migration path:

1. Add/install `pyiceberg[pyiceberg-core]` in the loader runtime.
2. Create a new table, for example `alpaca.bars_v2`, with `ICEBERG_T_TYPE=timestamp`.
3. Verify appends create `timestamptz` data and `day(t), S` partitions.
4. Backfill old string-`t` data into the new table with timestamp conversion.
5. Point frontend and loader at the new table.
6. Retire the old table after validation.

## Open decisions before implementation

- Should local `BATCH_SIZE` be `1000`, `500`, or keep existing `10` with only
  `BATCH_INTERVAL=300`?
- Should the UI card names be `Loader Data Lag` / `Iceberg Data Lag`, or
  `Operational Data Lag` / `Table Data Lag`?
- If file stats are unavailable, should the UI hide Iceberg Data Lag or show
  `Unavailable`?
- Should `/api/pipeline/status` include a debug field such as
  `iceberg.latest_record_t_source`?
