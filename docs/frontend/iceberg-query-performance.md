# Iceberg Query Performance Notes

This note tracks ideas for speeding up dashboard reads from the local Iceberg
warehouse and the future production table.

## Current situation

The frontend API reads Iceberg directly for:

- `/api/symbols`
- `/api/bars`
- the Iceberg freshness part of `/api/pipeline/status`

That is useful because Iceberg is the source of truth for landed market data,
but it can become slow when the table has many snapshots, many small files, or
when status endpoints scan data repeatedly.

## Biggest current bottlenecks

### 1. Many small appends

The local FAKEPACA stream can append one row per minute. Each successful append
creates a new Iceberg snapshot and metadata files. Over time this makes
planning slower even when each query only returns a few rows.

Mitigations:

- Increase `BATCH_SIZE`.
- Increase `BATCH_INTERVAL`.
- Avoid one-row flushes when low latency is not required.
- Periodically expire old snapshots.

Example local settings:

```bash
BATCH_SIZE=100
BATCH_INTERVAL=60
```

### 2. Monitoring scans `max(t)`

The monitoring freshness code currently scans the table's `t` column to compute
latest data time:

```text
max(t)
```

This is fine for small local testing, but it is not a good long-term monitoring
path. The dashboard should not scan the full Iceberg table every few seconds.

Better options:

- Emit `latest_record_t` from the loader metrics after each successful append.
- Maintain a small `pipeline_status` table or state file.
- Read Iceberg manifest upper bounds instead of scanning row data.

Preferred near-term fix: add `latest_record_t` to loader metrics and let
`/api/pipeline/status` use that for data freshness.

### 3. `t` is stored as a string

The values in `t` look like timestamps:

```text
2026-07-13T15:00:00Z
```

but the current schema stores `t` as a string:

```python
pa.field("t", pa.string())
NestedField(8, "t", StringType(), required=False)
```

String filtering works only because the values are consistently formatted as
UTC ISO-8601 strings. Lexicographic order matches time order for this format.

However, a string column is weaker than a real timestamp column for:

- timestamp-aware partition transforms,
- planner statistics,
- type validation,
- future SQL engines,
- avoiding subtle bugs if timestamp formatting ever varies.

Long-term target: store `t` as a real Iceberg timestamp/timestamptz field.
In PyIceberg 0.11.1 this append path requires the optional `pyiceberg-core`
runtime extra, so the timestamp-table path is gated behind
`ICEBERG_T_TYPE=timestamp` until that dependency is installed and validated.

## Query improvements

### Partition the table

The dashboard queries usually filter by symbol and time range:

```sql
WHERE S = 'FAKEPACA'
  AND t >= '2026-07-13T14:00:00Z'
  AND t <= '2026-07-13T23:59:59Z'
```

The table should eventually be partitioned for that shape.

Likely target:

```text
partition by day(t), S
```

If `t` remains a string, day partitioning is awkward and engine-dependent. This
is another reason to migrate `t` to a timestamp type first.

### Sort records before append

Before writing a batch, sort by:

```text
S, t
```

This can improve file-level min/max statistics and data skipping. It matters
more when batches contain multiple symbols and many rows.

Example unsorted file:

```text
AAPL      10:00
TSLA      10:01
AAPL      11:00
FAKEPACA  10:05
TSLA      11:10
```

File-level stats for that file are broad:

```text
S min=AAPL, max=TSLA
t min=10:00, max=11:10
```

For this query:

```sql
WHERE S = 'FAKEPACA'
  AND t >= '10:00'
  AND t <= '10:30'
```

the engine cannot skip the file from stats alone because `FAKEPACA` may be
inside the broad `AAPL..TSLA` symbol range.

If records are sorted by `S, t`, files are more likely to have tight bounds:

```text
FAKEPACA  10:05
FAKEPACA  10:10
FAKEPACA  10:30
FAKEPACA  11:15
```

Stats become:

```text
S min=FAKEPACA, max=FAKEPACA
t min=10:05, max=11:15
```

Files for `AAPL` or `TSLA` can be skipped before Parquet row data is read.
Sorting helps most after batching is large enough to create files with many rows;
it helps very little when each append writes only one or two rows.

### Compact metadata and expire snapshots

Snapshot expiration reduces old metadata that the planner has to consider.

Existing script:

```bash
ICEBERG_CATALOG_URI=sqlite:///./warehouse/catalog.db \
ICEBERG_WAREHOUSE=./warehouse \
uv run --with duckdb --package load python scripts/compact_iceberg.py --keep 20
```

This expires old snapshots. It does not rewrite small data files into larger
files. A future compaction job should also rewrite small files when the table
has many tiny Parquet files.

### Cache frontend API reads

The dashboard does not need every UI request to hit Iceberg directly.

Useful caches:

- `/api/symbols`: cache for 60 seconds or longer.
- `/api/bars`: cache by `symbol/from/to/limit` for 15-60 seconds.
- `/api/pipeline/status`: avoid any Iceberg row scan; use metrics for live
  state and Iceberg metadata only for snapshot count/freshness.

### Add downsampling for broad chart windows

The visualization page currently asks for up to `5000` rows. For wide windows,
the API could return coarser candles:

- 1-minute bars for short windows.
- 5-minute bars for intraday multi-hour windows.
- 15-minute or 1-hour bars for multi-day windows.

This keeps the chart responsive and reduces data transfer.

### Consider a read-optimized serving layer

Iceberg should remain the durable source of truth, but the UI could read recent
data from a smaller serving store:

- DuckDB file for local demo reads.
- SQLite table for recent bars.
- Postgres table/materialized view for dashboard reads.
- In-memory cache for the latest N bars per symbol.

This is useful if the dashboard needs low-latency refresh while Iceberg remains
optimized for analytical durability.

## Suggested order of work

1. Emit `latest_record_t` from the loader after each append.
2. Stop scanning `max(t)` in `/api/pipeline/status`.
3. Increase local batch settings to reduce one-row snapshots.
4. Run snapshot expiration periodically during local testing.
5. Add server-side cache for `/api/bars`.
6. Migrate `t` from string to timestamp.
7. Add day/symbol partitioning.
8. Add small-file rewrite compaction if the table accumulates many tiny files.
9. Add downsampling for wide visualization windows.

## Open questions

- Should the dashboard API continue using PyIceberg directly, or should it use
  DuckDB for local analytical reads?
- Should recent bars be served from a fast cache while historical bars stay in
  Iceberg?
- What freshness latency is acceptable for the visualization page: seconds,
  tens of seconds, or one minute?
- When migrating `t`, should we create a new table or evolve the existing table
  with a new timestamp column and backfill?
