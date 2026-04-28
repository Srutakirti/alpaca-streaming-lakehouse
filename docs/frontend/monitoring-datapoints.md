# Pipeline Monitoring — Datapoint Reference

Reference for every card and chart on the **Monitoring** page of the dashboard
(`frontend/web/src/pages/Monitoring.tsx`). For each datapoint: what it shows,
where the value comes from, when it changes, what the colors mean, and how to
interpret unusual values.

The page polls two backend endpoints on a fixed cadence:

| Endpoint                              | Cadence | Source code                                  |
|---------------------------------------|---------|----------------------------------------------|
| `GET /api/pipeline/status`            | 5 s     | `frontend/app/routes/pipeline.py:35`         |
| `GET /api/pipeline/metrics?...`       | 30 s    | `frontend/app/routes/pipeline.py:47`         |

### Timezone summary (read this before debugging time-related issues)

Every persisted timestamp in this pipeline is **UTC**, and every clock-time on
this page is also rendered as **UTC**. There is no local-timezone display
anywhere — Last Commit and the chart x-axis read from the same instants in the
same timezone, so they match digit-for-digit.

| Field on the page              | Stored as           | Displayed in                                |
|-------------------------------|---------------------|---------------------------------------------|
| Iceberg Freshness (`Xm ago`)  | UTC epoch ms        | Duration only — TZ doesn't apply            |
| Data Lag (`Xm`)               | UTC ISO `…Z` string | Duration only — TZ doesn't apply            |
| Data Lag sub `latest t=…`     | UTC ISO `…Z` string | **UTC**, displayed verbatim with `Z`        |
| Last Commit (HH:MM:SS UTC)    | UTC ISO `+00:00`    | **UTC**, formatted via `getUTC*` helpers    |
| Last Commit sub (date)        | UTC ISO `+00:00`    | **UTC**, sliced from string                 |
| Consumer-Lag chart x-axis     | UTC epoch s         | **UTC**, via lightweight-charts `timeFormatter` |

The backend's `/status` payload has three top-level keys:
- `extractor` — most recent metrics-line log entry from the extractor (or `null` if Cloud Logging access is disabled / no logs).
- `loader` — same for the loader.
- `iceberg` — derived live from the Iceberg table metadata + a column scan (`_iceberg_freshness()` in `pipeline.py:9`).

---

## 1. Extractor

**What:** Whether the extractor is producing to Kafka, plus its lifetime
"messages sent" counter.

**Value:**
- `Connected` — the most recent metrics log line was found.
- `No GCP logs` — `status.extractor === null`. Either Cloud Logging is unreachable, the env var `GCP_PROJECT_ID` isn't set on the frontend service, or the extractor hasn't logged anything within the lookback window.
- `...` — initial fetch in flight.

**Sub-line:** `<N> msgs sent` — value of `messages_sent` from the extractor's
metrics log line. Each "message" is one Kafka `produce()` call, which for the
real Alpaca feed is one WebSocket frame containing **a JSON array of bars** (see
`extract/extractor.py`). For the synthetic generator each call is also a JSON
array. So **this is a count of frames, not bar rows** — the loader expands each
frame into multiple Iceberg rows.

**Color:**
- Green when connected, muted otherwise. There is no per-value threshold; this
  card is a liveness signal, not a rate gauge.

**Reading it:**
- `messages_sent` should be monotonically increasing while the extractor is
  running. If it stays flat with `Connected`, the extractor is alive but not
  producing — usually a Kafka delivery failure (check the extractor's
  `delivery_failures` counter in Cloud Logging) or the WebSocket disconnected
  but the metrics task is still ticking.

---

## 2. Consumer Lag

**What:** Number of Kafka messages between the loader's committed offset and
the partition high-water mark. This is the single most important loader health
signal.

**Value:** `loader.consumer_lag` from the loader's metrics log line.
- Numeric (≥ 0) — actual lag.
- `N/A` — the loader logged `consumer_lag = -1`, meaning the broker's
  `ListOffsetsRequest` returned `OFFSET_INVALID` or timed out (Tansu has been
  observed to do this under load — see incident notes). The loader is alive
  but cannot compute lag right now.
- `No GCP logs` — `status.loader === null` (same condition as the extractor).

**Sub-line:** `<N> batches flushed` — `loader.batches_flushed`, lifetime
counter of successful `iceberg_table.append()` calls since the loader started.

**Color thresholds:**
| Value     | Color  |
|-----------|--------|
| 0         | green  |
| 1–99      | yellow |
| ≥ 100     | red    |
| `-1`/null | muted  |

**Reading it:**
- A steadily climbing number means the loader can't keep up with the producer.
  Common cause: each `iceberg_table.append()` to GCS takes 20–30 s, capping the
  loader at ~200 records/min per BATCH_SIZE=100. Bumping `BATCH_SIZE`
  amortizes the per-flush cost.
- A persistent `N/A` is itself a problem — you've lost observability. Restart
  the loader if it lasts more than a few metric ticks.

---

## 3. Last Batch

**What:** Size and duration of the most recent successful flush to Iceberg.

**Value:** `loader.last_flush_records` rendered as `<N> rows` (or `—` if 0 /
no loader logs).

**Sub-line:** `loader.last_flush_duration_ms` rendered as `<N> ms`. This is
wall-clock time spent inside `flush()` in `load/subscriber.py` — Arrow table
construction + `iceberg_table.append()` + manifest/metadata write to GCS. GCS
dominates (typically 20–30 s).

**Color:** green if rows > 0 in the most recent flush, muted otherwise.

**Reading it:**
- `last_flush_records` always equals `BATCH_SIZE` when the loader is keeping
  up with a steady producer. Smaller values mean the loader hit the
  `BATCH_INTERVAL` time-based flush before filling a batch, i.e. it's
  outpacing the producer (good).
- `last_flush_duration_ms` rising over time often indicates the metadata
  history is growing and reads/writes are getting slower. `scripts/compact_iceberg.py`
  helps.

---

## 4. Iceberg Freshness

**What:** How long ago the loader committed a new snapshot to the Iceberg
table — the "table-side" view of staleness, independent of bar timestamps.

**Value:** `Date.now() - iceberg.latest_snapshot_ts`, formatted as
`Xs ago` / `Xm ago` / `Xh ago`.

**Timezone:** The card displays a *duration*, so timezones don't apply directly.
Internally `iceberg.latest_snapshot_ts` is **Unix epoch milliseconds UTC**
(PyIceberg's `Snapshot.timestamp_ms`). `Date.now()` is also UTC epoch
milliseconds, so the subtraction is timezone-safe regardless of where the
viewer is.

`iceberg.latest_snapshot_ts` is computed in `_iceberg_freshness()` from
`max(table.metadata.snapshots, key=timestamp_ms)`. It equals the wall-clock
time the snapshot was *committed* by `iceberg_table.append()`, not when its
underlying bar data arrived.

**Sub-line:** `<N> snapshots` — `iceberg.snapshot_count`, total snapshots in
the Iceberg metadata. Increments by one per successful flush.

**Color:**
- Green if the latest snapshot is < 2 minutes old.
- Yellow otherwise (and when no snapshot yet exists).

**Reading it:**
- A green Freshness with a red Consumer Lag means: the loader is committing
  fine, but the producer is even faster — back-pressure is on Kafka, not on
  the writer.
- A red Freshness with a small/`N/A` Consumer Lag means: there's nothing to
  consume (extractor stopped, weekends, market closed) — that's expected.

---

## 5. Data Lag (now − max t)

**What:** Difference between wall-clock time and the latest **bar timestamp**
already in the Iceberg table. End-to-end freshness from the *data's* point of
view (vs. Iceberg Freshness, which is the table's point of view).

**Value:** `Date.now() - parse(iceberg.latest_record_t)`, formatted as
`Xs / Xm / Xh`. A leading `-` means the latest bar timestamp is in the
**future** — only happens when the synthetic generator is producing
fast-forwarded simulated bar times.

**Timezone:** The bar timestamp `iceberg.latest_record_t` is an ISO-8601
string **in UTC** (always ends in `Z`, e.g. `2026-04-28T07:23:25Z`) — that's
the format Alpaca emits and the synthetic generator preserves it. JavaScript
parses `Z`-suffixed strings as UTC, and `Date.now()` is UTC epoch ms, so the
duration calculation is timezone-safe. The displayed value is a duration
(seconds / minutes / hours), not a clock time.

`iceberg.latest_record_t` is computed by `_iceberg_freshness()` as
`pyarrow.compute.max(arrow.column("t"))` over the entire `t` column scan. ⚠️ This
scans every row on every status request — fine for small tables but expensive
on large warehouses (there's a TODO in the code to switch to manifest
upper-bounds).

**Sub-line:** `latest t=<ISO timestamp>` (preferred) — shown verbatim, so it
appears as a UTC timestamp ending in `Z`. Or `<N> rows` if the table is empty.
Comes from `iceberg.row_count` (also from the column scan).

**Color:**
| Condition                 | Color  |
|---------------------------|--------|
| lag is negative (future)  | yellow |
| 0–119 s                   | green  |
| 120–599 s                 | yellow |
| ≥ 600 s                   | red    |

**Reading it:**
- Real Alpaca minute bars produce a ~60 s steady-state Data Lag (you only
  receive a bar after its minute closes).
- A growing Data Lag with healthy Consumer Lag and Iceberg Freshness means:
  the source itself stopped. Check the extractor card.
- A `-Xh` value plus the synthetic generator running is normal — the
  synthetic generator emits simulated future timestamps deliberately.

---

## 6. Append Errors

**What:** Lifetime count of `iceberg_table.append()` failures by the current
loader process.

**Value:** `loader.iceberg_append_errors` (defaults to 0 if absent).

**Color:** green at 0, red otherwise.

**Reading it:**
- Should always be 0. Any non-zero value is worth investigating — typical
  causes: GCS permission revoked, catalog/warehouse drift (see
  `docs/runbooks/loader-catalog-warehouse-drift.md`), or schema mismatch
  between the producer's payload and the Iceberg table.
- The counter resets when the loader process restarts — Cloud Run restarts
  silently zero this without surfacing why. Cross-check with Cloud Logging
  for the actual exceptions.

---

## 7. Last Commit

**What:** Wall-clock timestamp of the loader's most recent successful
`consumer.commit(asynchronous=False)` — i.e. the moment Kafka offsets were
advanced after a successful Iceberg flush. This is the at-least-once "I've
durably persisted up to here" mark.

**Value:** `loader.last_commit_ts`.

**Timezone:** **UTC, explicitly labeled.** The loader logs this as a Python
`datetime.now(timezone.utc).isoformat()` (e.g.
`2026-04-28T07:22:51.989996+00:00`). The card formats the time part via
`formatUtcTime` (`frontend/web/src/lib/time.ts`) which derives every component
through `Date.prototype.getUTC*` and appends a literal ` UTC` suffix, so the
viewer's local timezone never affects the rendered string. The date sub-line
is sliced from the same UTC string. If you're in IST and the card shows
`07:22:51 UTC`, that means `12:52:51` your local time.

**Color:** muted (informational, no thresholding).

**Reading it:**
- Compare to your local clock: if Last Commit is older than a few minutes
  while the producer is healthy, the loader has stalled mid-flush.
- A moving Last Commit with rising Consumer Lag means the loader is making
  progress but slower than the producer.

---

## 8. Consumer Lag (last 60 min) — chart

**What:** Time series of the loader's `consumer_lag` value over the past 60
minutes.

**Source:** `GET /api/pipeline/metrics?component=loader&minutes=60`,
implemented by `logging_client.get_metrics_timeseries`. Each point is one
metrics log line from the loader within the lookback window.

**Timezone:** Each point's time is `Math.floor(new Date(d._ts).getTime() / 1000)`
where `d._ts` is the **UTC ISO-8601** timestamp Cloud Logging attached to the
log entry. Lightweight-charts is fed UTC epoch seconds. The x-axis labels and
crosshair tooltip are rendered as **UTC** via
`localization.timeFormatter` and `timeScale.tickMarkFormatter` (both delegate
to `formatUtcChartLabel` / `formatUtcChartTick` in
`frontend/web/src/lib/time.ts`). Last Commit and the chart's right-edge
timestamp will match digit-for-digit when they refer to the same instant.

**Filtering:** points with `consumer_lag` undefined or `< 0` are dropped
(`Monitoring.tsx:70`), so the chart only shows the periods when the broker's
`ListOffsetsRequest` returned a real value.

**Reading it:**
- Sawtooth around zero = healthy: lag accumulates between flushes and resets
  on commit.
- Monotonically increasing line = loader can't keep up with producer. Increase
  `BATCH_SIZE`, scale the producer down, or both.
- Long flat-zero stretches = no producer activity (extractor offline, market
  closed).
- Gaps in the chart = the broker was returning `-1` for that period (lag
  unavailable, not lag = 0).

---

## Where the source values come from

| UI field                          | Backend source                                            | Notes                                       |
|-----------------------------------|-----------------------------------------------------------|---------------------------------------------|
| `extractor.messages_sent`         | Cloud Logging — extractor metrics line                    | Lifetime per-process counter                |
| `loader.consumer_lag`             | Cloud Logging — loader metrics line                       | `-1` when broker offset query fails         |
| `loader.last_flush_records`       | Cloud Logging — loader metrics line                       | Per-flush snapshot                          |
| `loader.last_flush_duration_ms`   | Cloud Logging — loader metrics line                       | Includes GCS write time                     |
| `loader.batches_flushed`          | Cloud Logging — loader metrics line                       | Lifetime counter                            |
| `loader.iceberg_append_errors`    | Cloud Logging — loader metrics line                       | Lifetime counter, resets on restart         |
| `loader.last_commit_ts`           | Cloud Logging — loader metrics line                       | UTC ISO-8601 string                         |
| `iceberg.snapshot_count`          | `len(table.metadata.snapshots)`                           | Live read on each /status call              |
| `iceberg.latest_snapshot_ts`      | `max(snapshots).timestamp_ms`                             | Live read                                   |
| `iceberg.latest_record_t`         | `pc.max(table.scan(selected_fields=("t",)).to_arrow())`   | Full column scan — expensive on large data  |
| `iceberg.row_count`               | `len(arrow)` from same scan                               | Same scan as above                          |
