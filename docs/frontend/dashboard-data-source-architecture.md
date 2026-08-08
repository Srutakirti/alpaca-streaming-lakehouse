# Dashboard Data Source Architecture

This note captures the current design question: should the monitoring dashboard
be fed from Iceberg, or should it use another source for live pipeline state?

## Short answer

Use Iceberg as the source of truth for persisted market data, but do not make
Iceberg the primary source for live pipeline observability.

The dashboard should use two classes of sources:

| Dashboard need | Preferred source |
|----------------|------------------|
| Historical OHLC bars | Iceberg table |
| Symbol discovery | Iceberg table |
| Latest persisted bar timestamp | Iceberg table, or later Iceberg metadata |
| Latest Iceberg snapshot freshness | Iceberg metadata |
| Extractor connection state | Metrics/logs |
| Kafka consumer lag | Metrics/logs |
| Last batch size/duration | Metrics/logs |
| Append errors | Metrics/logs |
| Alert-style health signals | Metrics/logs or a metrics backend |

## Why Iceberg is good for data views

Iceberg is the durable analytical table. It is the correct place to read:

- OHLC history for charts.
- Which symbols have landed.
- Whether rows are being persisted.
- Snapshot freshness and table growth.
- Historical validation after an end-to-end run.

For this project, using Iceberg for the visualization page is appropriate. The
frontend is asking: "what data has actually landed?" Iceberg answers that
directly.

## Why Iceberg should not own operational health

Operational health is different from persisted data state. The monitoring page
needs to answer questions like:

- Is the extractor connected?
- Is Kafka backing up?
- Did the loader successfully append the latest batch?
- How long did the last append take?
- Are append errors happening right now?

Those signals live naturally in the running services and Kafka consumer group,
not in the Iceberg table. If the dashboard only reads Iceberg, it can miss
important failure modes:

- Extractor is disconnected, but Iceberg still has recent historical rows.
- Loader is stuck before append, so no new Iceberg snapshot exists, but the
  cause is hidden.
- Kafka lag is growing, but Iceberg freshness still looks acceptable for a few
  minutes.
- Append errors happen repeatedly, but failed attempts are not represented as
  successful Iceberg snapshots.

## Current local architecture

The current local dashboard split is reasonable:

```text
Extractor / Loader
  -> structured local logs

Frontend API
  -> local logs for live pipeline metrics
  -> Iceberg for persisted data and table freshness

Frontend UI
  -> /api/pipeline/status
  -> /api/pipeline/metrics
  -> /api/bars
```

When `GCP_PROJECT_ID` is unset, `frontend/app/logging_client.py` reads local
metrics from `.local-run/logs`. When `GCP_PROJECT_ID` is set, it can read from
Cloud Logging instead.

## Production target

A cleaner production version should look like this:

```text
Extractor / Loader
  -> structured logs
  -> metrics backend
  -> traces, optional

Kafka
  -> consumer group offsets / lag

Iceberg
  -> persisted market data
  -> snapshot metadata

Dashboard API
  -> metrics backend for service health
  -> Kafka/admin client or exported metric for lag
  -> Iceberg catalog/table for data visibility
```

Candidate metrics backends:

- Google Cloud Monitoring, if staying on GCP.
- Prometheus / OpenTelemetry, if running locally or on Kubernetes.
- A small service-owned `/metrics` endpoint for the loader and extractor.

## Known concern: scanning Iceberg for max(t)

The current freshness implementation computes the latest bar timestamp by
scanning the Iceberg `t` column:

```text
max(t) over the table
```

This is acceptable for a small local warehouse, but it will become expensive as
the table grows. Later options:

- Read record upper bounds from Iceberg manifests instead of scanning rows.
- Maintain a tiny pipeline status table updated by the loader.
- Emit `latest_record_t` as a loader metric after each append.
- Store a compact state file or key-value record beside the warehouse.

The preferred next step is probably to emit `latest_record_t` from the loader as
a metric, while still keeping Iceberg as the source for historical chart data.

## Decision to revisit later

Keep the dashboard hybrid:

- Iceberg for data and table-level freshness.
- Logs/metrics for live operational status.

Revisit when the local demo grows into a production deployment or when Iceberg
status requests become noticeably slow.
