# Cloud E2E Architecture

This document explains the current end-to-end cloud pipeline: how data moves, which GCP resources are involved, what is durable, what is scheduled, and where to monitor or query the result.

## System Goal

The pipeline captures Alpaca real-time bar data, buffers it through a Kafka-compatible broker, lands it into an Apache Iceberg table, and serves monitoring plus OHLC visualization from the persisted table.

Current cloud project:

```text
project-66783f65-9c3e-4880-9a3
```

Current region and zone:

```text
region: us-east1
zone:   us-east1-b
```

## High-Level Flow

```text
Alpaca WebSocket
    |
    | real-time stock bars
    v
Cloud Run Job: alpaca-extractor
    |
    | Kafka produce
    v
GCE VM: tansu-broker
    |
    | Kafka consume
    v
Cloud Run Service: alpaca-loader
    |
    | PyIceberg append
    v
Apache Iceberg table: alpaca.bars
    |
    | catalog metadata in Cloud SQL
    | data and Iceberg metadata files in GCS
    v
Cloud Run Service: alpaca-frontend
    |
    | FastAPI + web UI
    v
Monitoring and OHLC Visualization
```

## Resource Map

| Layer | Resource | Name | Role |
| --- | --- | --- | --- |
| Source | Alpaca WebSocket | IEX/real-time stream | Produces market bar events. |
| Extract | Cloud Run Job | `alpaca-extractor` | Connects to Alpaca and produces messages to Kafka. |
| Buffer | GCE VM | `tansu-broker` | Runs Tansu, the Kafka-compatible broker. |
| Network | Static external IP | `tansu-broker-static-ip` / `34.138.155.73` | Stable Kafka endpoint for Cloud Run jobs/services. |
| Load | Cloud Run Service | `alpaca-loader` | Consumes Kafka, batches rows, appends to Iceberg. |
| Catalog | Cloud SQL Postgres | `alpaca-iceberg-catalog` | Stores Iceberg catalog state for production tables. |
| Warehouse | GCS bucket | `project-66783f65-9c3e-4880-9a3-alpaca-iceberg-warehouse` | Stores Iceberg Parquet data, metadata JSON, manifests. |
| Serve | Cloud Run Service | `alpaca-frontend` | Serves API endpoints and frontend UI. |
| Schedule | Cloud Scheduler | `alpaca-infra-start`, `alpaca-extractor-start`, `alpaca-infra-stop` | Starts and stops daily runtime resources. |
| Control | Cloud Run Jobs | `alpaca-infra-start`, `alpaca-infra-stop` | Performs start/stop actions for cost control. |

## Data Contract

The main Iceberg table is:

```text
catalog:   alpaca_catalog
namespace: alpaca
table:     bars
full name: alpaca_catalog.alpaca.bars
```

Current logical row fields:

| Field | Meaning |
| --- | --- |
| `T` | Alpaca message type. |
| `S` | Symbol. |
| `o` | Open price. |
| `h` | High price. |
| `l` | Low price. |
| `c` | Close price. |
| `v` | Volume. |
| `t` | Bar timestamp, represented in UTC. |

All persisted and displayed timestamps should be treated as UTC.

## Extractor

`alpaca-extractor` is a Cloud Run Job.

It:

1. Reads Alpaca credentials from Secret Manager.
2. Opens the Alpaca WebSocket.
3. Subscribes to configured symbols.
4. Publishes each received frame to Kafka topic `alpaca-bars`.
5. Emits metrics logs for monitoring.

Important environment values:

```text
KAFKA_BROKER=34.138.155.73:9092
KAFKA_TOPIC=alpaca-bars
ALPACA_SYMBOLS=<configured symbols>
```

The extractor is a job, not a service. That fits the market-session workflow because it runs for a bounded period and can exit when the stream goes idle.

## Kafka / Tansu

`tansu-broker` is a small GCE VM running Tansu.

Current endpoint:

```text
34.138.155.73:9092
```

The external IP is reserved as a static regional address so stopping and starting the VM should not change the Kafka endpoint.

Tansu currently uses:

```text
memory://
```

That means Kafka is a transport buffer, not the durable source of truth. Durability is provided after successful Iceberg append into GCS plus Cloud SQL catalog metadata. The VM has persistent boot disk state and `tansu.service` is enabled, so the service should come back on VM restart, but in-memory Kafka messages do not survive a broker process/VM restart.

## Loader

`alpaca-loader` is a Cloud Run Service.

It:

1. Polls Kafka topic `alpaca-bars`.
2. Converts Alpaca frames into the Iceberg schema.
3. Buffers rows.
4. Flushes to Iceberg when batch size or batch interval is reached.
5. Commits Kafka offsets only after a successful Iceberg append.
6. Emits loader metrics logs used by the monitoring UI.

Current key behavior:

```text
BATCH_INTERVAL=300 seconds
```

So, under low volume, Iceberg snapshots are expected roughly every 5 minutes. Under higher volume, batch size can cause earlier flushes.

The loader is a background consumer. It does real work outside an HTTP request, so Cloud Run CPU throttling is disabled for the running instance:

```text
cpu_idle=false
```

Cost implication:

| State | Cost behavior |
| --- | --- |
| `min-instances=1` | Loader stays warm and CPU can remain allocated. Use during market hours. |
| `min-instances=0` | Loader scales to zero. Use outside market hours to save money. |

For the detailed loader runtime discussion and alternatives, see:

```text
docs/loader-runtime-architecture.md
```

## Iceberg Storage

Iceberg uses two storage planes:

| Plane | Backing service | What it stores |
| --- | --- | --- |
| Catalog | Cloud SQL Postgres | Table registrations, current metadata pointer, catalog state. |
| Warehouse | GCS | Parquet data files, Iceberg metadata JSON, manifest lists, manifests. |

Warehouse bucket:

```text
gs://project-66783f65-9c3e-4880-9a3-alpaca-iceberg-warehouse
```

Production table files are under the Iceberg warehouse path for:

```text
alpaca/bars
```

The Cloud SQL catalog must be available when production Iceberg tables are read or written through `alpaca_catalog`.

## Frontend And Monitoring

`alpaca-frontend` is a Cloud Run Service.

It serves:

| Endpoint | Purpose |
| --- | --- |
| `/api/symbols` | Lists symbols visible in Iceberg. |
| `/api/bars` | Returns OHLC bars for visualization. |
| `/api/pipeline/status` | Returns monitoring summary. |
| `/api/pipeline/metrics` | Returns metrics time series for charts. |

The monitoring page combines:

| Signal | Source |
| --- | --- |
| Extractor status | Cloud Logging metrics emitted by extractor. |
| Consumer lag | Loader/Kafka metrics. |
| Last batch | Loader metrics. |
| Last commit | Loader metrics after successful append and offset commit. |
| Iceberg freshness | Iceberg snapshot metadata. |
| Data lag | Latest record timestamp, moving toward loader metrics and Iceberg metadata/file stats. |

## Daily Cost-Control Schedule

The current intended weekday schedule is:

| Time | Timezone | Scheduler job | Action |
| --- | --- | --- | --- |
| `09:05` | `America/New_York` | `alpaca-infra-start` | Starts VM, starts Cloud SQL, sets loader min instances to 1. |
| `09:30` | `America/New_York` | `alpaca-extractor-start` | Starts the extractor job near market open. |
| `17:20` | `America/New_York` | `alpaca-infra-stop` | Sets loader min instances to 0, stops Cloud SQL, stops VM after the extractor idle-shutdown and loader drain window. |

The infra start/stop schedulers call Cloud Run control jobs:

```text
Cloud Scheduler -> Cloud Run Job -> gcloud command inside cloud-sdk container
```

This is used because Cloud Scheduler alone cannot easily perform every required start/stop action directly.

## Expected Running State

After `alpaca-infra-start`:

```text
tansu-broker               RUNNING
tansu-broker external IP   34.138.155.73
alpaca-iceberg-catalog     RUNNABLE / ALWAYS
alpaca-loader minScale     1
```

## Expected Idle State

After `alpaca-infra-stop`:

```text
tansu-broker               TERMINATED
alpaca-iceberg-catalog     STOPPED / NEVER
alpaca-loader minScale     0
```

## Manual Operations

For detailed manual commands, use:

```text
docs/runbooks/manual-cloud-pipeline-controls.md
```

Short version:

```bash
gcloud run jobs execute alpaca-infra-start \
  --project=project-66783f65-9c3e-4880-9a3 \
  --region=us-east1 \
  --wait

gcloud run jobs execute alpaca-extractor \
  --project=project-66783f65-9c3e-4880-9a3 \
  --region=us-east1 \
  --wait

gcloud run jobs execute alpaca-infra-stop \
  --project=project-66783f65-9c3e-4880-9a3 \
  --region=us-east1 \
  --wait
```

## Notebook Exploration

The local Spark notebooks are for exploration, not the live serving path.

Main notebook:

```text
notebooks/hadoop_catalog_with_on_demand_cloudsql.ipynb
```

It starts with a GCS-backed Hadoop Iceberg catalog for exploratory tables:

```text
gcs_iceberg.explore.sample_bars
```

It can optionally add the Cloud SQL production catalog when Cloud SQL and the Cloud SQL Auth Proxy are running:

```python
enable_cloudsql_catalog()
```

Production table:

```text
alpaca_catalog.alpaca.bars
```

If `/tmp` was cleaned and the notebook reports missing GCS key or JAR, run:

```bash
./scripts/setup_notebook_gcs_prereqs.sh
```

## Local Testing Architecture

Local tests use the same conceptual pipeline but smaller infrastructure:

```text
synthetic generator or local Alpaca extractor
    -> local Docker Tansu
    -> local loader
    -> local Iceberg warehouse
    -> FastAPI TestClient or local frontend
```

Important distinction:

| Run mode | Warehouse behavior |
| --- | --- |
| Automated pytest e2e | Uses temp sqlite catalog/warehouse and cleans up after test process. |
| Manual local run | Uses `./warehouse` and appends until explicitly removed. |
| Cloud run | Uses Cloud SQL catalog and GCS warehouse. |

## Durability And Failure Model

The strongest durability boundary is Iceberg:

```text
Kafka message -> loader append succeeds -> Kafka offset commit succeeds
```

The loader commits offsets after a successful append. This gives at-least-once behavior:

| Failure point | Expected behavior |
| --- | --- |
| Extractor fails before Kafka produce | Message may not exist downstream. |
| Loader fails before Iceberg append | Kafka offset is not committed; message can be retried. |
| Loader appends but fails before offset commit | Message can be processed again, so duplicates are possible. |
| Tansu VM restarts before messages are loaded | In-memory broker state can be lost. |
| Iceberg append succeeds | Data is durable in GCS and catalog state is in Cloud SQL. |

## Current Architecture Notes

- Terraform describes the base application stack, but some recent operational controls were applied directly with `gcloud`: static external IP, infra start/stop control jobs, and updated schedules.
- The older README resource table may show stale Tansu IP or scheduler times. Treat this document and `docs/runbooks/manual-cloud-pipeline-controls.md` as the current operating view.
- The frontend currently serves from Iceberg/Cloud SQL directly. That is acceptable for this project stage, but a future serving store such as Postgres can reduce latency for webapp-style reads while Iceberg remains the durable analytical store.
- If moving to a Hadoop Iceberg catalog for production, Cloud SQL catalog startup cost can be removed, but multi-writer coordination and catalog semantics need to be revisited carefully.
