# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Real-time data pipeline streaming Alpaca IEX bar data to GCP via **Tansu Kafka** and writing to **Apache Iceberg**. uv workspace with two independent components:

- **extract/** — Cloud Run Job. Connects to Alpaca WebSocket, authenticates via Secret Manager, subscribes to symbols, and produces bar messages to the Tansu Kafka broker.
- **load/** — Cloud Run Service. Consumes from the Kafka broker, batches records, and appends Parquet to an Iceberg table (Cloud SQL Postgres catalog + GCS warehouse in production; sqlite + local FS for local smoke testing).

`tansu_kafka/` is a separate sub-project (Terraform + test suite for the Tansu/Kafka broker) with its own `CLAUDE.md`. The Tansu broker Terraform module in that directory is reused by the root `terraform/` stack.

## Commands

```bash
# Run extractor locally (requires GCP_PROJECT_ID for Secret Manager + Cloud Logging; KAFKA_BROKER; ADC)
uv run --package extract python extract/extractor.py

# Run subscriber (loader) locally (sqlite catalog + local FS for smoke testing)
KAFKA_BROKER=localhost:9092 ICEBERG_CATALOG_URI=sqlite:///./warehouse/catalog.db \
ICEBERG_WAREHOUSE=./warehouse LOG_MODE=stdout \
uv run --package load python load/subscriber.py

# Run synthetic generator (local only — test source; no GCP credentials needed)
uv run --package extract python extract/helpers/synthetic_stock_generator.py \
  --kafka localhost:9092 --topic alpaca-bars --symbols AAPL TSLA NVDA --rate 20

# Start Tansu locally (Docker)
docker run -d --name tansu -p 9092:9092 ghcr.io/tansu-io/tansu:0.6.0 \
  --storage-engine memory:// \
  --kafka-listener-url tcp://0.0.0.0:9092 \
  --kafka-advertised-listener-url tcp://localhost:9092

# Build Docker images — MUST be from workspace root.
docker build -t alpaca-extractor -f extract/Dockerfile .
docker build -t alpaca-loader    -f load/Dockerfile    .

# Build + push to Artifact Registry (run before terraform apply)
bash scripts/build_and_push.sh v0.1.0

# Deploy full GCP stack
cd terraform && terraform init && terraform apply -var image_tag=v0.1.0

# Tear down everything
cd terraform && terraform destroy
```

## Architecture

```
[Local / Phase 1]
  synthetic_stock_generator ──► local Docker Tansu ──► subscriber (local) ──► ./warehouse/ (Iceberg sqlite)

[GCP / Phase 2-3]
  Alpaca WebSocket ──► extract/extractor.py ──► Tansu VM (GCE, memory://) ──► load/subscriber.py ──► Cloud SQL catalog + GCS warehouse
```

### Extractor (`extract/extractor.py`)
- Async WebSocket client with exponential backoff (capped at `BACKOFF_MAX`).
- Fetches `ALPACA_KEY` / `ALPACA_SECRET` from Secret Manager at startup (not env vars).
- Produces each Alpaca WebSocket frame verbatim to Kafka via `confluent_kafka.Producer`. **Frames are JSON arrays of bar objects**, not single bars — the loader relies on this shape.
- Delivery callback tracks `delivery_failures`; `messages_sent` increments only on confirmed delivery.
- Metrics emitted every `METRICS_INTERVAL` seconds (not per-message) to Cloud Logging + stdout.
- After `MAX_RETRIES` consecutive failures the loop exits. Cloud Run Job handles restart scheduling (Cloud Scheduler `alpaca-extractor-start`).

### Loader (`load/subscriber.py`)
- Synchronous `consumer.poll(1.0)` loop (confluent-kafka Consumer). `enable.auto.commit=False`.
- Each Kafka message is decoded as a JSON list; each element projected onto the Iceberg schema (`T,S,o,h,l,c,v,t`) — extra Alpaca fields dropped.
- Flushes when `len(records) >= BATCH_SIZE` OR (`elapsed >= BATCH_INTERVAL` AND records non-empty).
- **At-least-once**: `consumer.commit(asynchronous=False)` only called after a successful `iceberg_table.append()`.
- `consumer_lag` (most important metric) logged every `METRICS_INTERVAL` seconds.
- Health check HTTP server on `PORT` (default 8080) for Cloud Run liveness.

### Synthetic generator (`extract/helpers/synthetic_stock_generator.py`)
- Synchronous confluent-kafka producer; local-only test source.
- Produces JSON arrays of bars matching the Alpaca WebSocket shape (same 8 fields).
- CLI: `--symbols`, `--rate`, `--kafka` (default `localhost:9092`), `--topic` (default `alpaca-bars`), `--metrics-interval`.

## Environment Variables

**Extractor**: `GCP_PROJECT_ID`, `KAFKA_BROKER` (default `localhost:9092`), `KAFKA_TOPIC` (default `alpaca-bars`), `ALPACA_SYMBOLS` (comma-separated). Optional: `ALPACA_WS_URI`, `MAX_RETRIES`, `TIMEOUT`, `BACKOFF_MAX`, `LOG_MODE`, `LOG_NAME`, `LOG_LEVEL`, `METRICS_INTERVAL` (default 10s).

**Loader**: `KAFKA_BROKER`, `KAFKA_TOPIC`, `ICEBERG_CATALOG_URI`, `ICEBERG_WAREHOUSE`. Optional: `KAFKA_GROUP_ID`, `ICEBERG_NAMESPACE`, `ICEBERG_TABLE`, `BATCH_SIZE` (default 100), `BATCH_INTERVAL` (default 300s), `PORT` (default 8080), `GCP_PROJECT_ID` (for Cloud Logging), `LOG_MODE`, `LOG_LEVEL`, `METRICS_INTERVAL`.

## Iceberg Schema

`T` (string, type), `S` (string, symbol), `o/h/l/c` (float64, OHLC), `v` (int64, volume), `t` (string, timestamp).

Both the PyArrow schema (`PYARROW_SCHEMA`) and the PyIceberg schema (`ICEBERG_TABLE_SCHEMA`) are defined at the top of `load/subscriber.py`. Adding a field requires updating both, plus the projection dict in the consume loop.

## Terraform (GCP deployment)

Root `terraform/` is the single entry point. It composes 6 modules:

| Module | Purpose |
|---|---|
| `warehouse` | GCS buckets (Iceberg warehouse + Tansu storage bucket) |
| `catalog` | Cloud SQL Postgres (db-f1-micro), Iceberg database + user, password → Secret Manager |
| `artifact-registry` | Data source for existing `alpaca-datalake` AR repo |
| `tansu-broker` | Wraps `tansu_kafka/terraform/modules/{gcp-vm,tansu-install}`; `memory://` storage (org policy blocks HMAC key creation) |
| `extractor-job` | Cloud Run v2 Job for `alpaca-extractor` + SA + IAM |
| `loader-service` | Cloud Run v2 Service for `alpaca-loader` + Cloud SQL Auth Proxy via `volumes { cloud_sql_instance }` + SA + IAM |
| `scheduler` | Cloud Scheduler start (08:00 ET) + stop (17:00 ET) jobs, weekdays only |

Phase 2 staging: `terraform apply -target=module.warehouse -target=module.catalog -target=module.artifact_registry -target=module.tansu_broker` then run the loader locally against the cloud resources.

## Tansu broker — local vs GCP

The only difference between environments is `KAFKA_BROKER`:
- **Local**: `localhost:9092` (docker Tansu, `--storage-engine memory://`)
- **GCP VM**: `<vm_ip>:9092` (`terraform output kafka_broker_url`)

All code uses the `KAFKA_BROKER` env var; no code changes between environments.

## Notes

- `extract/helpers/synthetic_stock_generator.py` is the **local test source** — it's runnable (uses confluent-kafka). Run it against any Tansu instance to generate synthetic bar traffic.
- Root `main.py` is a uv-init placeholder, not a pipeline entrypoint.
- The original Pub/Sub topic (`alpaca-bars`) and subscription (`alpaca-bars-sub`) still exist in GCP but are no longer used. Remove them as a separate cleanup task.
- Deployment commands (Artifact Registry push, Cloud Run execution) and full GCP resource inventory live in `README.md`. The three-phase rollout plan is in `PLAN.md`. Per-phase decisions and blockers are in `PHASES.md`.
- **Tansu quirk**: does not auto-create topics. All producers/consumers call `AdminClient.create_topics()` on startup. `producer.list_topics(timeout=10)` is also required to force the TCP handshake before the production loop.
- **Tansu storage**: uses `memory://` on the GCP VM. The org policy `iam.disableServiceAccountKeyCreation` blocks GCS HMAC key creation, so s3:// backend is unavailable. Iceberg on GCS is the durable store; Tansu is transport only.
- **Cloud SQL Auth Proxy**: For local Phase 2 testing, download `cloud-sql-proxy` and run `cloud-sql-proxy <connection_name> --port 5432`. The loader's `ICEBERG_CATALOG_URI` then points to `127.0.0.1:5432`. In Cloud Run (Phase 3), the proxy runs as a sidecar via `volumes { cloud_sql_instance }` — the unix socket is at `/cloudsql/<connection_name>`.
- **IPv6 note**: Laptop is IPv6-only; Cloud SQL `authorized_networks` only accepts IPv4. Use Cloud SQL Auth Proxy for all local access.
