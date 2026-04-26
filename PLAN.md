# Plan: Tansu Kafka + Iceberg Pipeline — Local Smoke → GCP-VM Staging → Full GCP, IaC-Managed

> **Note:** When this plan is approved, copy it to `PLAN.md` at the repo root and commit it as the first checkpoint (see Phase 0 below). Tick checkboxes as work progresses; each `**Commit:**` line is a git commit boundary.

## Context

The current pipeline is Alpaca WebSocket → Pub/Sub → GCS Parquet. We're replacing the Pub/Sub backbone with **Tansu Kafka** and the GCS Parquet sink with an **Apache Iceberg** table, then making the entire stack — broker, extractor, loader, Iceberg catalog and warehouse — deployable and tear-down-able with **`terraform apply` / `terraform destroy`**, so when the US stock market opens the user can flip a switch and have the real extractor producing into Tansu and the loader appending to Iceberg in GCP.

The synthetic generator stays **local-only** and is used as the test source in Phases 1 and 2. The real Alpaca extractor takes over in Phase 3.

User-confirmed decisions:
- **Pub/Sub fully replaced** by Kafka.
- **Kafka client**: `confluent-kafka` everywhere (Tansu compat).
- **Tansu storage in GCP**: `s3://` against a GCS bucket via HMAC keys.
- **Iceberg in GCP**: PyIceberg `SqlCatalog` over Cloud SQL Postgres + GCS warehouse.
- **Iceberg locally**: PyIceberg `SqlCatalog` over sqlite + local FS warehouse (smoke only).
- All code/IaC committed; full Terraform apply/destroy lifecycle.

## Architecture (final state)

```
[Laptop]                                       [GCP project-66783f65-9c3e-4880-9a3]

synthetic_stock_generator ──┐ (Phases 1 & 2)
                            ▼
                       ┌────────────┐    ┌─────────────────────────┐
                       │ Tansu VM   │←──┤ alpaca-extractor (Job)   │ (Phase 3)
                       │  s3://gcs  │    └─────────────────────────┘
                       └─────┬──────┘
                             ▼
                  ┌─────────────────────────┐    ┌──────────────────┐
                  │ alpaca-loader (Service) │───→│ Cloud SQL        │
                  │ Kafka → Iceberg append  │    │ (catalog)        │
                  └────────────┬────────────┘    └──────────────────┘
                               ▼
                       gs://alpaca-iceberg-warehouse/
```

All three components emit periodic structured metric snapshots → Cloud Logging.

---

## Phase 0 — Bootstrap (commit the plan, branch off)

- [ ] Copy this file to repo root as `PLAN.md`.
- [ ] Create feature branch: `git checkout -b feat/tansu-iceberg-pipeline`.
- [ ] **Commit:** `docs: add tansu+iceberg pipeline plan`

---

## Phase 1 — Local smoke test (no GCP cost)

Goal: rewritten extractor / loader / generator working end-to-end on a local Docker Tansu with a sqlite catalog and local FS warehouse.

### 1.1 Dependency + Dockerfile updates
- [ ] `extract/pyproject.toml`: add `confluent-kafka>=2.3.0`; remove `google-cloud-pubsub`. Keep `google-cloud-secret-manager`, `google-cloud-logging`, `websockets`.
- [ ] `load/pyproject.toml`: add `confluent-kafka>=2.3.0`, `pyiceberg[sql,gcs]>=0.7.0`, `psycopg2-binary`, `google-cloud-logging>=3.14.0`; remove `google-cloud-pubsub`, `google-cloud-storage`. Keep `pyarrow`.
- [ ] `extract/Dockerfile` + `load/Dockerfile`: install `librdkafka1` at runtime; loader also installs `libpq5`.
- [ ] `uv sync` from repo root → resolve.
- [ ] **Commit:** `chore: switch deps to confluent-kafka + pyiceberg`

### 1.2 Rewrite `extract/extractor.py` (Alpaca → Kafka)
- [ ] Drop `google.cloud.pubsub_v1`; add `confluent_kafka.Producer`.
- [ ] Replace `publish()` (lines 57-58) with `producer.produce(topic, value=msg, callback=on_delivery)` + periodic `producer.poll(0)`.
- [ ] Extend `Metrics` dataclass (lines 17-32) with `delivery_failures`, `last_delivery_ts`.
- [ ] Replace per-message metrics log (line 103) with periodic emitter (`METRICS_INTERVAL`, default 10s).
- [ ] Env vars: `KAFKA_BROKER` (default `localhost:9092`), `KAFKA_TOPIC` (default `alpaca-bars`); drop `PUBSUB_TOPIC_ID`.
- [ ] Keep Secret Manager fetch (139-141), retry loop (80-119), SIGTERM/SIGINT handlers (149-154) intact in shape.
- [ ] **Commit:** `feat(extract): produce alpaca bars to kafka via confluent-kafka`

### 1.3 Rewrite `extract/helpers/synthetic_stock_generator.py`
- [ ] Remove `aiokafka`, remove `extract.app.conf_reader.Config` import.
- [ ] Replace asyncio loop with synchronous `Producer` + `time.sleep(1/rate)`.
- [ ] CLI: `--symbols`, `--rate`, `--kafka` (default `localhost:9092`), `--topic` (default `alpaca-bars`); drop `--config`.
- [ ] Keep `StockPriceSimulator`, `STOCK_UNIVERSE`, JSON-array-of-bars message shape.
- [ ] Add periodic-metrics emitter (stdout-only by default).
- [ ] Add `aiokafka` removal note + `helpers/` no longer dead-code in `CLAUDE.md`.
- [ ] **Commit:** `feat(extract): make synthetic generator runnable on confluent-kafka`

### 1.4 Rewrite `load/subscriber.py` → Kafka → Iceberg loader
- [ ] Drop `google.cloud.pubsub_v1`, `google.cloud.storage`, `pyarrow.parquet`, `records_to_parquet()`, `gcs_path()`.
- [ ] Add `confluent_kafka.Consumer`, `pyiceberg.catalog.sql.SqlCatalog`.
- [ ] Env vars: `KAFKA_BROKER`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`, `ICEBERG_CATALOG_URI`, `ICEBERG_WAREHOUSE`, `ICEBERG_NAMESPACE`, `ICEBERG_TABLE`; keep `BATCH_SIZE`, `BATCH_INTERVAL`, `PORT`.
- [ ] On startup: load/create namespace + table from translated `SCHEMA` (lines 22-31).
- [ ] Loop: `consumer.poll(1.0)` → decode → project (line 94 logic) → buffer → flush on size/time → `iceberg_table.append(arrow_table)` → `consumer.commit(asynchronous=False)`. `enable.auto.commit=False`.
- [ ] Lift `setup_logging` from `extract/extractor.py:40-54` (Cloud Logging + stdout fan-out).
- [ ] Add metrics emitter: `messages_consumed`, `records_appended`, `batches_flushed`, `last_flush_duration_ms`, `iceberg_append_errors`, `consumer_lag` (via `get_watermark_offsets - position`), `last_commit_ts`.
- [ ] Keep health-check HTTP server (lines 34-46) verbatim.
- [ ] SIGTERM/SIGINT pattern (mirror `extractor.py:149-154`) → close consumer cleanly.
- [ ] **Commit:** `feat(load): kafka→iceberg loader with cloud-logging metrics`

### 1.5 Run the smoke test
- [ ] `docker run -d --name tansu -p 9092:9092 ghcr.io/tansu-io/tansu:0.6.0 --storage-engine memory:// --kafka-listener-url tcp://0.0.0.0:9092 --kafka-advertised-listener-url tcp://localhost:9092`
- [ ] Terminal A — loader: `LOG_MODE=stdout KAFKA_BROKER=localhost:9092 ICEBERG_CATALOG_URI=sqlite:///./warehouse/catalog.db ICEBERG_WAREHOUSE=./warehouse uv run --package load python load/subscriber.py`
- [ ] Terminal B — generator: `uv run --package extract python extract/helpers/synthetic_stock_generator.py --kafka localhost:9092 --topic alpaca-bars --symbols AAPL TSLA NVDA --rate 20`
- [ ] Verify loader logs show `messages_consumed` rising, `consumer_lag ≈ 0`, `batches_flushed` ticking.
- [ ] Verify rows in Iceberg: `python -c "from pyiceberg.catalog.sql import SqlCatalog; c=SqlCatalog('d', uri='sqlite:///./warehouse/catalog.db', warehouse='./warehouse'); print(c.load_table('alpaca.bars').scan().to_arrow().to_pandas().tail())"`
- [ ] `docker stop tansu && docker rm tansu`.
- [ ] **Commit:** `chore: phase-1 local smoke results / fixes` (only if any fixes were needed)

---

## Phase 2 — GCP-VM staging (loader still local)

Goal: real Tansu VM + Cloud SQL + GCS warehouse stood up via Terraform; loader runs on laptop pointed at the cloud resources to validate networking and credentials before paying for Cloud Run.

### 2.1 Terraform skeleton at repo root
- [ ] Create `terraform/{main.tf,variables.tf,outputs.tf,providers.tf,terraform.tfvars.example}`.
- [ ] Define `google` + `google-beta` providers; `var.project_id`, `var.region` (default `us-east1`), `var.image_tag`, `var.alpaca_symbols`.
- [ ] **Commit:** `chore(infra): scaffold root terraform stack`

### 2.2 Storage modules
- [ ] `terraform/modules/warehouse/`: two `google_storage_bucket`s (`alpaca-iceberg-warehouse`, `alpaca-tansu-storage`), `google_storage_hmac_key` for the Tansu service account, HMAC secret pushed to Secret Manager. `prevent_destroy = true` toggleable via var.
- [ ] `terraform/modules/catalog/`: Cloud SQL Postgres `db-f1-micro`, database `iceberg`, user `iceberg`, password via `random_password` → Secret Manager.
- [ ] `terraform/modules/artifact-registry/`: data source for `alpaca-datalake` repo, `count = 0/1` to create only if absent.
- [ ] **Commit:** `feat(infra): warehouse, catalog, artifact-registry modules`

### 2.3 Tansu broker module (wraps existing tansu_kafka modules)
- [ ] `terraform/modules/tansu-broker/`: instantiates `tansu_kafka/terraform/modules/gcp-vm` + `tansu-install`.
- [ ] Edit `tansu_kafka/terraform/modules/tansu-install/main.tf` (additive): thread HMAC creds + `AWS_ENDPOINT_URL=https://storage.googleapis.com` env vars into the systemd unit; pass `--storage-engine s3://alpaca-tansu-storage/`. Defaults preserve current behavior.
- [ ] Outputs: `broker_url`, `broker_ip`.
- [ ] **Commit:** `feat(infra): tansu broker module with s3-on-gcs storage`

### 2.4 Apply staging subset
- [ ] `cd terraform && terraform init && terraform apply -target=module.warehouse -target=module.catalog -target=module.artifact_registry -target=module.tansu_broker`.
- [ ] Capture outputs: `broker_ip`, `cloudsql_connection_name`, `cloudsql_public_ip`, warehouse bucket, HMAC creds.
- [ ] Authorize laptop IP on Cloud SQL instance (or use Cloud SQL Auth Proxy locally).
- [ ] Run loader locally pointed at cloud: `KAFKA_BROKER=<vm_ip>:9092 ICEBERG_CATALOG_URI="postgresql+psycopg2://iceberg:<pw>@<sql_ip>/iceberg" ICEBERG_WAREHOUSE=gs://alpaca-iceberg-warehouse/ LOG_MODE=both GCP_PROJECT_ID=... uv run --package load python load/subscriber.py`.
- [ ] Run generator locally with `--kafka <vm_ip>:9092`.
- [ ] Verify: rows in GCS warehouse, catalog rows in Cloud SQL, Tansu objects in `gs://alpaca-tansu-storage/`, structured logs in Cloud Logging (filter `logName=~"alpaca-(stream|loader)"`).
- [ ] **Commit:** `chore: phase-2 staging results / config tweaks` (only if fixes needed)

---

## Phase 3 — Full GCP production (Cloud Run + scheduler)

Goal: extractor and loader running in Cloud Run; one `terraform apply` deploys, one `terraform destroy` removes.

### 3.1 Cloud Run + scheduler modules
- [ ] `terraform/modules/extractor-job/`: `google_cloud_run_v2_job` for `alpaca-extractor`. Env: `GCP_PROJECT_ID`, `KAFKA_BROKER`, `KAFKA_TOPIC=alpaca-bars`, `ALPACA_SYMBOLS`, `LOG_MODE=both`, `METRICS_INTERVAL=10`. SA with `roles/secretmanager.secretAccessor`, `roles/logging.logWriter`.
- [ ] `terraform/modules/loader-service/`: `google_cloud_run_v2_service` for `alpaca-loader`. Env adds `ICEBERG_CATALOG_URI=postgresql+psycopg2://iceberg:<sm-secret>@/iceberg?host=/cloudsql/<conn>`, `ICEBERG_WAREHOUSE=gs://alpaca-iceberg-warehouse/`, `BATCH_SIZE`, `BATCH_INTERVAL`, `KAFKA_GROUP_ID`. Cloud SQL Auth Proxy via `cloudsql_instance` annotation. SA with `roles/cloudsql.client`, `roles/storage.objectAdmin` (warehouse bucket only), `roles/logging.logWriter`. Min instances = 1.
- [ ] `terraform/modules/scheduler/`: `alpaca-extractor-start` (`0 8 * * 1-5` America/New_York) + `alpaca-extractor-stop` (`0 17 * * 1-5`). Closes README TODO.
- [ ] Wire all modules into root `main.tf`.
- [ ] **Commit:** `feat(infra): cloud run extractor/loader + scheduler modules`

### 3.2 Image build/push script
- [ ] Add `scripts/build_and_push.sh`: builds both images tagged with `$TAG`, pushes to Artifact Registry. Image tag is a `terraform.tfvars` var.
- [ ] **Commit:** `chore: image build/push script`

### 3.3 Documentation
- [ ] Update `CLAUDE.md`: new Kafka/Iceberg architecture, env vars, three-phase rollout reference.
- [ ] Update `README.md`: replace Pub/Sub section with Kafka + Iceberg; add `terraform apply`/`destroy` recipe; tick the Iceberg + scheduler-stop TODOs.
- [ ] **Commit:** `docs: refresh CLAUDE.md and README for kafka+iceberg`

### 3.4 Production deploy dry-run
- [ ] `bash scripts/build_and_push.sh v0.1.0` → both images in Artifact Registry.
- [ ] `terraform apply -var image_tag=v0.1.0` → full stack up.
- [ ] One-shot: `gcloud run jobs execute alpaca-extractor --region=us-east1`. Verify (a) extractor connects to Alpaca, (b) producer metrics show `messages_sent` rising in Cloud Logging, (c) loader Cloud Run logs show `consumer_lag` near zero, (d) rows arrive in `gs://alpaca-iceberg-warehouse/`.
- [ ] Verify scheduler jobs visible: `gcloud scheduler jobs list --location=us-east1`.
- [ ] **Commit:** `chore: phase-3 deploy verified` (only if config tweaks were needed)

### 3.5 Market-open day (no work, just monitoring)
- [ ] At 08:00 ET, scheduler triggers `alpaca-extractor-start`. Watch Cloud Logging dashboard.
- [ ] Confirm `consumer_lag` stays near zero through the open.
- [ ] Confirm `alpaca-extractor-stop` fires at 17:00 ET.

### 3.6 Teardown when needed
- [ ] `terraform destroy` removes broker VM, Cloud SQL, Cloud Run, scheduler, HMAC keys.
- [ ] Buckets default to `prevent_destroy = true`; flip via var when intentional.

---

## Critical files / line refs to reuse

- `extract/extractor.py:40-54` — `setup_logging`. Lift verbatim into loader.
- `extract/extractor.py:17-32` — `Metrics` dataclass + `snapshot()`. Template for both producer and consumer metrics.
- `extract/extractor.py:149-154` — SIGTERM/SIGINT shutdown pattern.
- `load/subscriber.py:22-31` — pyarrow schema. Reuse for in-flight Arrow tables and as the source for the Iceberg schema translation.
- `load/subscriber.py:34-46` — health-check HTTP server (Cloud Run liveness).
- `tansu_kafka/python/src/tansu_test/main.py:54-110` — confluent-kafka Producer/Consumer config reference (delivery callbacks, manual commit, polling).
- `tansu_kafka/terraform/modules/{gcp-vm,tansu-install}/` — reused as-is via the new `terraform/modules/tansu-broker` wrapper; only `tansu-install/main.tf` needs additive HMAC env-var threading.

## Out of scope

- Nessie REST catalog (Cloud SQL Postgres satisfies the goal; one-line `ICEBERG_CATALOG_URI` swap later).
- Iceberg partitioning / schema evolution (initial table unpartitioned; day-level partition on `t` is a fast follow).
- Multi-broker Tansu / HA. Single VM is sufficient for bar volume.
- Production-grade Cloud SQL hardening (HA, private IP, custom backups). Defaults are fine for the data volume.
- Removing existing Pub/Sub topic + subscription resources from GCP — separate cleanup.
- Migrating historical Parquet from `gs://alpaca-iex-raw-events/` into the Iceberg table.
