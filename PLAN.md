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

- [x] Copy this file to repo root as `PLAN.md`.
- [x] Create feature branch: `git checkout -b feat/tansu-iceberg-pipeline`.
- [x] **Commit:** `docs: add tansu+iceberg pipeline plan`

---

## Phase 1 — Local smoke test (no GCP cost)

Goal: rewritten extractor / loader / generator working end-to-end on a local Docker Tansu with a sqlite catalog and local FS warehouse.

### 1.1 Dependency + Dockerfile updates
- [x] `extract/pyproject.toml`: add `confluent-kafka>=2.3.0`; remove `google-cloud-pubsub`.
- [x] `load/pyproject.toml`: add `confluent-kafka>=2.3.0`, `pyiceberg[sql-sqlite,sql-postgres,gcsfs,gcp-auth]>=0.7.0`, `psycopg2-binary`, `google-cloud-logging`.
- [x] Both Dockerfiles: install `librdkafka1` at runtime; loader also `libpq5`.
- [x] **Commit:** `chore: switch deps to confluent-kafka + pyiceberg`

### 1.2 Rewrite `extract/extractor.py` (Alpaca → Kafka)
- [x] Done. AdminClient topic creation on startup, Producer with delivery callback, periodic metrics emitter.
- [x] **Commit:** `feat(extract): produce alpaca bars to kafka via confluent-kafka`

### 1.3 Rewrite `extract/helpers/synthetic_stock_generator.py`
- [x] Done. confluent-kafka sync producer, AdminClient topic creation, CLI cleaned up.
- [x] **Commit:** `feat(extract): make synthetic generator runnable on confluent-kafka`

### 1.4 Rewrite `load/subscriber.py` → Kafka → Iceberg loader
- [x] Done. confluent Consumer, PyIceberg SqlCatalog, at-least-once commits, consumer_lag metric.
- [x] **Commit:** `feat(load): kafka→iceberg loader with cloud-logging metrics`

### 1.5 Run the smoke test ✅ PASSED
- [x] Started Tansu via Docker (memory:// storage)
- [x] Loader started: health server up, Iceberg table created (`alpaca.bars`)
- [x] Generator started: `sent=279+, errors=0`
- [x] Loader metrics confirmed: `messages_consumed=356, records_appended=1530, batches_flushed=35+, consumer_lag≈0, iceberg_append_errors=0`
- [x] Iceberg table verified: 1530 rows, symbols AAPL/TSLA/NVDA, schema correct
- [x] Tansu stopped and removed
- [x] **Commit:** `fix: tansu requires explicit topic creation + producer warmup`

> **Note (Tansu quirk):** Tansu does not auto-create topics. All producers/consumers must call `AdminClient.create_topics()` on startup. `producer.list_topics(timeout=10)` is also required to force the TCP handshake before the production loop starts, otherwise `poll(0)` fires with no ready callbacks.

---

## Phase 2 — GCP-VM staging (loader still local)

Goal: real Tansu VM + Cloud SQL + GCS warehouse stood up via Terraform; loader runs on laptop pointed at the cloud resources to validate networking and credentials before paying for Cloud Run.

### 2.1 Terraform skeleton at repo root
- [x] Created `terraform/{main.tf,variables.tf,outputs.tf,providers.tf,terraform.tfvars.example}`.
- [x] **Commit:** `feat(infra): full terraform stack — warehouse, catalog, tansu, cloud run, scheduler`

### 2.2 Storage modules
- [x] `terraform/modules/warehouse/`: GCS buckets, HMAC key, Secret Manager.
- [x] `terraform/modules/catalog/`: Cloud SQL Postgres, database, password → SM.
- [x] `terraform/modules/artifact-registry/`: data source for existing `alpaca-datalake` AR repo.

### 2.3 Tansu broker module
- [x] `terraform/modules/tansu-broker/`: wraps gcp-vm + tansu-install with HMAC s3 creds.
- [x] `tansu-install/main.tf`: additive HMAC env-var threading for s3:// backend.

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
