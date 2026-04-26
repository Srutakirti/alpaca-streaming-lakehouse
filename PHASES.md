# Pipeline Phase Log

Documents what was done in each phase, decisions made, blockers hit, and how they were resolved.

---

## Phase 0 — Bootstrap

**Goal**: Commit the plan and branch off.

**What happened**:
- Copied the plan to `PLAN.md` at repo root.
- Created feature branch `feat/tansu-iceberg-pipeline`.
- Committed the plan as the first checkpoint.

---

## Phase 1 — Local Smoke Test

**Goal**: Rewritten extractor / loader / generator working end-to-end on a local Docker Tansu with a sqlite catalog and local FS warehouse.

### What was built

| Component | Change |
|---|---|
| `extract/extractor.py` | Replaced `google-cloud-pubsub` with `confluent-kafka` Producer + AdminClient |
| `load/subscriber.py` | Replaced Pub/Sub + GCS Parquet with Kafka Consumer + PyIceberg SqlCatalog |
| `extract/helpers/synthetic_stock_generator.py` | Replaced broken `aiokafka` with synchronous `confluent-kafka` Producer |
| `extract/pyproject.toml` | Added `confluent-kafka>=2.3.0`, removed `google-cloud-pubsub` |
| `load/pyproject.toml` | Added `confluent-kafka`, `pyiceberg[sql-sqlite,sql-postgres,gcsfs,gcp-auth]`, `psycopg2-binary`, `google-cloud-logging` |
| `extract/Dockerfile` | Added `librdkafka1` apt package |
| `load/Dockerfile` | Added `librdkafka1 libpq5` apt packages |

### Key design decisions

- **At-least-once delivery**: `enable.auto.commit=False`; `consumer.commit(asynchronous=False)` only after successful `iceberg_table.append()`.
- **Metrics emitter thread**: Both extractor and loader emit structured JSON snapshots on a configurable interval (not per-message), which lands cleanly in Cloud Logging.
- **Health check server**: HTTP server on `PORT` (default 8080) kept in the loader for Cloud Run liveness probes.

### Blockers and fixes

| Blocker | Fix |
|---|---|
| `pyiceberg[sql,gcs]` extras don't exist in v0.11.1 | Correct extras: `sql-sqlite`, `sql-postgres`, `gcsfs`, `gcp-auth` |
| `SQLite: unable to open database file` | Added `os.makedirs(ICEBERG_WAREHOUSE, exist_ok=True)` before SqlCatalog init |
| Generator `sent=0`, no delivery callbacks | `producer.poll(0)` is non-blocking; librdkafka needs `producer.list_topics(timeout=10)` first to force the TCP handshake |
| **Tansu does not auto-create topics** (critical) | Added `AdminClient.create_topics()` to all three components on startup |
| PyIceberg catalog name mismatch | Must use same name (`alpaca_catalog`) when creating and reading |

### Smoke test result ✅

- **1,530 rows** written to Iceberg: symbols AAPL, TSLA, NVDA
- `consumer_lag ≈ 0`, `iceberg_append_errors = 0`, `batches_flushed = 35+`
- Tansu: local Docker with `--storage-engine memory://`
- Catalog: sqlite at `./warehouse/catalog.db`
- Warehouse: local `./warehouse/`

---

## Phase 2 — GCP-VM Staging

**Goal**: Real Tansu VM + Cloud SQL + GCS warehouse stood up via Terraform; loader runs locally pointed at cloud resources to validate networking and credentials before paying for Cloud Run.

### Infrastructure deployed

| Resource | Details |
|---|---|
| GCE VM `tansu-broker` | e2-micro, Ubuntu 24.04, us-east1-b — `35.196.44.0:9092` |
| Cloud SQL Postgres | `alpaca-iceberg-catalog`, db-f1-micro, us-east1 — `35.237.144.68` |
| GCS bucket | `project-66783f65-9c3e-4880-9a3-alpaca-iceberg-warehouse` |
| GCS bucket | `project-66783f65-9c3e-4880-9a3-alpaca-tansu-storage` |
| Secret Manager | `ICEBERG_DB_PASSWORD` — Postgres password for `iceberg` user |
| Artifact Registry | `alpaca-datalake` (existing, data-sourced) |

### Blockers and fixes

| Blocker | Fix |
|---|---|
| `iam.disableServiceAccountKeyCreation` org policy blocks HMAC key creation | Switched Tansu to `memory://` storage — Iceberg on GCS is the durable store |
| Cloud SQL `authorized_networks` rejects IPv6 (laptop is IPv6-only) | Used Cloud SQL Auth Proxy locally instead of adding an IP whitelist entry |
| `min_instance_count` not valid directly in Cloud Run v2 `template` block | Moved it inside a `scaling {}` block |
| Cloud SQL instance in `PENDING_CREATE` when Terraform retried | Imported instance into state; waited for `RUNNABLE` state; re-applied |

### Local commands used (Phase 2 test)

```bash
# Cloud SQL Auth Proxy
/tmp/cloud-sql-proxy project-66783f65-9c3e-4880-9a3:us-east1:alpaca-iceberg-catalog --port 5432 &

# Loader pointed at GCP
KAFKA_BROKER=35.196.44.0:9092 \
ICEBERG_CATALOG_URI="postgresql+psycopg2://iceberg:<pw>@127.0.0.1:5432/iceberg" \
ICEBERG_WAREHOUSE="gs://project-66783f65-9c3e-4880-9a3-alpaca-iceberg-warehouse/" \
LOG_MODE=both GCP_PROJECT_ID=project-66783f65-9c3e-4880-9a3 PORT=8181 \
uv run --package load python load/subscriber.py

# Generator → GCP Tansu
uv run --package extract python extract/helpers/synthetic_stock_generator.py \
  --kafka 35.196.44.0:9092 --symbols AAPL TSLA NVDA --rate 10
```

### Staging result ✅

- **204+ records** appended to Iceberg in GCS, `iceberg_append_errors = 0`
- **4 Parquet files** confirmed in `gs://…/alpaca/bars/data/` + full Iceberg metadata
- **Structured logs** confirmed in Cloud Logging (`component=alpaca-loader`, metrics snapshots with `batches_flushed`, `records_appended`, `consumer_lag`)
- Tansu `memory://` on the VM is sufficient — all durability comes from Iceberg on GCS

---

## Phase 3 — Full GCP Production

**Goal**: Extractor and loader running in Cloud Run; one `terraform apply` deploys, one `terraform destroy` removes.

> Work in progress — see PLAN.md for current checkpoint status.
