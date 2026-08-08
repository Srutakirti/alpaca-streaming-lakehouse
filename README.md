# GCP Alpaca Datalake

Streams real-time bar data from Alpaca WebSocket, publishes to a **Tansu Kafka** broker, and writes to **Apache Iceberg** tables backed by Cloud SQL (catalog) and GCS (warehouse).

## Architecture

```
Alpaca WebSocket (IEX feed)
        │
   Cloud Run Job (alpaca-extractor)
        │  confluent-kafka Producer
        ▼
   Tansu VM — Kafka broker  ← also receives synthetic bars from local generator (for testing)
   (s3://GCS backend for durability)
        │  confluent-kafka Consumer
        ▼
   Cloud Run Service (alpaca-loader)
        │  PyIceberg append (at-least-once)
        ▼
   Iceberg table
   catalog: Cloud SQL Postgres
   warehouse: gs://alpaca-iceberg-warehouse/
```

## Three-Phase Rollout

| Phase | Tansu | Loader | Iceberg catalog | Source |
|---|---|---|---|---|
| 1 — local smoke | docker (localhost:9092) | local | sqlite + `./warehouse/` | synthetic generator |
| 2 — GCP staging | GCP VM (terraform) | local | Cloud SQL + GCS | synthetic generator |
| 3 — production | GCP VM (terraform) | Cloud Run Service | Cloud SQL + GCS | real Alpaca extractor |

See `TESTING_PLAN.md` for the local test and e2e checklist.

## Quick Start — Manual Local Pipeline

For the simplest real-Alpaca local run, create `.env` or `.env.local`:

```bash
ALPACA_KEY=your_key
ALPACA_SECRET=your_secret
ALPACA_SYMBOLS=AAPL,TSLA,NVDA
```

Then run:

```bash
make local-real-up
make local-real-status
make local-real-logs
make local-real-down
```

This starts Tansu, the loader, the Rust Alpaca extractor, the FastAPI backend, and the
Vite frontend dev server when `frontend/web/node_modules` exists. Logs are written under
`.local-run/logs/`, and pid files under `.local-run/pids/`.

If both `.env` and `.env.local` exist, `.env.local` is loaded last and overrides `.env`.

If the frontend web process is skipped, install its dependencies once:

```bash
(cd frontend/web && npm install)
```

The manual commands below are the same pipeline expanded into separate terminals.

```bash
# 1. Start Tansu
make up

# 2. Start the loader in terminal A
KAFKA_BROKER=localhost:9092 \
ICEBERG_CATALOG_URI=sqlite:///./warehouse/catalog.db \
ICEBERG_WAREHOUSE=./warehouse LOG_MODE=stdout \
uv run --package load python load/subscriber.py

# 3. Start the synthetic generator in terminal B
uv run --package extract python extract/helpers/synthetic_stock_generator.py \
  --kafka localhost:9092 --topic alpaca-bars --symbols AAPL TSLA NVDA --rate 20

# 4. Inspect the Iceberg table
python -c "
from pyiceberg.catalog.sql import SqlCatalog
c = SqlCatalog('d', uri='sqlite:///./warehouse/catalog.db', warehouse='./warehouse')
print(c.load_table('alpaca.bars').scan().to_arrow().to_pandas().tail())
"

# 5. Clean up
make down
```

This manual run writes to the persistent local Iceberg warehouse at `./warehouse`.
`make down` only stops Docker resources; it does not delete `./warehouse`. If you run
the loader again with the same `ICEBERG_CATALOG_URI` and `ICEBERG_WAREHOUSE`, PyIceberg
reuses the existing table and appends new Parquet files plus new metadata versions.

Useful inspection paths:

```bash
# Data files
ls -lt warehouse/alpaca/bars/data | head

# Iceberg metadata JSON and Avro manifest files
ls -lt warehouse/alpaca/bars/metadata | head

# Latest table metadata JSON
latest=$(ls -t warehouse/alpaca/bars/metadata/*.metadata.json | head -1)
jq '.["current-snapshot-id"]' "$latest"
```

## Tests

```bash
# Fast local unit/API tests; no Docker or GCP.
make test

# Loader integration test against local Tansu.
make test-integration

# Full local pipeline: synthetic generator -> Tansu -> loader -> Iceberg -> API.
make e2e

# Python lint plus Rust fmt/clippy.
make lint

# Ad-hoc inspection helpers for a running local stack.
make smoke
```

Real-Alpaca local run helpers:

```bash
make local-real-up       # start broker, loader, extractor, API, and Vite if installed
make local-real-status   # show process status
make local-real-logs     # tail .local-run/logs/*.log
make local-real-down     # stop local processes and Tansu
```

Automated integration/e2e tests use pytest temp warehouses, not `./warehouse`. Those
tests prove behavior and then their temp directories are cleaned by pytest. Use the
manual local pipeline above when you want files to remain available for inspection.

## GCP Resources

| Resource | Name | Notes |
|---|---|---|
| GCS Bucket | `project-66783f65-9c3e-4880-9a3-alpaca-iceberg-warehouse` | Iceberg Parquet files |
| GCS Bucket | `project-66783f65-9c3e-4880-9a3-alpaca-tansu-storage` | Reserved (Tansu uses memory://) |
| Cloud SQL | `alpaca-iceberg-catalog` | db-f1-micro Postgres, us-east1 |
| GCE VM | `tansu-broker` | e2-micro, Ubuntu 24.04, static IP `34.138.155.73:9092` |
| Cloud Run Job | `alpaca-extractor` | Alpaca WS → Kafka |
| Cloud Run Service | `alpaca-loader` | Kafka → Iceberg, private ingress, min=1 during market hours and min=0 after infra stop |
| Cloud Run Service | `alpaca-frontend` | Dashboard/API, private ingress |
| Artifact Registry | `alpaca-datalake` | us-east1 |
| Secret Manager | `ALPACA_KEY`, `ALPACA_SECRET` | Alpaca credentials (manually created) |
| Secret Manager | `ICEBERG_DB_PASSWORD` | Cloud SQL password (Terraform-managed) |
| Cloud Scheduler | `alpaca-infra-start` | 09:05 ET weekdays |
| Cloud Scheduler | `alpaca-extractor-start` | 09:30 ET weekdays |
| Cloud Scheduler | `alpaca-infra-stop` | 17:20 ET weekdays |

**Project**: `project-66783f65-9c3e-4880-9a3`

Current cloud operations are documented in:

- `docs/architecture-cloud-e2e.md`
- `docs/runbooks/manual-cloud-pipeline-controls.md`

## Secrets (Secret Manager)

Alpaca credentials are manually created and never managed by Terraform:
```bash
echo -n "your_value" | gcloud secrets versions add ALPACA_KEY --data-file=- --project=project-66783f65-9c3e-4880-9a3
echo -n "your_value" | gcloud secrets versions add ALPACA_SECRET --data-file=- --project=project-66783f65-9c3e-4880-9a3
```

## Deploy (Phase 2/3)

```bash
# 1. Copy and fill in tfvars
cd terraform
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars: set project_id, image_tag

# 2. Phase 2: bring up infra only (no Cloud Run yet)
terraform init
terraform apply -target=module.warehouse -target=module.catalog \
                -target=module.artifact_registry -target=module.tansu_broker

# 3. Build + push images (from workspace root)
bash scripts/build_and_push.sh v0.1.0

# 4. Phase 3: full deploy
terraform apply -var image_tag=v0.1.0

# Get the Kafka broker URL for local testing
terraform output kafka_broker_url

# Get the Cloud SQL URI for local loader testing (Phase 2)
terraform output catalog_uri_public   # requires authorized_networks in terraform/main.tf

# Tear everything down
terraform destroy
```

## Environment Variables

### Extractor (Cloud Run Job)

| Variable | Value | Notes |
|---|---|---|
| `GCP_PROJECT_ID` | `project-66783f65-9c3e-4880-9a3` | |
| `KAFKA_BROKER` | `<vm_ip>:9092` | From `terraform output kafka_broker_url` |
| `KAFKA_TOPIC` | `alpaca-bars` | |
| `ALPACA_SYMBOLS` | `AAPL,TSLA` | |
| `LOG_MODE` | `both` | `stdout` \| `cloud` \| `both` |
| `METRICS_INTERVAL` | `10` | Seconds between metric log snapshots |

### Loader (Cloud Run Service)

| Variable | Value | Notes |
|---|---|---|
| `KAFKA_BROKER` | `<vm_ip>:9092` | |
| `KAFKA_TOPIC` | `alpaca-bars` | |
| `ICEBERG_CATALOG_URI` | `postgresql+psycopg2://...` | Cloud SQL Auth Proxy unix socket path |
| `ICEBERG_WAREHOUSE` | `gs://…-alpaca-iceberg-warehouse/` | |
| `BATCH_SIZE` | `100` | |
| `BATCH_INTERVAL` | `300` | |

## Iceberg Table

Namespace `alpaca`, table `bars`. Schema:
`T` (string), `S` (string), `o` (double), `h` (double), `l` (double), `c` (double), `v` (int64), `t` (string).

Query with PyIceberg or DuckDB:
```python
from pyiceberg.catalog.sql import SqlCatalog
c = SqlCatalog("prod", uri="postgresql+psycopg2://iceberg:<pw>@<host>/iceberg",
               warehouse="gs://...-alpaca-iceberg-warehouse/")
df = c.load_table("alpaca.bars").scan().to_arrow().to_pandas()
```

## IAM Roles

| Service Account | Roles |
|---|---|
| `alpaca-extractor` | `secretmanager.secretAccessor`, `logging.logWriter` |
| `alpaca-loader` | `cloudsql.client`, `storage.objectAdmin` (warehouse bucket), `logging.logWriter` |
| `tansu-broker` | `storage.objectAdmin` (tansu bucket) |
| `alpaca-scheduler` | `run.invoker` |

## TODO

- [x] Kafka-based pipeline (Pub/Sub replaced)
- [x] Iceberg sink (GCS Parquet replaced)
- [x] Cloud Scheduler to start infra at 09:05 ET, start extractor at 09:30 ET, and stop infra at 17:20 ET (weekdays)
- [x] Full Terraform lifecycle (`apply` / `destroy`)
- [ ] Remove legacy Pub/Sub topic `alpaca-bars` and subscription `alpaca-bars-sub` from GCP
- [ ] Add day-level Iceberg partitioning on `t` field
- [ ] Codify direct `gcloud` infra-control changes in Terraform: static Tansu IP, infra start/stop jobs, and scheduler IAM.
