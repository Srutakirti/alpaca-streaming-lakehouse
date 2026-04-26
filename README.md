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

See `PLAN.md` for the full checklist with verification steps.

## Quick Start — Local Smoke Test (Phase 1)

```bash
# 1. Start Tansu
docker run -d --name tansu -p 9092:9092 ghcr.io/tansu-io/tansu:0.6.0 \
  --storage-engine memory:// \
  --kafka-listener-url tcp://0.0.0.0:9092 \
  --kafka-advertised-listener-url tcp://localhost:9092

# 2. Start the loader (terminal A)
KAFKA_BROKER=localhost:9092 \
ICEBERG_CATALOG_URI=sqlite:///./warehouse/catalog.db \
ICEBERG_WAREHOUSE=./warehouse LOG_MODE=stdout \
uv run --package load python load/subscriber.py

# 3. Start the synthetic generator (terminal B)
uv run --package extract python extract/helpers/synthetic_stock_generator.py \
  --kafka localhost:9092 --topic alpaca-bars --symbols AAPL TSLA NVDA --rate 20

# 4. Inspect the Iceberg table
python -c "
from pyiceberg.catalog.sql import SqlCatalog
c = SqlCatalog('d', uri='sqlite:///./warehouse/catalog.db', warehouse='./warehouse')
print(c.load_table('alpaca.bars').scan().to_arrow().to_pandas().tail())
"

# 5. Clean up
docker stop tansu && docker rm tansu
```

## GCP Resources

| Resource | Name | Notes |
|---|---|---|
| GCS Bucket | `<project>-alpaca-iceberg-warehouse` | Iceberg Parquet files |
| GCS Bucket | `<project>-alpaca-tansu-storage` | Tansu s3:// backend |
| Cloud SQL | `alpaca-iceberg-catalog` | db-f1-micro Postgres |
| GCE VM | `tansu-broker` | e2-micro, Ubuntu 24.04 |
| Cloud Run Job | `alpaca-extractor` | Alpaca WS → Kafka |
| Cloud Run Service | `alpaca-loader` | Kafka → Iceberg |
| Artifact Registry | `alpaca-datalake` | us-east1 |
| Secret Manager | `ALPACA_KEY`, `ALPACA_SECRET` | Alpaca credentials |
| Secret Manager | `ICEBERG_DB_PASSWORD` | Cloud SQL password |
| Secret Manager | `TANSU_HMAC_SECRET` | GCS HMAC secret for Tansu |
| Cloud Scheduler | `alpaca-extractor-start` | 08:00 ET weekdays |
| Cloud Scheduler | `alpaca-extractor-stop` | 17:00 ET weekdays |

**Project**: `project-66783f65-9c3e-4880-9a3`

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
- [x] Cloud Scheduler to stop extractor at 17:00 ET
- [ ] Remove legacy Pub/Sub topic `alpaca-bars` and subscription `alpaca-bars-sub` from GCP
- [ ] Add day-level Iceberg partitioning on `t` field
- [ ] Harden `alpaca-extractor-stop` scheduler (cancel running executions via Cloud Function)
