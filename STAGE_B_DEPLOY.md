# Stage B (full GCP) — what shipped on 2026-04-27

Goal: synthetic generator runs on the laptop; everything else (Tansu broker,
loader, Iceberg catalog/warehouse, dashboard SPA + API) runs in GCP and is
provisioned by `terraform apply` / removed by `terraform destroy`.

## What was added

### `frontend/Dockerfile` (new)
Multi-stage image. Stage 1 builds the Vite SPA on `node:22-slim`. Stage 2
installs the `frontend` uv workspace package on `python:3.12-slim` and copies
the built SPA into `frontend/app/static/` where `frontend/app/main.py` mounts
it. Resulting image runs `uv run --package frontend python frontend/server.py`
and listens on `$PORT` (default 8080).

`.dockerignore` keeps `node_modules`, the local `frontend/web/dist`, the
local `warehouse/`, and `__pycache__` out of the build context.

### `scripts/build_and_push.sh` (modified)
Builds and pushes a third image (`alpaca-frontend`) alongside the existing
extractor and loader images. Same tag, same registry path.

### `terraform/modules/frontend-service/` (new)
Clone of `loader-service` with these deltas:
- Service account `alpaca-frontend`.
- IAM: `roles/cloudsql.client`, `roles/logging.logWriter`,
  **`roles/logging.viewer`** (so the FastAPI backend can read extractor +
  loader log entries for `/api/pipeline/status`), and
  **`roles/storage.objectViewer`** on the warehouse bucket (read-only is
  sufficient — frontend never writes).
- `min_instance_count = 0` (request-driven; cold-start is acceptable for a
  dashboard).
- No Kafka envs (frontend doesn't consume the topic).
- Cloud SQL Auth Proxy mounted at `/cloudsql` via
  `volumes { cloud_sql_instance }`, identical to loader-service.
- Public access toggle: `var.allow_unauthenticated = true` (default) creates
  a `roles/run.invoker` binding for `allUsers`.

### `terraform/main.tf` and `terraform/outputs.tf` (modified)
Added `module "frontend_service"` after `module "loader_service"`, wired to
the catalog + warehouse outputs. Exposed `frontend_service_url` as a root
output.

## Verified end-to-end on 2026-04-27

`terraform apply -var image_tag=v0.2.0-frontend -var allow_bucket_destroy=true`
produced **7 added, 4 changed, 0 destroyed**:

| Output | Value |
|---|---|
| `frontend_service_url` | `https://alpaca-frontend-asiokmz6bq-ue.a.run.app` |
| `kafka_broker_url`     | `35.196.44.0:9092` |
| `loader_service_url`   | `https://alpaca-loader-asiokmz6bq-ue.a.run.app` |

Smoke checks (deployed):
- `GET /api/health` → `{"status":"ok"}` (HTTP 200, ~0.6s).
- `GET /` → 200, `text/html` (SPA index served from `frontend/app/static/`).
- `GET /api/symbols` → `["AAPL","NVDA","TSLA"]`.
- `GET /api/pipeline/status` → snapshot count, latest record timestamp,
  and (after a generator run) live loader metrics — proves both the
  GCS+Cloud-SQL Iceberg read path and the Cloud Logging read path.

Generator run from laptop:
```
uv run --package extract python extract/helpers/synthetic_stock_generator.py \
  --kafka "$(terraform -chdir=terraform output -raw kafka_broker_url)" \
  --topic alpaca-bars --symbols AAPL TSLA NVDA --rate 20
```
Loader log moved from `messages_consumed: 0` → `68`, `batches_flushed: 0` →
`1`, `records_appended: 0` → `102`, `iceberg_append_errors: 0`. Iceberg
snapshot count went 28 → 29; row_count 2460 → 2562. Frontend reflected the
new state on the next `/api/pipeline/status` poll.

## Operating procedures

### Redeploy (full apply from scratch)

```
bash scripts/build_and_push.sh <tag>
terraform -chdir=terraform apply -var image_tag=<tag> -var allow_bucket_destroy=true
```

The `allow_bucket_destroy=true` is set up-front so a later `destroy` works
on non-empty buckets without state surgery.

### Run the generator against the cloud broker

```
export KAFKA_BROKER="$(terraform -chdir=terraform output -raw kafka_broker_url)"
uv run --package extract python extract/helpers/synthetic_stock_generator.py \
  --kafka "$KAFKA_BROKER" --topic alpaca-bars --symbols AAPL TSLA NVDA --rate 20
```

### Tear down

```
terraform -chdir=terraform destroy -var image_tag=<tag> -var allow_bucket_destroy=true
```

Removes the Cloud Run services (extractor job, loader, frontend), the
Tansu VM + firewall rules, the Cloud SQL instance + database + secret, the
GCS warehouse and Tansu storage buckets, the Cloud Scheduler jobs, and all
associated service accounts + IAM bindings.

## Caveats

- **Catalog state is not durable across destroy.** Iceberg namespace +
  table metadata lives in the Cloud SQL instance. After a destroy + reapply
  the catalog is empty; the loader recreates the namespace + table on its
  first batch. `/api/bars` will return empty (or 500) until then.
- **Artifact Registry images are stack-owned.** `terraform destroy` removes
  the repo and every image in it. Always run `build_and_push.sh` *before*
  the next `apply`.
- **Public dashboard.** The frontend is exposed to the internet via
  `allUsers` invoker. Switch `var.allow_unauthenticated` to `false` and
  front with IAP / an auth-aware load balancer if that becomes a concern.
- **Loader flush cadence.** Defaults are `BATCH_SIZE=100`, `BATCH_INTERVAL=300s`.
  A short generator burst (<100 records, <5 min) won't trigger a flush —
  pump enough records or wait the interval.
- **Iceberg freshness scan (deferred).** `_iceberg_freshness` in
  `frontend/app/routes/pipeline.py` still does a full `t`-column scan on
  every `/api/pipeline/status` call. Acceptable at demo scale; tracked for
  a follow-up to read manifest upper-bounds instead.
