# Frontend Implementation Plan

FastAPI backend + React SPA as a Cloud Run service. Monitoring dashboard + OHLC visualization.

## Checklist

- [x] **Persist this checklist in the repo.** (`frontend_backend_plan.md`)
- [x] **Scaffold uv member.** `frontend/pyproject.toml`, add to root workspace members, `uv sync`.
- [x] **Backend: Iceberg read.** `iceberg_client.py`, `routes/bars.py` (`/api/symbols`, `/api/bars`).
- [x] **Backend: Cloud Logging read.** `logging_client.py`, `routes/pipeline.py` (`/api/pipeline/status`, `/api/pipeline/metrics`).
- [x] **Backend: app shell.** `app/main.py` — FastAPI, CORS, static mount, `/api/health`. Curl smoke test.
- [x] **SPA scaffold.** Vite+React+TS, react-router, TanStack Query, lightweight-charts.
- [x] **Monitoring page.** Status cards, lag sparkline, last-batch table; polls `/api/pipeline/status` every 5s.
- [x] **Visualization page.** Symbol picker + date range → candlestick + volume chart.
- [x] **Commit checkpoint:** `feat(frontend): fastapi backend with iceberg + cloud logging read endpoints` ✓
- [x] 🛑 **Stage A verification.** Synthetic generator → GCP Tansu; local FastAPI + Vite → GCP Iceberg + Cloud Logging. **Pause for user sign-off.**
- [ ] **Iceberg max(t) — switch to manifest upper-bounds.** `_iceberg_freshness` in `frontend/app/routes/pipeline.py` currently scans the whole `t` column to compute `latest_record_t`. Fine for local sqlite warehouse; will be expensive on the GCS warehouse. Replace with a read of per-file column upper-bounds from the current snapshot's manifest list (no data scan). *(Deferred — acceptable at demo scale; revisit if `/api/pipeline/status` latency becomes a problem.)*
- [x] **Dockerfile.** Multi-stage (node build → python+uv). Build + run locally.
- [x] **build_and_push.sh.** Append `alpaca-frontend` build/push. Test with `v0.2.0`.
- [x] **Commit checkpoint:** `feat(frontend): dockerfile and build script integration`
- [x] **Terraform module.** `terraform/modules/frontend-service/` (clone loader-service). Wire into root `terraform/main.tf`.
- [x] **Stage B deploy.** `terraform apply -var image_tag=v0.2.0-frontend -var allow_bucket_destroy=true`. Walk both pages on deployed URL.
- [x] **Commit checkpoint:** `feat(frontend): cloud run service deployed`
- [ ] **Docs.** Update `CLAUDE.md`, `README.md`, `PHASES.md`.
- [ ] **Final commit:** `docs: document frontend component`

---

## Production deployment notes (Stage B)

Recorded after the cloud-deployed verification on 2026-04-27 (image tag `v0.2.0-frontend`, frontend service URL `https://alpaca-frontend-asiokmz6bq-ue.a.run.app`).

- **Public access.** The frontend Cloud Run service is deployed with `allUsers` granted `roles/run.invoker`, gated by `var.allow_unauthenticated = true` in `terraform/modules/frontend-service`. Anyone with the URL can hit the SPA and `/api/*`. To restrict later: set `allow_unauthenticated = false` and front the service with IAP or an external HTTPS LB with auth.
- **Destroy requires `allow_bucket_destroy=true`.** The Iceberg warehouse and Tansu storage buckets default to `force_destroy = false`. Run `terraform apply` (and any `destroy`) with `-var allow_bucket_destroy=true` so non-empty buckets can be torn down. The current applied state has `force_destroy=true` on both.
- **Catalog state is not durable across destroy.** Iceberg namespace + table metadata lives in the Cloud SQL Postgres instance. `terraform destroy` drops the instance, so the next `apply` starts with an empty catalog. The loader recreates the namespace + table on its first batch — `/api/bars` will 500 until a generator run produces records and the loader flushes.
- **Artifact Registry images are owned by the stack.** The `artifact-registry` module owns the repo; `terraform destroy` removes all images. Always run `bash scripts/build_and_push.sh <tag>` **before** `terraform apply` after a teardown.
- **Generator runs from the laptop unchanged.** `extract/helpers/synthetic_stock_generator.py --kafka $(terraform -chdir=terraform output -raw kafka_broker_url)` works because the Tansu VM has a public IP, port 9092 is open `0.0.0.0/0`, and Tansu advertises its public IP. No firewall or broker-config changes are required for the laptop → cloud generator path.
- **Loader flush cadence.** With defaults `BATCH_SIZE=100` / `BATCH_INTERVAL=300s`, a short generator burst may not trigger a size-based flush. Either let the 5-minute interval elapse or pump enough records to clear `BATCH_SIZE`.

---

## Stage A verification steps (local frontend → GCP services)

```
synthetic_stock_generator (laptop)
   ↓
GCP Tansu VM  →  Cloud Run loader  →  GCS Iceberg + Cloud SQL catalog
                                              ↑ reads
                                    local FastAPI (:8080)
                                              ↑ reads Cloud Logging
                                    local Vite SPA (:5173)
```

1. `cloud-sql-proxy <PROJECT>:us-east1:alpaca-iceberg-catalog --port 5432`
2. `terraform output -raw kafka_broker_url` → get `<vm_ip>`
3. Run generator:
   ```
   uv run --package extract python extract/helpers/synthetic_stock_generator.py \
     --kafka <vm_ip>:9092 --topic alpaca-bars --symbols AAPL TSLA NVDA --rate 20
   ```
4. Run backend (prod env):
   ```
   GCP_PROJECT_ID=<project> \
   ICEBERG_CATALOG_URI="postgresql+psycopg2://iceberg:<pw>@127.0.0.1:5432/iceberg" \
   ICEBERG_WAREHOUSE="gs://<warehouse-bucket>" \
   uv run --package frontend uvicorn frontend.app.main:app --reload --port 8080
   ```
5. Run SPA: `cd frontend/web && npm run dev`
6. Smoke checks:
   - `curl localhost:8080/api/health` → 200
   - `curl localhost:8080/api/symbols` → AAPL/TSLA/NVDA
   - `curl 'localhost:8080/api/bars?symbol=AAPL&limit=10'` → 10 rows
   - `curl localhost:8080/api/pipeline/status` → non-null metrics
   - `http://localhost:5173` → both pages render with data

---

## Stage B verification steps (Cloud Run deploy)

1. `docker build -t alpaca-frontend -f frontend/Dockerfile .`
2. `bash scripts/build_and_push.sh v0.2.0`
3. `cd terraform && terraform apply -var image_tag=v0.2.0`
4. `curl $(terraform output -raw frontend_url)/api/health` → 200
5. Walk both pages in browser; confirm parity with Stage A.

---

## API surface

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/health` | liveness |
| GET | `/api/pipeline/status` | extractor + loader last metrics + Iceberg freshness |
| GET | `/api/pipeline/metrics?component=loader&minutes=60` | Cloud Logging timeseries |
| GET | `/api/symbols` | distinct symbols (cached 60s) |
| GET | `/api/bars?symbol=AAPL&from=ISO&to=ISO&limit=5000` | OHLC rows |
| GET | `/*` | built SPA static assets |
