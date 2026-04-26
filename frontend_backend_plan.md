# Frontend Implementation Plan

FastAPI backend + React SPA as a Cloud Run service. Monitoring dashboard + OHLC visualization.

## Checklist

- [x] **Persist this checklist in the repo.** (`frontend_backend_plan.md`)
- [ ] **Scaffold uv member.** `frontend/pyproject.toml`, add to root workspace members, `uv sync`.
- [ ] **Backend: Iceberg read.** `iceberg_client.py`, `routes/bars.py` (`/api/symbols`, `/api/bars`).
- [ ] **Backend: Cloud Logging read.** `logging_client.py`, `routes/pipeline.py` (`/api/pipeline/status`, `/api/pipeline/metrics`).
- [ ] **Backend: app shell.** `app/main.py` — FastAPI, CORS, static mount, `/api/health`. Curl smoke test.
- [ ] **Commit checkpoint:** `feat(frontend): fastapi backend with iceberg + cloud logging read endpoints`
- [ ] **SPA scaffold.** Vite+React+TS, react-router, TanStack Query, lightweight-charts.
- [ ] **Monitoring page.** Status cards, lag sparkline, last-batch table; polls `/api/pipeline/status` every 5s.
- [ ] **Visualization page.** Symbol picker + date range → candlestick + volume chart.
- [ ] **Commit checkpoint:** `feat(frontend): react spa with monitoring and visualization views`
- [ ] 🛑 **Stage A verification.** Synthetic generator → GCP Tansu; local FastAPI + Vite → GCP Iceberg + Cloud Logging. **Pause for user sign-off.**
- [ ] **Dockerfile.** Multi-stage (node build → python+uv). Build + run locally.
- [ ] **build_and_push.sh.** Append `alpaca-frontend` build/push. Test with `v0.2.0`.
- [ ] **Commit checkpoint:** `feat(frontend): dockerfile and build script integration`
- [ ] **Terraform module.** `terraform/modules/frontend-service/` (clone loader-service). Wire into root `terraform/main.tf`.
- [ ] **Stage B deploy.** `terraform apply -var image_tag=v0.2.0`. Walk both pages on deployed URL.
- [ ] **Commit checkpoint:** `feat(frontend): cloud run service deployed`
- [ ] **Docs.** Update `CLAUDE.md`, `README.md`, `PHASES.md`.
- [ ] **Final commit:** `docs: document frontend component`

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
