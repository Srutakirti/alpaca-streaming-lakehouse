# Frontend

Monitoring + visualization dashboard for the Alpaca → Tansu Kafka → Iceberg pipeline.

A single Cloud Run service (image `alpaca-frontend`, Terraform module `frontend-service`)
that bundles two things into one container:

- **A FastAPI backend** (`app/`) — read-only JSON API over the Iceberg table and Cloud Logging.
- **A Vite/React SPA** (`web/`) — built to static assets and served by that same FastAPI app.

The service never writes to the pipeline. It reads the Iceberg table (OHLC bars) and tails
the extractor/loader metrics logs to render pipeline health and candlestick charts.

---

## High-level

### Architecture

```
                    ┌──────────────────────────────────────────┐
  Browser ───────►  │  FastAPI (app/main.py) on :8080           │
                    │                                            │
                    │   /                → React SPA (static)    │
                    │   /assets/*        → JS/CSS bundle         │
                    │   /api/health      → liveness              │
                    │   /api/symbols     ┐                       │
                    │   /api/bars        ┘─► Iceberg (bars table) │
                    │   /api/pipeline/*  ──► Cloud Logging + Iceberg
                    └──────────────────────────────────────────┘
                                  │                    │
                                  ▼                    ▼
                       Iceberg table             Cloud Logging
                  (Cloud SQL catalog +      (alpaca-extractor /
                     GCS warehouse)            alpaca-loader logs)
```

In production both the SPA and the API are served from the same origin, so the browser
talks to relative paths (`/api/...`). In local dev the Vite dev server proxies `/api`
to the FastAPI process on `:8080` (see `web/vite.config.ts`).

### Subfolders

| Path | What it is |
|---|---|
| `app/` | FastAPI backend — routers, Iceberg reader, Cloud Logging reader. Built static assets land in `app/static/` inside the image. |
| `app/routes/` | HTTP route handlers grouped by concern (`bars`, `pipeline`). |
| `web/` | Vite/React/TypeScript SPA source. Built to `web/dist/` and copied into the image. |
| `web/src/` | React app source — entrypoint, pages, and shared helpers. |
| `web/src/pages/` | One component per route: `Monitoring` and `Visualization`. |
| `web/src/lib/` | Framework-agnostic helpers (UTC time formatting). |
| `web/public/` | Static files copied verbatim into the build (favicon, icons). |

### Data flow

1. **Bars** — `web` calls `/api/symbols` and `/api/bars`; `app/routes/bars.py` scans the
   Iceberg `alpaca.bars` table (column + row-filter pushdown) and returns OHLCV rows.
2. **Pipeline health** — `web` polls `/api/pipeline/status` and `/api/pipeline/metrics`;
   `app/routes/pipeline.py` combines the latest extractor/loader metric log entries
   (`app/logging_client.py`) with Iceberg snapshot/freshness info (`app/iceberg_client.py`).

### Build & run

```bash
# 1. Build the SPA (must happen before building the image — dist/ is bundled in)
(cd web && npm install && npm run build)

# 2. Run the backend locally (serves built dist + JSON APIs) from the repo root
ICEBERG_CATALOG_URI=sqlite:///./warehouse/catalog.db ICEBERG_WAREHOUSE=./warehouse \
uv run --package frontend python frontend/server.py
# → http://localhost:8080

# Or run the SPA with hot-reload against a running backend
(cd web && npm run dev)   # Vite on :5173, proxies /api → :8080
```

The `Dockerfile` does both stages: a `node` builder stage runs `npm run build`, then a
`python:3.12-slim` stage installs the package with `uv` and copies `web/dist/` into
`frontend/app/static/`. **Build the image from the workspace root**, not from `frontend/`:

```bash
docker build -t alpaca-frontend -f frontend/Dockerfile .
```

### Environment variables

| Var | Default | Used by |
|---|---|---|
| `PORT` | `8080` | `server.py` (uvicorn bind port) |
| `ICEBERG_CATALOG_URI` | `sqlite:///./warehouse/catalog.db` | `iceberg_client.py` |
| `ICEBERG_WAREHOUSE` | `./warehouse` | `iceberg_client.py` |
| `ICEBERG_NAMESPACE` | `alpaca` | `iceberg_client.py` |
| `ICEBERG_TABLE` | `bars` | `iceberg_client.py` |
| `GCP_PROJECT_ID` | _(empty)_ | `logging_client.py` — without it, pipeline-metric endpoints return null/empty and the UI shows "No GCP logs" |
| `LOG_NAME_EXTRACTOR` | `alpaca-extractor` | `logging_client.py` |
| `LOG_NAME_LOADER` | `alpaca-loader` | `logging_client.py` |

---

## Low-level (file by file)

### Backend (`app/`)

**`server.py`** (package root) — Process entrypoint. Inserts the workspace root onto
`sys.path` so `frontend.*` is importable, then launches uvicorn on
`frontend.app.main:app` at `$PORT` (default 8080). This is the container `CMD`.

**`app/main.py`** — Builds the `FastAPI` app. Adds permissive CORS (GET-only), mounts the
two routers (`bars` under `/api`, `pipeline` under `/api/pipeline`), and exposes
`/api/health`. If a built SPA exists at `app/static/`, it mounts `/assets` for the JS/CSS
bundle and adds a catch-all route that serves `index.html` for any other path — the
client-side router (`react-router-dom`) then takes over (SPA fallback).

**`app/iceberg_client.py`** — Thin accessor for the Iceberg `bars` table. Reads catalog
config from env vars, lazily creates a `SqlCatalog` (cached with `lru_cache`, and `mkdir`s
a local warehouse dir for sqlite/local-FS smoke testing). `get_table()` re-loads the table
on every call so callers always see fresh snapshot metadata while reusing the cached
catalog connection.

**`app/logging_client.py`** — Reads pipeline metrics out of Google Cloud Logging. The
extractor/loader emit structured metric snapshots as log entries; this module tails them.
- `_tail_component(...)` — internal helper; returns the most recent dict payload for a log
  name within the last N minutes, or `None`.
- `get_last_extractor_metrics()` / `get_last_loader_metrics()` — latest snapshot per
  component (15-minute window).
- `get_metrics_timeseries(component, minutes)` — time-ordered list of snapshots (each row
  annotated with `_ts`), used for the lag sparkline.
- All functions short-circuit to empty/`None` when `GCP_PROJECT_ID` is unset and swallow
  client errors, so the dashboard degrades gracefully without GCP access.

**`app/routes/bars.py`** — OHLC data endpoints.
- `GET /api/symbols` — distinct `S` values from the table, scanned with a 60-second
  in-process cache.
- `GET /api/bars?symbol=&from=&to=&limit=` — scans the table with an `EqualTo("S", …)`
  row filter plus optional `t` range bounds and column projection (`t,o,h,l,c,v`), capped
  at `limit` (≤10000). Returns a list of bar dicts.

**`app/routes/pipeline.py`** — Pipeline-health endpoints.
- `_iceberg_freshness()` — internal; reports snapshot count, latest snapshot timestamp,
  and (by scanning the `t` column) the max record timestamp + row count. Has a TODO to use
  manifest upper-bounds instead of full scans for large warehouses.
- `GET /api/pipeline/status` — merges latest extractor metrics, latest loader metrics, and
  Iceberg freshness into one object (the Monitoring page's primary feed).
- `GET /api/pipeline/metrics?component=&minutes=` — metric time series for `loader` or
  `extractor` (drives the lag sparkline).

**`app/__init__.py` / `app/routes/__init__.py`** — empty package markers.

### Frontend (`web/`)

**`web/index.html`** — Vite HTML entry; mounts the React root and loads `src/main.tsx`.

**`web/src/main.tsx`** — React bootstrap. Wraps `<App>` in `QueryClientProvider`
(`@tanstack/react-query` for data fetching/caching) and `BrowserRouter`, renders into
`#root` under `StrictMode`.

**`web/src/App.tsx`** — Top-level layout and routing. Renders the nav bar and maps two
routes: `/` → `Monitoring`, `/viz` → `Visualization`.

**`web/src/pages/Monitoring.tsx`** — Pipeline health page. Polls `/api/pipeline/status`
(5s) and `/api/pipeline/metrics` (30s). Renders status cards (extractor connection,
consumer lag, last batch, Iceberg freshness, data lag, append errors, last commit) with
color thresholds, plus a `lightweight-charts` line sparkline of consumer lag over the last
60 minutes. Includes local helpers for lag formatting (`dataLagLabel`, `msAgo`) and the
`StatusCard` / `LagSparkline` subcomponents.

**`web/src/pages/Visualization.tsx`** — OHLC candlestick page. Symbol dropdown
(from `/api/symbols`) plus UTC date/time range inputs with draft-vs-applied state (changes
only take effect on **Apply**/Enter), client-side validation, and an open-ended "to" bound.
Fetches `/api/bars` and renders a `lightweight-charts` candlestick + volume-histogram
chart. Deduplicates bars sharing the same unix second into a single merged candle (charts
require strictly ascending times) and shows guidance when fewer than 2 distinct timestamps
exist.

**`web/src/lib/time.ts`** — UTC time-formatting helpers used across both pages. Everything
derives from `Date.prototype.getUTC*` so the viewer's local timezone never leaks into
rendered labels. Exposes `formatUtcDateTime`, `formatUtcTime`, `formatUtcDate`, plus
`formatUtcChartLabel` / `formatUtcChartTick` for lightweight-charts axis ticks and
crosshair labels.

**`web/src/App.css` / `web/src/index.css`** — Global and component styling (dark theme,
cards, charts, controls).

**`web/src/assets/`** — Bundled images referenced from code (`hero.png`, logos).

**`web/public/`** — Static assets copied verbatim into the build (`favicon.svg`,
`icons.svg`).

### Config files (`web/`)

| File | Purpose |
|---|---|
| `package.json` | Dependencies (React 19, react-router, react-query, lightweight-charts) and scripts (`dev`, `build`, `lint`, `preview`). |
| `vite.config.ts` | Vite config; React plugin + dev-server proxy from `/api` → `localhost:8080`. |
| `tsconfig.json` / `tsconfig.app.json` / `tsconfig.node.json` | TypeScript project references (app vs. Node tooling). |
| `eslint.config.js` | ESLint flat config for the SPA. |
| `web/README.md` | Stock Vite React+TS template readme (not project-specific). |

### Packaging

**`pyproject.toml`** — `frontend` workspace package: FastAPI, uvicorn, pyiceberg
(`sql-postgres`), psycopg2, pyarrow, google-cloud-logging.

**`Dockerfile`** — Two-stage build: (1) `node:22-slim` builds the SPA; (2) `python:3.12-slim`
installs the package via `uv` and copies `web/dist/` → `frontend/app/static/`. Exposes
8080 and runs `server.py`.
