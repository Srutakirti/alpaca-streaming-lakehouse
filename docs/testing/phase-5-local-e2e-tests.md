# Testing — Phase 5 (local e2e orchestration)

This documents the local end-to-end test and orchestration added in Phase 5 of
`TESTING_PLAN.md`. The goal is one command that proves the deploy-identical local
pipeline works from a synthetic Alpaca-shaped source all the way to the frontend API.

Scope: local Tansu via Docker Compose, make targets, and one opt-in pytest e2e module.
The real Rust extractor still needs Alpaca credentials + market hours, so this e2e uses
the existing synthetic generator as the deterministic data source.

---

## How to run

There are two local workflows:

| Workflow | Command | Warehouse behavior |
|---|---|---|
| Automated e2e test | `make e2e` | Uses a pytest temp sqlite catalog/warehouse; cleaned with the test process |
| Scripted real-Alpaca local run | `make local-real-up` | Uses `./warehouse`; persists until explicitly removed |
| Manual monitorable pipeline | `make up` + run loader + run generator | Uses `./warehouse`; persists until explicitly removed |

Use the automated e2e when you want a pass/fail proof. Use the manual pipeline when you
want to watch Kafka, Iceberg data files, Iceberg metadata files, or the frontend API over
time.

### Automated e2e

```bash
make e2e
```

`make e2e` does three things:

1. starts Tansu with Docker Compose,
2. waits until the broker answers metadata requests,
3. runs `KAFKA_BROKER=localhost:9092 uv run pytest -m e2e`.

Expected: `1 passed`. The target leaves the broker running for local debugging; clean up
with:

```bash
make down
```

The e2e test does **not** write to `./warehouse`. It uses the shared pytest
`tmp_iceberg` fixture, which creates a temporary sqlite catalog and filesystem
warehouse under pytest's temp directory. That keeps automated runs isolated from your
manual local data.

You can also run pieces directly:

```bash
make up
make wait-kafka
KAFKA_BROKER=localhost:9092 uv run pytest -m e2e -v
make down
```

### Manual monitorable run

The shortest path for a real Alpaca WebSocket run is the local supervisor:

```bash
# .env or .env.local
ALPACA_KEY=your_key
ALPACA_SECRET=your_secret
ALPACA_SYMBOLS=AAPL,TSLA,NVDA
```

```bash
make local-real-up
make local-real-status
make local-real-logs
make local-real-down
```

`make local-real-up` starts Tansu, waits for Kafka, then starts the loader, Rust
extractor, FastAPI backend, and Vite frontend dev server when `frontend/web/node_modules`
exists. It stores logs in `.local-run/logs/` and pid files in `.local-run/pids/`.
If both `.env` and `.env.local` exist, `.env.local` is loaded last and overrides `.env`.

If Vite is skipped, install once:

```bash
(cd frontend/web && npm install)
```

The fully expanded manual workflow is below.

Start the broker:

```bash
make up
```

Run the loader in one terminal:

```bash
KAFKA_BROKER=localhost:9092 \
ICEBERG_CATALOG_URI=sqlite:///./warehouse/catalog.db \
ICEBERG_WAREHOUSE=./warehouse \
LOG_MODE=stdout \
BATCH_SIZE=10 \
BATCH_INTERVAL=5 \
uv run --package load python load/subscriber.py
```

Run the synthetic generator in another terminal:

```bash
uv run --package extract python extract/helpers/synthetic_stock_generator.py \
  --kafka localhost:9092 \
  --topic alpaca-bars \
  --symbols AAPL TSLA NVDA \
  --rate 5
```

This writes to the persistent local Iceberg table:

```text
warehouse/
  catalog.db
  alpaca/bars/data/*.parquet
  alpaca/bars/metadata/*.metadata.json
  alpaca/bars/metadata/*.avro
```

`make down` stops Tansu and removes the Docker network, but it does not delete
`./warehouse`. Re-running the manual pipeline against the same env vars reuses the same
sqlite catalog/table and appends new data files plus new Iceberg metadata versions.

---

## What was added

| File | Purpose |
|---|---|
| `docker-compose.yml` | local Tansu broker on `localhost:9092` using `memory://` storage |
| `Makefile` | repeatable `up`/`down`/`test`/`test-integration`/`e2e`/`lint`/`smoke` targets |
| `scripts/wait_kafka.py` | bounded readiness check for Kafka metadata before tests run |
| `tests/e2e/test_pipeline_e2e.py` | synthetic generator → Kafka → loader → Iceberg → frontend API test |
| `README.md` / `CLAUDE.md` | documented the new local commands |

---

## Docker Compose broker

`docker-compose.yml` runs:

- image: `ghcr.io/tansu-io/tansu:0.6.0`
- storage: `memory://`
- listener: `tcp://0.0.0.0:9092`
- advertised listener: `tcp://localhost:9092`

The loader/frontend still run through `uv run` instead of containers. That keeps local
iteration fast and avoids introducing a second runtime shape for app code. The broker is
the only infrastructure dependency the integration/e2e suites need.

---

## Make targets

| Target | What it does |
|---|---|
| `make up` | starts the local Tansu service |
| `make down` | stops and removes the local Tansu service/network |
| `make wait-kafka` | waits for broker metadata using `scripts/wait_kafka.py` |
| `make test` | runs default Python tests and Rust unit tests |
| `make test-integration` | starts Tansu, waits for it, then runs `pytest -m integration` |
| `make e2e` | starts Tansu, waits for it, then runs `pytest -m e2e` |
| `make lint` | runs ruff on active Python paths plus Rust fmt/clippy |
| `make smoke` | wraps the ad-hoc Kafka, Iceberg, and frontend inspection scripts |

`make test-integration` and `make e2e` use the same `$KAFKA_BROKER` variable the apps
use. Override it to point tests at a different broker:

```bash
KAFKA_BROKER=35.x.x.x:9092 make e2e
```

---

## The e2e test

`tests/e2e/test_pipeline_e2e.py` is marked `@pytest.mark.e2e`, so it is deselected by
default. It proves one complete local path:

```text
synthetic_stock_generator.py
        -> Tansu Kafka topic
        -> load.subscriber.run_consumer()
        -> tmp sqlite Iceberg warehouse
        -> frontend FastAPI TestClient
```

Test flow:

1. Generate a unique Kafka topic and consumer group for isolation.
2. Start `extract/helpers/synthetic_stock_generator.py` as a subprocess for a few
   seconds with `AAPL` and `TSLA`.
3. Run the real loader consume loop (`run_consumer`) in a thread against the test's
   `tmp_iceberg` table.
4. Wait on `metrics.records_appended` instead of sleeping blindly.
5. Assert Iceberg has rows for both generated symbols.
6. Retarget the frontend's Iceberg client to the same temp warehouse, clear frontend
   caches, and call the real FastAPI app via `TestClient`.
7. Assert `/api/symbols`, `/api/bars`, and `/api/pipeline/status` reflect the generated
   data.

Cloud Logging is mocked to `None` in the e2e because the local proof is about data-plane
flow. Frontend route-specific logging behavior is already covered in Phase 3.

---

## Monitoring manual runs

Kafka topic contents:

```bash
uv run --package load python scripts/peek_kafka.py \
  --broker localhost:9092 \
  --topic alpaca-bars \
  --from-beginning \
  --max 20
```

Iceberg rows:

```bash
ICEBERG_CATALOG_URI=sqlite:///./warehouse/catalog.db \
ICEBERG_WAREHOUSE=./warehouse \
uv run --with duckdb --package load python scripts/query_iceberg.py
```

Newest data files:

```bash
watch -n 1 'ls -lt warehouse/alpaca/bars/data | head -10'
```

Newest Iceberg metadata JSON files:

```bash
watch -n 1 'ls -lt warehouse/alpaca/bars/metadata/*.metadata.json | head -10'
```

Inspect the latest metadata JSON:

```bash
latest=$(ls -t warehouse/alpaca/bars/metadata/*.metadata.json | head -1)
echo "$latest"
jq '.["current-snapshot-id"]' "$latest"
jq '.snapshots[-1]' "$latest"
```

The metadata directory also contains Avro manifest files (`*.avro`). The `*.metadata.json`
files are the table metadata versions; each successful append advances the current
snapshot and writes a new metadata version.

---

## Testing patterns used (and why)

- **One live external dependency** — only Kafka/Tansu is external. Iceberg is a real
  sqlite-backed temp warehouse, and FastAPI runs in process via `TestClient`.
- **Unique topic/group per run** — repeat runs do not collide with previous offsets.
- **Poll actual pipeline state** — `_wait_until` watches loader metrics, not a fixed
  sleep, which keeps the test bounded and less flaky.
- **Use production entry points** — the test drives the existing generator, loader
  `run_consumer`, loader `bootstrap_iceberg` fixture path, and frontend routes.
- **Cache hygiene before API assertions** — frontend Iceberg catalog and `/symbols`
  caches are cleared after the loader writes rows, matching the Phase 3 pattern.
- **Same suite, local or cloud** — broker selection comes from `$KAFKA_BROKER`; local
  compose is only the default convenience.

---

## Verification performed

```bash
uv run pytest
# 42 passed, 3 deselected

cd wsr && cargo test
# 18 passed

make lint
# passed

make e2e
# 1 passed, 44 deselected
```

After the e2e run, `make down` stopped and removed the Tansu container/network.

## Files (paths relative to repo root)

- `docker-compose.yml` — local Tansu broker.
- `Makefile` — local test/lint/e2e/smoke command surface.
- `scripts/wait_kafka.py` — broker readiness helper.
- `tests/e2e/test_pipeline_e2e.py` — full local e2e test.
- `extract/helpers/synthetic_stock_generator.py` — e2e producer/data source.
- `load/subscriber.py` — `run_consumer`, `project_frame`, `flush`, and Iceberg bootstrap
  used by the test path.
- `frontend/app/main.py`, `frontend/app/routes/bars.py`,
  `frontend/app/routes/pipeline.py`, `frontend/app/iceberg_client.py` — API read side
  exercised by `TestClient`.
- `conftest.py` — shared `tmp_iceberg` and `iceberg_env` fixtures.

## Commit trail

| Commit subject | Phase |
|---|---|
| `test(e2e): docker-compose + Makefile + full local pipeline e2e test` | 5, pending |
