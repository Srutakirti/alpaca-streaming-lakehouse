# Testing File Inventory

This is a structured map of the files used by the local test suite, what each file is
for, and the role it plays in testing the Alpaca → Kafka → Iceberg → frontend pipeline.

---

## Test configuration and shared harness

| File | Role in testing |
|---|---|
| `pyproject.toml` | Defines the root dev dependency group, workspace sources, pytest paths, marker defaults, and marker registration. |
| `conftest.py` | Provides shared factories/fixtures (`make_bar`, `make_frame`, `iceberg_env`, `tmp_iceberg`) and the logical pytest run-order hook. |
| `tests/test_scaffolding.py` | Smoke-tests that the test harness can import workspace packages and create a usable temp Iceberg table. |

---

## Loader unit tests

| File | Role in testing |
|---|---|
| `load/tests/test_projection.py` | Verifies `project_frame()` maps Alpaca bar frames onto the Iceberg schema and drops extra fields. |
| `load/tests/test_should_flush.py` | Verifies the loader flush predicate at size/time boundaries. |
| `load/tests/test_flush.py` | Verifies `flush()` appends to Iceberg, handles empty batches, accumulates metrics, and counts append failures. |
| `load/tests/test_metrics.py` | Verifies `_Metrics.snapshot()` defaults and updated counter values. |

---

## Loader integration tests

| File | Role in testing |
|---|---|
| `load/tests/test_consume_integration.py` | Opt-in Kafka → loader → Iceberg integration tests using a live broker and temp Iceberg warehouse. |

---

## Frontend API tests

| File | Role in testing |
|---|---|
| `frontend/tests/conftest.py` | Builds `TestClient` fixtures over seeded or empty temp Iceberg warehouses and resets frontend caches. |
| `frontend/tests/test_api.py` | Tests all FastAPI JSON routes, route validation, Iceberg reads, freshness reporting, and mocked Cloud Logging behavior. |

---

## End-to-end tests

| File | Role in testing |
|---|---|
| `tests/e2e/test_pipeline_e2e.py` | Opt-in full local path: synthetic generator → Tansu → loader → Iceberg → FastAPI API assertions. |

---

## Rust extractor tests

| File | Role in testing |
|---|---|
| `wsr/src/config.rs` | Contains inline unit tests for config defaults, required env handling, CSV parsing, numeric errors, and secret redaction. |
| `wsr/src/ws.rs` | Contains inline unit tests for WebSocket handshake/control-frame classification. |
| `wsr/src/metrics.rs` | Contains inline unit tests for metrics snapshots and timestamp rendering. |
| `wsr/Cargo.toml` | Defines the Rust package/dependencies used by `cargo test`, `cargo clippy`, and `cargo fmt --check`. |

---

## Source modules exercised by tests

| File | Role in testing |
|---|---|
| `load/subscriber.py` | Main loader source under test: schema, projection, flush logic, Kafka consume loop, metrics, and Iceberg bootstrap. |
| `frontend/app/main.py` | FastAPI app object used by `TestClient`. |
| `frontend/app/iceberg_client.py` | Frontend Iceberg catalog/table loader retargeted by tests to temp warehouses. |
| `frontend/app/routes/bars.py` | `/api/symbols` and `/api/bars` route logic exercised by API and e2e tests. |
| `frontend/app/routes/pipeline.py` | `/api/pipeline/status` and `/api/pipeline/metrics` route logic exercised by API and e2e tests. |
| `frontend/app/logging_client.py` | Cloud Logging boundary mocked by frontend tests and e2e tests. |
| `extract/helpers/synthetic_stock_generator.py` | Synthetic Alpaca-shaped Kafka producer used by the local e2e test and manual smoke testing. |

---

## Local orchestration and command surface

| File | Role in testing |
|---|---|
| `docker-compose.yml` | Starts local Tansu on `localhost:9092` for integration/e2e tests. |
| `Makefile` | Provides the supported local commands: `test`, `test-integration`, `e2e`, `lint`, `up`, `down`, and `smoke`. |
| `scripts/local_real_pipeline.sh` | Supervises a full local real-Alpaca run with logs/pids for loader, extractor, API, and frontend web. |
| `scripts/wait_kafka.py` | Waits for Kafka metadata before integration/e2e tests run. |

---

## Smoke and inspection helpers

| File | Role in testing |
|---|---|
| `scripts/peek_kafka.py` | Debug consumer for inspecting Kafka topic contents during smoke tests. |
| `scripts/query_iceberg.py` | Ad-hoc Iceberg query helper for validating rows and summaries in local or prod warehouses. |
| `scripts/inspect_frontend_api.py` | Calls frontend API endpoints and prints responses for manual smoke/debug sessions. |

---

## Testing documentation

| File | Role in testing |
|---|---|
| `TESTING_PLAN.md` | Master plan and progress tracker for the phased testing effort. |
| `docs/testing/phase-0-1-loader-tests.md` | Explains the shared harness and loader unit tests. |
| `docs/testing/phase-2-loader-integration-tests.md` | Explains the Kafka → loader → Iceberg integration tests. |
| `docs/testing/phase-3-frontend-tests.md` | Explains the FastAPI route tests and frontend cache retargeting. |
| `docs/testing/phase-4-rust-extractor-tests.md` | Explains the Rust extractor unit tests and refactors. |
| `docs/testing/phase-5-local-e2e-tests.md` | Explains the Docker Compose, Makefile, and full local e2e test. |
| `docs/testing/testing-file-inventory.md` | This file; maps the testing files and their roles. |
