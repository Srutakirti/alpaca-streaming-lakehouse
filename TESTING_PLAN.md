# Plan: Local end-to-end testing + test suite for the Alpaca → Kafka → Iceberg pipeline

## Context

The pipeline has three working-but-untested core pieces: a **Rust extractor** (`wsr/`,
Alpaca WebSocket → Kafka), a **Python loader** (`load/subscriber.py`, Kafka → Iceberg),
and a **FastAPI frontend** (`frontend/`, reads Iceberg + Cloud Logging). All of it runs
in GCP today, but there is **zero automated test coverage**: no pytest, no Rust tests, no
fixtures, no local orchestration, no CI. Validation has been manual smoke-testing only.

The goal is to **fully exercise the pipeline locally and solidify it with good tests**
before relying on the cloud, while keeping the code deploy-identical local→cloud (it
already is — everything is driven by env vars like `KAFKA_BROKER`, `ICEBERG_CATALOG_URI`,
`ICEBERG_WAREHOUSE`). Cloud/Terraform wiring of the Rust extractor is a **later phase**,
done only after local testing is solid.

**Decisions (confirmed with user):**
- Focus on the **Rust extractor (`wsr/`)** as the canonical extractor. Ignore/retire
  `extract/extractor.py` for this effort; do not add tests for it.
- **Local docker** integration: fast unit tests run anywhere; integration + e2e tests
  spin up Tansu via docker-compose and are **opt-in** (pytest markers). No CI Kafka.
- **No GitHub Actions** — provide a `Makefile` with `test`/`lint`/`e2e`/`up`/`down` targets.
- **Frontend: Python API tests only** (no Vitest/React tests).

**Design principle — same suite, local or cloud.** Every integration/e2e test reads its
connection config from the *same env vars the apps use* (`KAFKA_BROKER`,
`ICEBERG_CATALOG_URI`, `ICEBERG_WAREHOUSE`, `GCP_PROJECT_ID`), defaulting to local
docker + sqlite. Pointing the suite at cloud later = export different env vars, no code
change. This keeps "tests can eventually validate the cloud deployment itself" true by
construction.

**Local e2e reality:** the Rust extractor needs real Alpaca creds + market hours, so the
automated local e2e drives the pipeline with the existing **synthetic generator**
(`extract/helpers/synthetic_stock_generator.py`) → Tansu → loader → Iceberg → frontend.
The Rust extractor itself is covered by unit tests plus a documented real-cred smoke check.

---

## Progress

| Phase | Status | Commit |
|---|---|---|
| 0 — scaffolding | ✅ done | `52d1f43` |
| 1 — loader units | ✅ done | `af4d1cd` |
| 2 — loader integration | ✅ done | `8feb497` |
| (logical run order) | ✅ done | `60c9069` — pipeline-flow ordering hook |
| 3 — frontend API | ✅ done | `dfda24f` |
| 4 — Rust extractor | ✅ done | `fceec76` |
| 5 — local e2e orchestration | ✅ done | uncommitted |
| 6 — cloud deploy of `wsr/` | ⬜ todo | after local green |

**Test counts:** 42 Python unit/API tests pass by default (`uv run pytest`), 44 with
integration (`-m integration`, Tansu up), 1 e2e (`-m e2e`, Tansu up); 18 Rust unit pass
(`cd wsr && cargo test`). Docs in
`docs/testing/`: `phase-0-1-loader-tests.md`, `phase-2-loader-integration-tests.md`,
`phase-3-frontend-tests.md`, `phase-4-rust-extractor-tests.md`,
`phase-5-local-e2e-tests.md`, `testing-file-inventory.md`.

---

## Phase 0 — Test tooling & scaffolding ✅

- [x] Added `[dependency-groups] dev` to root `pyproject.toml` (`pytest`, `pytest-cov`,
      `httpx`, `requests`, `ruff`) **plus the 3 workspace members** via
      `[tool.uv.sources] = { workspace = true }`, so `uv run pytest` at the root has all
      app deps.
- [x] Added `[tool.pytest.ini_options]`: `addopts = "-m 'not integration and not e2e'"`,
      `testpaths`, `pythonpath = ["."]`, and registered `integration` / `e2e` markers.
- [x] Created `load/tests/`, `frontend/tests/`, `tests/e2e/`.
- [x] Shared fixtures in **repo-root `conftest.py`** (not `tests/conftest.py` — must be
      above all test trees to be visible): `tmp_iceberg`, `iceberg_env`, `make_bar`,
      `make_frame`, `alpaca_fields`. `tmp_iceberg` reuses `bootstrap_iceberg()` and
      **monkeypatches the module-level `ICEBERG_*` constants** (bound at import time).
- [x] Added a smoke test (`tests/test_scaffolding.py`) for the harness itself.
- [x] **Commit:** `test: add pytest scaffolding, dev deps, and shared fixtures` (`52d1f43`)

## Phase 1 — Loader: refactor for testability + unit tests ✅

- [x] Extracted `project_frame(batch)` — derives its field list from
      `PYARROW_SCHEMA.names` (kills the projection/schema drift).
- [x] Extracted `should_flush(num_records, elapsed, batch_size, batch_interval)`.
- [x] Left `flush()` / `bootstrap_iceberg()` as-is.
- [x] Unit tests: `test_projection.py`, `test_should_flush.py` (parametrized truth table),
      `test_flush.py` (append + error path via a fake table), `test_metrics.py`.
- [x] **Commit:** `test(load): extract pure helpers; unit-test projection, flush, metrics`
      (`af4d1cd`)

## Phase 2 — Loader: integration test (real Kafka + Iceberg) ✅

- [x] Extracted `run_consumer(consumer, table, metrics, logger, stop, ...)` from `main()`
      (caller owns the consumer lifecycle so the test can inspect committed offsets).
- [x] `load/tests/test_consume_integration.py` (`@pytest.mark.integration`): produces
      frames to `$KAFKA_BROKER`, runs the consumer, asserts rows in Iceberg + at-least-once
      offset commit, and that a fresh catalog load sees the committed snapshot.
- [x] Verified against local Tansu (`docker run … ghcr.io/tansu-io/tansu:0.6.0`).
- [x] **Commit:** `test(load): add kafka-to-iceberg consume-loop integration test` (`8feb497`)

## Phase 3 — Frontend: Python API tests ✅

- [x] `frontend/tests/{conftest.py,test_api.py}` — `seeded_client` / `empty_client`
      `TestClient` fixtures. Warehouse seeded **through the loader's `project_frame`+`flush`**.
- [x] Retargets `iceberg_client` (monkeypatch constants + `_catalog.cache_clear()`) and
      resets `bars._symbols_cache` per test.
- [x] Mocks `logging_client.*`; asserts graceful degradation when `GCP_PROJECT_ID` unset.
- [x] Covered all 5 routes incl. `/bars` validation (422s) and `/pipeline/metrics` bounds.
- [x] Fixed a conftest-name collision by exposing `alpaca_fields` as a fixture (don't
      `import conftest`).
- [x] **Commit:** `test(frontend): API route tests over fixture iceberg with mocked logging`
      (`dfda24f`)

## Phase 4 — Rust extractor: unit tests ✅

- [x] `config.rs`: refactored `from_env` → pure `from_getter(closure)` (avoids `unsafe`
      env mutation in edition 2024 — **no `serial_test` needed**). Tests: defaults,
      missing-required, CSV symbols, overrides, invalid-number, `redact()`, Debug-no-leak.
- [x] `ws.rs`: extracted `classify_frame` + `FrameClass` from `expect` (pure). Tests:
      connected/authenticated/subscription match; error→Fatal (w/ + w/o code); type/msg
      mismatch; data-bar-during-handshake mismatch.
- [x] `metrics.rs`: `snapshot()` defaults + reflects atomic updates; `iso()` null/string.
- [x] `cargo test` (18 pass), `cargo clippy`, `cargo fmt --check` all clean.
- [x] **Commit:** `test(wsr): unit-test config, frame classification, and metrics` (`fceec76`)

## Phase 5 — Local e2e orchestration ✅

- [x] `docker-compose.yml` at repo root: a `tansu` service
      (`ghcr.io/tansu-io/tansu:0.6.0`, `--storage-engine memory://`, port 9092) so the local
      broker is one command. (Loader/frontend stay `uv run` for fast iteration; optionally add
      profiles for them later.)
- [x] `Makefile` targets: `up`/`down` (compose), `test` (fast unit, default marker filter),
      `test-integration` (`-m integration`, brings up tansu), `e2e` (`-m e2e`),
      `lint` (`ruff` for Python if desired + `cargo fmt/clippy` for Rust), `smoke`
      (existing `scripts/peek_kafka.py` / `query_iceberg.py` / `inspect_frontend_api.py`).
- [x] `tests/e2e/test_pipeline_e2e.py`, marked `@pytest.mark.e2e`: with tansu up, run the
      **synthetic generator** for a few seconds → run the loader (`run_consumer`) until rows
      appear → drive the **frontend `TestClient`** against the same warehouse and assert
      `/api/symbols` and `/api/bars` return the generated symbols/bars. One assertion chain
      proving extractor-shape→Kafka→Iceberg→API end to end.
- [x] Update `CLAUDE.md` / `README.md` Commands with `make` targets and the e2e procedure;
      add a `docs/runbooks/` note only if recovery steps exceed one manual step.
- [ ] **Commit:** `test(e2e): docker-compose + Makefile + full local pipeline e2e test`

## Phase 6 — Cloud deployment of the Rust extractor (after local is green)

Outline only; execute after Phases 0–5 pass locally. Because all config is env-driven, the
**same integration/e2e suite validates cloud** by exporting cloud env (`KAFKA_BROKER=<vm_ip>:9092`,
prod `ICEBERG_*`, `GCP_PROJECT_ID`).
- [ ] Add a Terraform module wiring `wsr/` as a Cloud Run **Job** (reuse the
      `extractor-job` pattern in `terraform/`): SA + IAM, Alpaca creds via Secret Manager →
      env (`ALPACA_KEY`/`ALPACA_SECRET`), `KAFKA_BROKER` from `tansu_broker` output, and a
      Cloud Scheduler trigger (market-hours start, watchdog/`DATA_IDLE_TIMEOUT` stop) mirroring
      the existing `scheduler` module.
- [ ] Build/push the `wsr` image via `scripts/build_and_push.sh`, `terraform apply`, then run
      the integration suite against cloud env vars as the deployment gate.

---

## Verification

- **Fast unit suite (default):** `make test` → `uv run pytest -m "not integration and not e2e"`
  and `cd wsr && cargo test`. Green with no Docker/GCP.
- **Integration:** `make up && make test-integration` — loader consume-loop against local Tansu
  writes to a pytest temp sqlite Iceberg warehouse and commits offsets.
- **End-to-end:** `make e2e` — synthetic generator → Tansu → loader → Iceberg → frontend API
  returns the generated bars using a pytest temp warehouse, isolated from `./warehouse`.
- **Manual monitorable run:** `make up`, run `load/subscriber.py` with
  `ICEBERG_CATALOG_URI=sqlite:///./warehouse/catalog.db` and `ICEBERG_WAREHOUSE=./warehouse`,
  then run the synthetic generator. This persists Parquet data and Iceberg metadata under
  `./warehouse` until explicitly removed.
- **Rust quality gate:** `cd wsr && cargo test && cargo clippy && cargo fmt --check`.
- **Rust real-cred smoke (manual, market hours):** export Alpaca creds + `KAFKA_BROKER`,
  `cargo run --release`, confirm frames via `scripts/peek_kafka.py` and rows via
  `scripts/query_iceberg.py`.
- **Cloud parity (Phase 6):** re-run `-m integration` with cloud env vars; same asserts must pass.

## Critical files

- `pyproject.toml` (root) — dev dependency-group + pytest config/markers.
- `load/subscriber.py` — extract `project_frame`, `should_flush`, `run_consumer` (behavior-preserving).
- `load/tests/*`, `frontend/tests/*`, `conftest.py`, `tests/e2e/test_pipeline_e2e.py`.
- `wsr/src/{config.rs,ws.rs,metrics.rs}` — `#[cfg(test)]` modules; `classify_frame` refactor in `ws.rs`.
- `wsr/Cargo.toml` — `[dev-dependencies]` (`serial_test`).
- `docker-compose.yml`, `Makefile` (new, repo root).
- `CLAUDE.md` / `README.md` — document `make` targets + e2e procedure.

## Reused existing code (do not reinvent)

- `load.subscriber.bootstrap_iceberg()` / `flush()` — fixtures and tests build on these.
- `frontend.app.iceberg_client.get_table()` + `routes/bars.py`, `routes/pipeline.py` — tested as-is.
- `extract/helpers/synthetic_stock_generator.py` — the e2e data source.
- `scripts/{peek_kafka,query_iceberg,inspect_frontend_api}.py` — `make smoke` wraps these.
