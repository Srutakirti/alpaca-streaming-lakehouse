# Testing — Phases 0 & 1 (scaffolding + loader unit tests)

This documents the test infrastructure and the loader's unit tests built in
Phases 0–1 of `TESTING_PLAN.md`. It captures the *why* behind the setup so future
tests slot in without rediscovering the same constraints.

Scope here: the test **harness** (Phase 0) and the **loader** (`load/subscriber.py`)
unit tests (Phase 1). Loader integration tests (Phase 2), the frontend (Phase 3),
the Rust extractor (Phase 4), and end-to-end (Phase 5) are documented separately
as those phases land.

---

## How to run

```bash
uv run pytest                 # default: fast unit tests only (integration/e2e deselected)
uv run pytest -v              # one line per test, in logical order
uv run pytest -vv             # full assert diffs on failure
uv run pytest -v -s --log-cli-level=INFO   # also stream app logs from inside tests
uv run pytest --cov=load --cov-report=term-missing   # coverage for the loader
```

The default run needs **no Docker and no GCP**. Expected today: `26 passed, 2 deselected`.

---

## Phase 0 — test tooling & scaffolding

### What was added

| File | Purpose |
|---|---|
| `pyproject.toml` (root) | `[dependency-groups] dev`, `[tool.uv.sources]`, `[tool.pytest.ini_options]` |
| `conftest.py` (repo root) | shared fixtures + data factory + run-order hook |
| `tests/test_scaffolding.py` | smoke test for the harness itself |
| `load/tests/`, `frontend/tests/`, `tests/e2e/` | test trees |

### Key decisions

- **Dev environment pulls in the workspace members.** The root `dev` group lists
  `pytest`, `pytest-cov`, `httpx`, `requests`, `ruff` **and** the three workspace
  members (`extract`, `load`, `frontend`) via `[tool.uv.sources] = { workspace = true }`.
  That is what lets a single `uv run pytest` at the repo root import `load.subscriber`,
  `frontend.app.*`, and the synthetic generator (and their transitive deps:
  confluent-kafka, pyiceberg, pyarrow, fastapi). `dev` is uv's default group, so
  `uv sync` / `uv run` install it automatically.

- **Shared fixtures live in the repo-root `conftest.py`, not `tests/conftest.py`.**
  A `conftest.py` only applies to its own directory and below. Because the test trees
  are spread across `load/tests/`, `frontend/tests/`, and `tests/`, the shared
  fixtures must sit at the repo root to be visible to all of them.

- **`pythonpath = ["."]`** puts the repo root on `sys.path` so `import load.subscriber`
  and `import frontend.app.*` resolve. `load` and `extract` have no `__init__.py`
  (implicit namespace packages); `frontend` is a regular package.

- **Markers + default filter make integration/e2e opt-in.**
  ```toml
  addopts = "-m 'not integration and not e2e'"
  markers = ["integration: needs a live Kafka broker", "e2e: full pipeline"]
  ```
  Plain `uv run pytest` runs only unit tests. `-m integration` / `-m e2e` / `-m ''`
  opt back in. Note pytest's `-m` **replaces** the default expression rather than
  ANDing with it — that is precisely why opting in works.

### Fixtures (in `conftest.py`)

- `make_bar(...)` — one Alpaca bar object (the 8 schema fields `T,S,o,h,l,c,v,t`);
  `**extra` adds fields the loader must drop.
- `make_frame(n, symbol, ...)` — a frame (JSON array of `n` bars), each with a
  distinct minute timestamp.
- `iceberg_env(tmp_path)` — sets the `ICEBERG_*` env vars at a per-test sqlite
  catalog + local-FS warehouse; returns the mapping.
- `tmp_iceberg` — a bootstrapped, empty `alpaca.bars` Iceberg table on that tmp
  warehouse. **It reuses the real `load.subscriber.bootstrap_iceberg()`** rather than
  reimplementing catalog setup, so tests exercise the production bootstrap path.

  > **Gotcha encoded here:** `subscriber`'s `ICEBERG_*` values are module-level
  > constants bound from the environment **at import time**. Setting env vars in a
  > fixture *after* import would not redirect `bootstrap_iceberg()`. So `tmp_iceberg`
  > also `monkeypatch.setattr`s those constants on the module object. Any future
  > fixture that needs to retarget Iceberg must do the same (or we refactor
  > `bootstrap_iceberg()` to take parameters).

### Deterministic, logical run order

`conftest.py` defines `pytest_collection_modifyitems`, which sorts the collected
tests by the `_RUN_ORDER` tuple so they execute in pipeline-data-flow order
regardless of filename or `testpaths`:

```
scaffolding -> projection -> should_flush -> flush -> metrics
            -> consume_integration -> api (frontend) -> pipeline_e2e
```

The sort is **stable** (definition order preserved within a file). New test files
join the sequence by adding their stem to `_RUN_ORDER`. This is for **readability of
results**, not fail-fast: every test still runs and reports (we deliberately did not
add `-x`).

### The scaffolding smoke test (`tests/test_scaffolding.py`)

A "smoke test" for the **harness itself** — it tests the test infrastructure, not the
pipeline. It is a cheap tripwire so that when a *real* test later fails, you can trust
the failure points at the code under test, not a broken fixture/path/env. It checks:

1. `load`, `frontend.app`, and `extract.helpers` all import from a root run
   (proves the dev env + sources + pythonpath are wired), and the loader's schema
   field order is exactly `T,S,o,h,l,c,v,t`.
2. `tmp_iceberg` yields a usable, **empty** table (proves the fixture + per-test
   isolation work — no leakage from `./warehouse/` or prior runs).
3. `make_frame` produces well-formed bars (the factory most other tests build on).

---

## Phase 1 — loader refactor + unit tests

### Behavior-preserving refactor of `load/subscriber.py`

`main()` was a single function with logic inlined in the consume loop. Three pure-ish
pieces were extracted so they can be unit-tested in isolation. **Runtime behavior of
`main()` is unchanged** — it now calls these instead of inlining them.

| Extracted | What it is | Note |
|---|---|---|
| `project_frame(batch)` | decoded frame → list of schema-projected records | Field list derived from `PYARROW_SCHEMA.names` — removes the old "update the projection dict too" drift when a schema field changes |
| `should_flush(num_records, elapsed, batch_size, batch_interval)` | the flush-trigger predicate | Pure function: `full OR (interval elapsed AND records buffered)` |
| `run_consumer(consumer, table, metrics, logger, stop, ...)` | the poll loop extracted from `main()` (added in Phase 2) | Caller owns the consumer lifecycle so tests can inspect committed offsets |

`flush()` and `bootstrap_iceberg()` were left as-is (already testable; `flush` takes
its dependencies as arguments).

### Tests (`load/tests/`)

- **`test_projection.py`** — projection keeps only the 8 schema fields, drops extra
  Alpaca fields (`n`, `vw`, …), fills missing fields with `None`, projects every bar
  in a multi-bar frame, handles the empty frame, and stays in sync with
  `PYARROW_SCHEMA`.
- **`test_should_flush.py`** — a parametrized truth table for the trigger: size
  boundary is inclusive (`>= batch_size`), interval boundary is inclusive
  (`>= batch_interval`), and an **empty buffer never flushes** even past the interval
  (no empty commits).
- **`test_flush.py`** — `flush()` appends a known batch to `tmp_iceberg` and the rows
  read back equal the input; the empty batch is a no-op returning `True`; a forced
  append error is caught, returns `False`, and increments `iceberg_append_errors`
  (using a fake table whose `.append()` raises); repeated flushes accumulate metrics
  and rows.
- **`test_metrics.py`** — `_Metrics.snapshot()` has the expected keys, the
  `alpaca-loader` component name, sane defaults, and reflects updates with the
  duration rounded to 1 dp.

### Testing patterns used (and why)

- **Pure functions over inline logic** — projection and the flush decision are now
  testable without Kafka or Iceberg: fast, deterministic, no I/O.
- **Reuse production code in fixtures** — `tmp_iceberg` calls the real
  `bootstrap_iceberg()`; tests assert against the same schema the loader writes.
- **Real Iceberg in unit tests, but hermetic** — `flush()` is tested against an
  actual sqlite-backed Iceberg table in a tmp dir (not a mock), so the Arrow→Iceberg
  append path is genuinely exercised while staying isolated and fast.
- **Fakes only at the failure boundary** — the append-error test injects a tiny fake
  table that raises, rather than mocking pyiceberg internals, to assert the
  error-handling contract (`return False` + error counter) without brittleness.
- **Parametrized boundaries** — `should_flush` is checked at and around each
  threshold, where off-by-one bugs live.

---

## Cross-cutting notes / gotchas

- **Same suite, local or cloud.** Tests read connection config from the *same env
  vars the apps use* (`KAFKA_BROKER`, `ICEBERG_*`, `GCP_PROJECT_ID`), defaulting to
  local. Pointing the suite at cloud later is an env change, not a code change.
- **Pre-existing Pylance warnings in `subscriber.py`** (e.g. `NewTopic` import path,
  `msg.error()` typing) predate this work and were left untouched.
- **Integration tests need a broker.** They are deselected by default. Start one with
  the documented Tansu `docker run` (Phase 5 wraps this in `make up`) and run
  `KAFKA_BROKER=localhost:9092 uv run pytest -m integration`.
- **Adding a schema field** now touches: `PYARROW_SCHEMA`, `ICEBERG_TABLE_SCHEMA`
  (both in `load/subscriber.py`). `project_frame` follows `PYARROW_SCHEMA`
  automatically; the projection test asserts the two stay aligned.

## Commit trail

| Commit | Phase |
|---|---|
| `test: add pytest scaffolding, dev deps, fixtures` | 0 |
| `test(load): extract pure helpers + unit tests for projection/flush/metrics` | 1 |
| `test(load): kafka->iceberg consume-loop integration test` | 2 |
