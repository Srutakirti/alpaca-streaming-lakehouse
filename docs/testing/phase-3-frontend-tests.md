# Testing — Phase 3 (frontend API tests)

This documents the frontend test suite built in Phase 3 of `TESTING_PLAN.md`. The
frontend (`frontend/app/`) is a FastAPI service that **reads** the Iceberg table the
loader writes and reads Cloud Logging for pipeline metrics. These tests exercise all
five JSON routes without GCP, against a seeded throwaway warehouse.

Scope here: the frontend's Python JSON API. The React UI (`frontend/web/`) is out of
scope (decision: Python API tests only). See `phase-0-1-loader-tests.md` for the
shared harness those tests build on.

---

## How to run

```bash
uv run pytest frontend/tests -v        # just the frontend tests
uv run pytest                          # full default suite (frontend included)
```

No Docker and no GCP required. Expected: `16 passed`.

---

## What was added

| File | Purpose |
|---|---|
| `frontend/tests/conftest.py` | `seeded_client` / `empty_client` TestClient fixtures + `seed_info` |
| `frontend/tests/test_api.py` | 16 tests across the 5 routes |

The routes under test (`frontend/app/`):

| Route | Backed by | Tested |
|---|---|---|
| `GET /api/health` | — | status + body |
| `GET /api/symbols` | Iceberg | unique + sorted; empty warehouse |
| `GET /api/bars` | Iceberg | symbol filter, from/to range, limit cap, validation (422) |
| `GET /api/pipeline/status` | Iceberg + Cloud Logging | freshness from real data; logging mocked; degradation |
| `GET /api/pipeline/metrics` | Cloud Logging | timeseries passthrough; param validation |

---

## Key decisions & gotchas

### The frontend reads the same warehouse the loader writes

`seeded_client` builds a per-test `tmp_iceberg` table (the loader's fixture) and seeds
it **through the loader's own write path** — `project_frame()` + `flush()` from
`load.subscriber`. Then it points the frontend's catalog at that same warehouse. So a
single test spans the real write side and the real read side on one schema and one
warehouse, exactly as in production — no hand-built Arrow tables, no schema drift.

The seeded dataset is fixed and asserted against via the `seed_info` fixture:
`AAPL × 3, TSLA × 2` (5 rows), with `make_frame`'s minute-incrementing timestamps so
the latest record is `2026-06-27T14:32:00Z`.

### Retargeting the frontend's Iceberg client (same gotcha as the loader)

`frontend/app/iceberg_client.py` reads `ICEBERG_*` into **module-level constants at
import time** and caches the `SqlCatalog` with `@lru_cache`. So pointing it at the tmp
warehouse requires, per test:

1. `monkeypatch.setattr` the four `ICEBERG_*` constants on the `iceberg_client` module,
2. `iceberg_client._catalog.cache_clear()` — drop the cached catalog so the next
   `get_table()` rebuilds it against the tmp warehouse,
3. reset `bars._symbols_cache = ([], 0.0)` — the `/symbols` route has its own 60s TTL
   cache that would otherwise leak symbols across tests.

`_point_frontend_at()` in the conftest does all three; both the setup and teardown call
it so no state bleeds between tests. (Both loader and frontend use the catalog name
`alpaca_catalog`, so they're reading/writing a compatible catalog.)

### Cloud Logging is mocked, not contacted

`/api/pipeline/status` and `/api/pipeline/metrics` call `frontend.app.logging_client`.
Tests `monkeypatch.setattr` those functions (`get_last_extractor_metrics`,
`get_last_loader_metrics`, `get_metrics_timeseries`) to return canned values — the
routes are validated for shape/passthrough without a GCP project. Because
`pipeline.py` calls them as `logging_client.<fn>()` (module attribute access), patching
the attribute on the module is sufficient.

### Graceful degradation without GCP

`logging_client` short-circuits to `None` / `[]` when `PROJECT_ID` is empty. The
degradation test sets `PROJECT_ID = ""` and asserts `/api/pipeline/status` still returns
`extractor: null`, `loader: null`, **but** the Iceberg-derived freshness
(`row_count`, etc.) is still correct. This proves the dashboard stays useful locally
(no Cloud Logging) — the same code path that runs on a laptop.

---

## Coverage by route

- **`/api/health`** — 200 + `{"status": "ok"}`.
- **`/api/symbols`** — returns `["AAPL", "TSLA"]` (sorted, de-duped); empty warehouse → `[]`.
- **`/api/bars`** — by symbol returns the right count and only the chart fields
  (`t,o,h,l,c,v`); unknown symbol → `[]`; `from`/`to` range filters by ISO timestamp;
  `limit` caps the row count; `limit > 10000` → 422; missing `symbol` → 422.
- **`/api/pipeline/status`** — freshness computed from real seeded data
  (`row_count == 5`, `latest_record_t == 14:32`, `snapshot_count >= 1`); logging mocked;
  degradation without GCP; empty-warehouse case (no snapshots, no `latest_record_t`).
- **`/api/pipeline/metrics`** — passes component+minutes through to the logging client and
  returns its series verbatim; defaults to `loader`/60m; rejects bad `component` (422) and
  out-of-range `minutes` (0 or 1441 → 422).

---

## Testing patterns used (and why)

- **Real read+write path, mocked only at the external boundary** — Iceberg is real
  (seeded via the loader); only Cloud Logging (a GCP network dependency) is mocked.
  Tests catch real schema/query bugs while staying hermetic and offline.
- **TestClient over the actual ASGI app** — `TestClient(frontend.app.main.app)` exercises
  routing, query-param parsing, and FastAPI validation (the 422s) as a real client would.
- **Validation is part of the contract** — the 422 cases assert FastAPI's `Query`
  constraints (`le=10000`, `pattern`, `ge/le`) actually reject bad input.
- **Per-test cache hygiene** — the import-time constants + two caches (`_catalog`,
  `_symbols_cache`) are reset around every test so results are deterministic and isolated.

---

## Cross-cutting note: the conftest-collision fix

Adding `frontend/tests/conftest.py` introduced a **second** `conftest` module. The
scaffolding test had done `from conftest import ALPACA_FIELDS`, which is an anti-pattern
(conftest files shouldn't be imported by name) and now resolved to the wrong module,
breaking collection. Fixed by exposing the value as an `alpaca_fields` **fixture** in the
root `conftest.py` and consuming it via dependency injection. Lesson for future test
trees: share values through fixtures, never `import conftest`.

## Commit trail

| Commit | Phase |
|---|---|
| `test(frontend): FastAPI route tests against fixture Iceberg + mocked logging` | 3 |
