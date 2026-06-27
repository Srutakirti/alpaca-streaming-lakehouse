"""Fixtures for the frontend API tests.

Builds a FastAPI TestClient whose Iceberg catalog points at the same per-test tmp
warehouse the loader writes to, seeded with a known set of bars via the loader's
own write path (project_frame + flush). This keeps the read side (frontend) and
write side (loader) on one schema and one warehouse, exactly as in production.

Like the loader, frontend.app.iceberg_client reads its ICEBERG_* values into
module-level constants at import time and caches the catalog with lru_cache, so we
monkeypatch the constants and clear the cache (and the symbols cache) per test.
"""
import logging

import pytest
from fastapi.testclient import TestClient

# The known dataset every seeded test asserts against.
SEED = {"AAPL": 3, "TSLA": 2}  # symbol -> bar count
SEED_TOTAL = sum(SEED.values())
# make_frame assigns minute-incrementing timestamps from 14:30; the latest bar is
# AAPL's 3rd (14:32). Both symbols start at 14:30.
LATEST_T = "2026-06-27T14:32:00Z"
_ICEBERG_KEYS = ("ICEBERG_CATALOG_URI", "ICEBERG_WAREHOUSE", "ICEBERG_NAMESPACE", "ICEBERG_TABLE")


@pytest.fixture
def seed_info() -> dict:
    """Expected facts about the seeded dataset, for assertions."""
    return {"total": SEED_TOTAL, "latest_t": LATEST_T, "symbols": sorted(SEED)}


def _point_frontend_at(iceberg_env, monkeypatch):
    """Redirect the frontend's catalog at the tmp warehouse and reset its caches."""
    from frontend.app import iceberg_client
    from frontend.app.routes import bars

    for key in _ICEBERG_KEYS:
        monkeypatch.setattr(iceberg_client, key, iceberg_env[key])
    iceberg_client._catalog.cache_clear()
    bars._symbols_cache = ([], 0.0)


@pytest.fixture
def seeded_client(iceberg_env, tmp_iceberg, make_frame, monkeypatch):
    """TestClient over a warehouse seeded with SEED bars (AAPL x3, TSLA x2)."""
    from load.subscriber import _Metrics, flush, project_frame

    records = []
    for symbol, n in SEED.items():
        records += project_frame(make_frame(n=n, symbol=symbol))
    assert flush(records, tmp_iceberg, _Metrics(), logging.getLogger("seed"))

    _point_frontend_at(iceberg_env, monkeypatch)

    from frontend.app.main import app
    client = TestClient(app)
    yield client

    from frontend.app import iceberg_client
    from frontend.app.routes import bars
    iceberg_client._catalog.cache_clear()
    bars._symbols_cache = ([], 0.0)


@pytest.fixture
def empty_client(iceberg_env, tmp_iceberg, monkeypatch):
    """TestClient over an empty (bootstrapped, unseeded) warehouse."""
    _point_frontend_at(iceberg_env, monkeypatch)

    from frontend.app.main import app
    client = TestClient(app)
    yield client

    from frontend.app import iceberg_client
    from frontend.app.routes import bars
    iceberg_client._catalog.cache_clear()
    bars._symbols_cache = ([], 0.0)
