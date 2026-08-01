"""Shared pytest fixtures for the pipeline test suite.

Lives at the repo root (not under tests/) so the fixtures are visible to every
test tree: load/tests/, frontend/tests/, and tests/e2e/.

The Iceberg fixtures build a throwaway sqlite catalog + local-FS warehouse under
a per-test tmp dir. They reuse load.subscriber.bootstrap_iceberg() (the same code
the loader runs in production) and point it at the tmp warehouse, so tests exercise
the real bootstrap path rather than a parallel reimplementation.
"""
from typing import Callable

import pytest

# Run tests in a deterministic, logical order that follows the pipeline's data
# flow: harness sanity first, then a frame's journey (project -> decide flush ->
# flush -> metrics), then the same wired to a real broker, then the frontend API,
# then the full end-to-end. Files not listed keep their collected position at the
# end. Within a file, definition order is preserved (the sort below is stable).
_RUN_ORDER = (
    "test_scaffolding",         # test harness itself
    "test_projection",          # frame -> schema-projected records
    "test_should_flush",        # batching decision
    "test_flush",               # batch -> Iceberg append
    "test_metrics",             # metrics snapshot
    "test_consume_integration", # Kafka -> loader -> Iceberg (integration)
    "test_api",                 # frontend JSON API (Phase 3)
    "test_pipeline_e2e",        # full pipeline end-to-end (Phase 5)
)


def pytest_collection_modifyitems(items: list) -> None:
    def rank(item) -> int:
        for i, key in enumerate(_RUN_ORDER):
            if key in item.nodeid:
                return i
        return len(_RUN_ORDER)

    items.sort(key=rank)

# The 8 fields of the Alpaca bar frame == the Iceberg schema (T,S,o,h,l,c,v,t).
ALPACA_FIELDS = ["T", "S", "o", "h", "l", "c", "v", "t"]

# Iceberg module-level constants overridden per test so bootstrap targets tmp_path.
_ICEBERG_KEYS = (
    "ICEBERG_CATALOG_URI",
    "ICEBERG_WAREHOUSE",
    "ICEBERG_NAMESPACE",
    "ICEBERG_TABLE",
)


def _make_bar(
    symbol: str = "AAPL",
    t: str = "2026-06-27T14:30:00Z",
    o: float = 100.0,
    h: float = 101.0,
    l: float = 99.0,  # noqa: E741 - Alpaca/Iceberg field name
    c: float = 100.5,
    v: int = 1000,
    type_: str = "b",
    **extra,
) -> dict:
    """One Alpaca bar object. `extra` lets tests add fields the loader must drop."""
    bar = {"T": type_, "S": symbol, "o": o, "h": h, "l": l, "c": c, "v": v, "t": t}
    bar.update(extra)
    return bar


@pytest.fixture
def alpaca_fields() -> list:
    return list(ALPACA_FIELDS)


@pytest.fixture
def make_bar() -> Callable[..., dict]:
    return _make_bar


@pytest.fixture
def make_frame(make_bar) -> Callable[..., list]:
    """Build a frame (JSON array of bars), giving each bar a distinct minute `t`."""
    def _frame(n: int = 1, symbol: str = "AAPL", base_minute: int = 30, **kw) -> list:
        return [
            make_bar(symbol=symbol, t=f"2026-06-27T14:{base_minute + i:02d}:00Z", **kw)
            for i in range(n)
        ]
    return _frame


@pytest.fixture
def iceberg_env(tmp_path, monkeypatch) -> dict:
    """Set the ICEBERG_* env vars at a per-test sqlite catalog + warehouse.

    Returns the mapping so callers can build a second reader (e.g. the frontend)
    against the same warehouse.
    """
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    env = {
        "ICEBERG_CATALOG_URI": f"sqlite:///{warehouse}/catalog.db",
        "ICEBERG_WAREHOUSE": str(warehouse),
        "ICEBERG_NAMESPACE": "alpaca",
        "ICEBERG_TABLE": "bars",
    }
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    return env


@pytest.fixture
def tmp_iceberg(iceberg_env, monkeypatch):
    """A bootstrapped alpaca.bars Iceberg table backed by the per-test tmp warehouse.

    bootstrap_iceberg() reads load.subscriber's module-level constants, which were
    bound from env at import time, so we override them on the module object here.
    """
    from load import subscriber

    for key in _ICEBERG_KEYS:
        monkeypatch.setattr(subscriber, key, iceberg_env[key])
    return subscriber.bootstrap_iceberg()
