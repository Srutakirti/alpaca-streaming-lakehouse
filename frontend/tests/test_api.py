"""API tests for the frontend FastAPI service.

Iceberg-backed routes (/health, /symbols, /bars, the freshness part of
/pipeline/status) run against a seeded tmp warehouse. Cloud-Logging-backed routes
(/pipeline/status extractor+loader, /pipeline/metrics) are tested with the
logging client monkeypatched, plus a graceful-degradation case when GCP is absent.
"""
from frontend.app import logging_client


# --- /api/health ---------------------------------------------------------------

def test_health_ok(seeded_client):
    resp = seeded_client.get("/api/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


# --- /api/symbols --------------------------------------------------------------

def test_symbols_unique_sorted(seeded_client):
    resp = seeded_client.get("/api/symbols")
    assert resp.status_code == 200
    assert resp.json() == ["AAPL", "TSLA"]  # sorted, de-duplicated


def test_symbols_empty_warehouse(empty_client):
    resp = empty_client.get("/api/symbols")
    assert resp.status_code == 200
    assert resp.json() == []


# --- /api/bars -----------------------------------------------------------------

def test_bars_by_symbol(seeded_client):
    resp = seeded_client.get("/api/bars", params={"symbol": "AAPL"})
    assert resp.status_code == 200
    rows = resp.json()
    assert len(rows) == 3
    # Projected to the chart fields only (no S/T).
    assert set(rows[0]) == {"t", "o", "h", "l", "c", "v"}


def test_bars_unknown_symbol_is_empty(seeded_client):
    resp = seeded_client.get("/api/bars", params={"symbol": "ZZZ"})
    assert resp.status_code == 200
    assert resp.json() == []


def test_bars_from_to_range_filter(seeded_client):
    # AAPL bars are at 14:30, 14:31, 14:32 -> from 14:31 keeps the last two.
    resp = seeded_client.get(
        "/api/bars",
        params={"symbol": "AAPL", "from": "2026-06-27T14:31:00Z"},
    )
    assert resp.status_code == 200
    ts = [r["t"] for r in resp.json()]
    assert ts == ["2026-06-27T14:31:00Z", "2026-06-27T14:32:00Z"]

    resp = seeded_client.get(
        "/api/bars",
        params={"symbol": "AAPL", "to": "2026-06-27T14:30:00Z"},
    )
    assert [r["t"] for r in resp.json()] == ["2026-06-27T14:30:00Z"]


def test_bars_limit_caps_rows(seeded_client):
    resp = seeded_client.get("/api/bars", params={"symbol": "AAPL", "limit": 2})
    assert resp.status_code == 200
    assert len(resp.json()) <= 2


def test_bars_limit_over_max_rejected(seeded_client):
    resp = seeded_client.get("/api/bars", params={"symbol": "AAPL", "limit": 10001})
    assert resp.status_code == 422  # Query(le=10000)


def test_bars_requires_symbol(seeded_client):
    resp = seeded_client.get("/api/bars")
    assert resp.status_code == 422


# --- /api/pipeline/status ------------------------------------------------------

def test_pipeline_status_freshness_from_iceberg(seeded_client, seed_info, monkeypatch):
    monkeypatch.setattr(logging_client, "get_last_extractor_metrics", lambda: {"messages_sent": 42})
    monkeypatch.setattr(
        logging_client,
        "get_last_loader_metrics",
        lambda: {"consumer_lag": 0, "latest_record_t": seed_info["latest_t"]},
    )

    body = seeded_client.get("/api/pipeline/status").json()

    assert body["extractor"] == {"messages_sent": 42}
    assert body["loader"] == {"consumer_lag": 0, "latest_record_t": seed_info["latest_t"]}
    ice = body["iceberg"]
    assert ice["row_count"] == seed_info["total"]
    assert ice["snapshot_count"] >= 1
    assert ice["latest_record_t_source"] == "file_stats"
    assert "latest_record_t" in ice
    assert ice["latest_snapshot_ts"] is not None


def test_pipeline_status_degrades_without_gcp(seeded_client, seed_info, monkeypatch):
    # No GCP project -> logging client returns None, but Iceberg freshness still works.
    monkeypatch.setattr(logging_client, "PROJECT_ID", "")
    monkeypatch.setattr(logging_client, "get_last_extractor_metrics", lambda: None)
    monkeypatch.setattr(logging_client, "get_last_loader_metrics", lambda: None)

    body = seeded_client.get("/api/pipeline/status").json()
    assert body["extractor"] is None
    assert body["loader"] is None
    assert body["iceberg"]["row_count"] == seed_info["total"]


def test_pipeline_status_empty_warehouse_freshness(empty_client, monkeypatch):
    monkeypatch.setattr(logging_client, "get_last_extractor_metrics", lambda: None)
    monkeypatch.setattr(logging_client, "get_last_loader_metrics", lambda: None)

    ice = empty_client.get("/api/pipeline/status").json()["iceberg"]
    assert ice["snapshot_count"] == 0
    assert ice["latest_snapshot_ts"] is None
    # No rows -> latest_record_t/row_count are not set by _iceberg_freshness.
    assert "latest_record_t" not in ice


# --- /api/pipeline/metrics -----------------------------------------------------

def test_pipeline_metrics_returns_timeseries(seeded_client, monkeypatch):
    series = [{"consumer_lag": 1, "_ts": "2026-06-27T14:30:00Z"},
              {"consumer_lag": 0, "_ts": "2026-06-27T14:31:00Z"}]
    captured = {}

    def fake_timeseries(component, minutes):
        captured["component"], captured["minutes"] = component, minutes
        return series

    monkeypatch.setattr(logging_client, "get_metrics_timeseries", fake_timeseries)

    resp = seeded_client.get("/api/pipeline/metrics", params={"component": "loader", "minutes": 30})
    assert resp.status_code == 200
    assert resp.json() == series
    assert captured == {"component": "loader", "minutes": 30}


def test_pipeline_metrics_defaults_to_loader_60m(seeded_client, monkeypatch):
    captured = {}
    monkeypatch.setattr(
        logging_client, "get_metrics_timeseries",
        lambda component, minutes: captured.update(component=component, minutes=minutes) or [],
    )
    seeded_client.get("/api/pipeline/metrics")
    assert captured == {"component": "loader", "minutes": 60}


def test_pipeline_metrics_validates_component(seeded_client):
    resp = seeded_client.get("/api/pipeline/metrics", params={"component": "bogus"})
    assert resp.status_code == 422


def test_pipeline_metrics_validates_minutes_bounds(seeded_client):
    assert seeded_client.get("/api/pipeline/metrics", params={"minutes": 0}).status_code == 422
    assert seeded_client.get("/api/pipeline/metrics", params={"minutes": 1441}).status_code == 422
