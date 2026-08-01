import logging

from frontend.app.routes import pipeline


def test_iceberg_freshness_uses_file_stats(iceberg_env, tmp_iceberg, make_frame, monkeypatch):
    from frontend.app import iceberg_client
    from load.subscriber import _Metrics, flush, project_frame

    for key in ("ICEBERG_CATALOG_URI", "ICEBERG_WAREHOUSE", "ICEBERG_NAMESPACE", "ICEBERG_TABLE"):
        monkeypatch.setattr(iceberg_client, key, iceberg_env[key])
    iceberg_client._catalog.cache_clear()

    records = project_frame(make_frame(n=3, symbol="AAPL"))
    assert flush(records, tmp_iceberg, _Metrics(), logging.getLogger("seed"))

    result = pipeline._iceberg_freshness()

    assert result["snapshot_count"] >= 1
    assert result["row_count"] == 3
    assert result["latest_record_t_source"] == "file_stats"
    assert "latest_record_t" in result


def test_iceberg_freshness_falls_back_when_file_stats_are_truncated(
    iceberg_env,
    tmp_iceberg,
    make_frame,
    monkeypatch,
):
    from frontend.app import iceberg_client
    from load.subscriber import _Metrics, flush, project_frame

    for key in ("ICEBERG_CATALOG_URI", "ICEBERG_WAREHOUSE", "ICEBERG_NAMESPACE", "ICEBERG_TABLE"):
        monkeypatch.setattr(iceberg_client, key, iceberg_env[key])
    iceberg_client._catalog.cache_clear()

    records = project_frame(make_frame(n=3, symbol="AAPL"))
    assert flush(records, tmp_iceberg, _Metrics(), logging.getLogger("seed"))

    original = pipeline._latest_record_t_from_file_stats

    def truncated_stats(table):
        _latest, row_count = original(table)
        return "2026-07-16T20:5:", row_count

    monkeypatch.setattr(pipeline, "_latest_record_t_from_file_stats", truncated_stats)

    result = pipeline._iceberg_freshness()

    assert result["row_count"] == 3
    assert result["latest_record_t_source"] == "max_t_scan"
    assert result["latest_record_t"] == "2026-06-27T14:32:00Z"
