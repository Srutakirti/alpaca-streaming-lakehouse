from pathlib import Path

import pytest

from gce_hadoop_catalog.spark_catalog import SparkCatalogSettings, settings_from_environment, spark_configuration


def test_local_catalog_configuration_is_hadoop_and_utc() -> None:
    settings = SparkCatalogSettings(warehouse="file:///tmp/warehouse")
    configuration = spark_configuration(settings)

    assert configuration["spark.sql.catalog.alpaca.type"] == "hadoop"
    assert configuration["spark.sql.catalog.alpaca.warehouse"] == "file:///tmp/warehouse"
    assert configuration["spark.sql.session.timeZone"] == "UTC"
    assert configuration["spark.sql.caseSensitive"] == "true"


def test_default_local_warehouse_uses_the_notebook_repository_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("ICEBERG_WAREHOUSE", raising=False)

    settings = settings_from_environment(tmp_path)

    assert settings.warehouse == (tmp_path / ".local-notebook" / "warehouse").as_uri()


def test_gcs_catalog_requires_explicit_switch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ICEBERG_WAREHOUSE", "gs://example/warehouse")
    monkeypatch.delenv("NOTEBOOK_ENABLE_GCS", raising=False)

    with pytest.raises(ValueError, match="NOTEBOOK_ENABLE_GCS"):
        settings_from_environment()


def test_gcs_catalog_uses_the_explicit_connector(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    connector = tmp_path / "gcs-connector.jar"
    connector.touch()
    monkeypatch.setenv("ICEBERG_WAREHOUSE", "gs://example/warehouse")
    monkeypatch.setenv("NOTEBOOK_ENABLE_GCS", "true")
    monkeypatch.setenv("GCS_CONNECTOR_JAR", str(connector))

    configuration = spark_configuration(settings_from_environment())

    assert configuration["spark.hadoop.fs.gs.impl"].endswith("GoogleHadoopFileSystem")
    assert configuration["spark.jars"] == str(connector)
