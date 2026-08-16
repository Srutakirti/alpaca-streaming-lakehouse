import os
from pathlib import Path

import pytest

from gce_hadoop_catalog.config import LocalSettings
from gce_hadoop_catalog.hadoop_catalog import HadoopCatalog
from gce_hadoop_catalog.loader import Loader
from gce_hadoop_catalog.synthetic_producer import default_start, generate_bars


@pytest.mark.integration
def test_hadoop_catalog_writes_and_restart_is_idempotent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    jar = Path.home() / ".ivy2/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.9.2.jar"
    if not jar.is_file() and not os.environ.get("ICEBERG_SPARK_RUNTIME_JAR"):
        pytest.skip("set ICEBERG_SPARK_RUNTIME_JAR to run the Spark integration test")
    monkeypatch.setenv("ICEBERG_SPARK_RUNTIME_JAR", os.environ.get("ICEBERG_SPARK_RUNTIME_JAR", str(jar)))
    settings = LocalSettings(runtime_dir=tmp_path, commit_window_seconds=300)
    catalog = HadoopCatalog(settings)
    bars = generate_bars(start=default_start(), periods=7)
    try:
        assert Loader(catalog).load(bars).inserted_rows == 14
        assert catalog.count() == 14
        assert Loader(catalog).load(bars).inserted_rows == 0
        assert catalog.count() == 14
    finally:
        catalog.close()
