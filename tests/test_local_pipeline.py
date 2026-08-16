from pathlib import Path

import pytest

from gce_hadoop_catalog.config import LocalSettings
from gce_hadoop_catalog.hadoop_catalog import HadoopCatalog
from gce_hadoop_catalog.runtime import run_bounded
from gce_hadoop_catalog.synthetic_producer import default_start, generate_bars


@pytest.mark.integration
def test_bounded_tansu_sqlite_to_hadoop_catalog(tmp_path: Path) -> None:
    settings = LocalSettings(runtime_dir=tmp_path, broker_port=19095)
    bars = generate_bars(start=default_start(), periods=3)
    result = run_bounded(settings, bars)
    assert result.inserted_rows == 6
    assert result.committed_windows == 1

    catalog = HadoopCatalog(settings)
    try:
        assert catalog.count() == 6
    finally:
        catalog.close()
