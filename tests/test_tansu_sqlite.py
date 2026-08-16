from pathlib import Path

import pytest

from gce_hadoop_catalog.config import LocalSettings
from gce_hadoop_catalog.tansu_sqlite import TansuSqlite


def test_tansu_uses_sqlite_and_loopback_listener(tmp_path: Path) -> None:
    settings = LocalSettings(runtime_dir=tmp_path, broker_port=19093)
    broker = TansuSqlite(settings)

    assert broker.storage_uri == "sqlite:///var/lib/tansu/tansu.sqlite"
    assert broker.command == [
        "docker",
        "run",
        "--rm",
        "--name",
        "gce-hadoop-catalog-tansu-19093",
        "--pull=never",
        "--network",
        "host",
        "--volume",
        f"{tmp_path}/tansu:/var/lib/tansu",
        "ghcr.io/tansu-io/tansu:0.6.0",
        "--kafka-listener-url",
        "tcp://127.0.0.1:19093",
        "--kafka-advertised-listener-url",
        "tcp://127.0.0.1:19093",
        "--storage-engine",
        "sqlite:///var/lib/tansu/tansu.sqlite",
    ]


@pytest.mark.integration
def test_tansu_starts_with_sqlite(tmp_path: Path) -> None:
    """The container removes the host GLIBC dependency from broker testing."""
    settings = LocalSettings(runtime_dir=tmp_path, broker_port=19094)
    with TansuSqlite(settings) as broker:
        assert broker.process is not None and broker.process.poll() is None
        assert settings.tansu_sqlite_path.parent.is_dir()
