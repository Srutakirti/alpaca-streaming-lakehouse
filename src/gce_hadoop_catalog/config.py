"""Local runtime configuration. Data timestamps are always UTC."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LocalSettings:
    runtime_dir: Path
    namespace: str = "synthetic"
    table: str = "bars"
    topic: str = "synthetic-bars"
    broker_host: str = "127.0.0.1"
    broker_port: int = 19092
    commit_window_seconds: int = 300

    @property
    def broker_url(self) -> str:
        return f"{self.broker_host}:{self.broker_port}"

    @property
    def warehouse_dir(self) -> Path:
        return self.runtime_dir / "warehouse"

    @property
    def tansu_sqlite_path(self) -> Path:
        return self.runtime_dir / "tansu" / "tansu.sqlite"

    @classmethod
    def from_environment(cls) -> "LocalSettings":
        runtime = Path(os.environ.get("PIPELINE_RUNTIME_DIR", ".local-run")).resolve()
        return cls(runtime_dir=runtime)

    def prepare(self) -> None:
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.warehouse_dir.mkdir(parents=True, exist_ok=True)
