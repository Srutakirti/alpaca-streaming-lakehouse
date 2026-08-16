#!/usr/bin/env python3
"""Run one bounded local Tansu SQLite -> HadoopCatalog flow."""

from gce_hadoop_catalog.config import LocalSettings
from gce_hadoop_catalog.runtime import run_bounded
from gce_hadoop_catalog.synthetic_producer import default_start, generate_bars


def main() -> None:
    settings = LocalSettings.from_environment()
    bars = generate_bars(start=default_start(), periods=12)
    result = run_bounded(settings, bars)
    print(f"inserted_rows={result.inserted_rows} committed_windows={result.committed_windows}")


if __name__ == "__main__":
    main()
