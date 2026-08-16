#!/usr/bin/env python3
"""Publish one bounded Alpaca-compatible synthetic batch to a running broker."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from gce_hadoop_catalog.config import LocalSettings
from gce_hadoop_catalog.runtime import ensure_topic, publish
from gce_hadoop_catalog.synthetic_producer import default_start, generate_bars


def main() -> None:
    settings = LocalSettings.from_environment()
    bars = generate_bars(start=default_start(), periods=12)
    ensure_topic(settings)
    publish(settings, bars)
    print(f"published_bars={len(bars)}")


if __name__ == "__main__":
    main()
