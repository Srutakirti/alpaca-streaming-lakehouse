"""Idempotent five-minute window loader for the HadoopCatalog table."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from .hadoop_catalog import HadoopCatalog
from .models import MarketBar
from .time_utils import window_start


@dataclass(frozen=True)
class LoadResult:
    committed_windows: int
    inserted_rows: int


class Loader:
    def __init__(self, catalog: HadoopCatalog) -> None:
        self.catalog = catalog

    def load(self, bars: Iterable[MarketBar]) -> LoadResult:
        grouped: dict[datetime, list[MarketBar]] = defaultdict(list)
        for bar in bars:
            grouped[window_start(bar.event_time, self.catalog.settings.commit_window_seconds)].append(bar)
        inserted_rows = 0
        committed_windows = 0
        for start in sorted(grouped):
            inserted = self._commit_window(grouped[start])
            inserted_rows += inserted
            if inserted:
                committed_windows += 1
        return LoadResult(committed_windows=committed_windows, inserted_rows=inserted_rows)

    def _commit_window(self, bars: list[MarketBar]) -> int:
        self.catalog.ensure_table()
        spark = self.catalog.start()
        before = self.catalog.count()
        inbound = spark.createDataFrame([bar.as_row() for bar in bars]).dropDuplicates(["event_id"])
        inbound.createOrReplaceTempView("incoming_bars")
        spark.sql(
            f"""
            MERGE INTO {self.catalog.identifier} AS target
            USING incoming_bars AS source
            ON target.event_id = source.event_id
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        return self.catalog.count() - before
