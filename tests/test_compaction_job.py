from __future__ import annotations

import importlib.util
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace


SCRIPT = Path(__file__).parents[1] / "iceberg-maintenance" / "compact_table.py"
SPEC = importlib.util.spec_from_file_location("compact_table", SCRIPT)
assert SPEC and SPEC.loader
compact_table = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(compact_table)


class Result:
    def __init__(self, row: object) -> None:
        self.row = row

    def first(self) -> object:
        return self.row


class Spark:
    def sql(self, statement: str) -> Result:
        if statement.endswith(".snapshots ORDER BY committed_at DESC LIMIT 1"):
            return Result(
                SimpleNamespace(
                    snapshot_id=123,
                    committed_at=datetime(2026, 9, 12, 16, 0, tzinfo=UTC),
                    summary={"total-records": "42"},
                )
            )
        return Result(SimpleNamespace(data_file_count=3))


def test_snapshot_metrics_calls_dataframe_count() -> None:
    assert compact_table.snapshot_metrics(Spark(), "hadoop.alpaca.bars") == {
        "snapshot_id": "123",
        "committed_at_utc": "2026-09-12T16:00:00Z",
        "total_records": 42,
        "data_file_count": 3,
    }


def test_write_receipt_writes_utf8_bytes_and_closes_stream() -> None:
    class Output:
        written: bytearray | None = None
        closed = False

        def write(self, value: bytearray) -> None:
            self.written = value

        def close(self) -> None:
            self.closed = True

    output = Output()

    class Path:
        def __init__(self, uri: str) -> None:
            self.uri = uri

        def getFileSystem(self, _configuration: object) -> object:
            return SimpleNamespace(create=lambda _path, _overwrite: output)

    spark = SimpleNamespace(
        sparkContext=SimpleNamespace(
            _jvm=SimpleNamespace(org=SimpleNamespace(apache=SimpleNamespace(hadoop=SimpleNamespace(fs=SimpleNamespace(Path=Path))))),
            _jsc=SimpleNamespace(hadoopConfiguration=lambda: object()),
        )
    )

    compact_table.write_receipt(spark, "gs://bucket/receipts/result.json", {"status": "succeeded"})

    assert json.loads(bytes(output.written or b"").decode("UTF-8")) == {"status": "succeeded"}
    assert output.closed
