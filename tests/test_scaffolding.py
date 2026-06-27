"""Smoke test for the test scaffolding itself.

Confirms the dev environment can import the workspace members and that the
tmp_iceberg fixture produces a usable, empty Iceberg table.
"""
import pyarrow as pa

from conftest import ALPACA_FIELDS


def test_members_importable():
    from load import subscriber
    from frontend.app import iceberg_client  # noqa: F401
    from extract.helpers import synthetic_stock_generator  # noqa: F401

    assert subscriber.PYARROW_SCHEMA.names == ALPACA_FIELDS


def test_tmp_iceberg_is_empty_and_usable(tmp_iceberg):
    arrow = tmp_iceberg.scan().to_arrow()
    assert isinstance(arrow, pa.Table)
    assert len(arrow) == 0


def test_make_frame_factory(make_frame):
    frame = make_frame(n=3, symbol="TSLA")
    assert len(frame) == 3
    assert {b["S"] for b in frame} == {"TSLA"}
    assert all(set(ALPACA_FIELDS).issubset(b) for b in frame)
