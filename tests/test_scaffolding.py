"""Smoke test for the test scaffolding itself.

Confirms the dev environment can import the workspace members and that the
tmp_iceberg fixture produces a usable, empty Iceberg table.
"""
import pyarrow as pa


def test_members_importable(alpaca_fields):
    from load import subscriber
    from frontend.app import iceberg_client  # noqa: F401
    from extract.helpers import synthetic_stock_generator  # noqa: F401

    assert subscriber.PYARROW_SCHEMA.names == alpaca_fields


def test_tmp_iceberg_is_empty_and_usable(tmp_iceberg):
    arrow = tmp_iceberg.scan().to_arrow()
    assert isinstance(arrow, pa.Table)
    assert len(arrow) == 0


def test_make_frame_factory(make_frame, alpaca_fields):
    frame = make_frame(n=3, symbol="TSLA")
    assert len(frame) == 3
    assert {b["S"] for b in frame} == {"TSLA"}
    assert all(set(alpaca_fields).issubset(b) for b in frame)
