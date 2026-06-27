"""Unit tests for project_frame: decoded frame -> schema-projected records."""
from load import subscriber
from load.subscriber import SCHEMA_FIELDS, project_frame


def test_keeps_only_schema_fields_and_drops_extras(make_bar):
    bar = make_bar(symbol="AAPL", n=12345, vw=100.1, extra_field="drop me")
    [record] = project_frame([bar])

    assert set(record) == set(SCHEMA_FIELDS)
    assert "n" not in record and "vw" not in record and "extra_field" not in record


def test_values_pass_through_unchanged(make_bar):
    bar = make_bar(symbol="TSLA", o=1.0, h=2.0, l=0.5, c=1.5, v=42, t="2026-06-27T14:31:00Z")
    [record] = project_frame([bar])

    assert record == {
        "T": "b", "S": "TSLA", "o": 1.0, "h": 2.0,
        "l": 0.5, "c": 1.5, "v": 42, "t": "2026-06-27T14:31:00Z",
    }


def test_missing_schema_field_becomes_none():
    bar = {"S": "NVDA", "c": 5.0}  # most fields absent
    [record] = project_frame([bar])

    assert record["S"] == "NVDA"
    assert record["c"] == 5.0
    assert record["T"] is None and record["o"] is None and record["v"] is None


def test_projects_every_bar_in_a_multi_bar_frame(make_frame):
    frame = make_frame(n=4, symbol="AAPL")
    records = project_frame(frame)

    assert len(records) == 4
    assert [r["t"] for r in records] == [b["t"] for b in frame]


def test_empty_frame_yields_no_records():
    assert project_frame([]) == []


def test_schema_fields_match_pyarrow_schema():
    # Guards the single-source-of-truth: projection follows PYARROW_SCHEMA.
    assert SCHEMA_FIELDS == subscriber.PYARROW_SCHEMA.names
    assert SCHEMA_FIELDS == ["T", "S", "o", "h", "l", "c", "v", "t"]
