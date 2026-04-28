"""Tests for ocelint.loader."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from ocelint.loader import ParseError, detect_format, load


# --- detect_format ---------------------------------------------------------


def test_detect_format_by_extension(tmp_path: Path) -> None:
    cases: dict[str, str] = {
        "a.json": "json",
        "a.jsonocel": "json",
        "a.JSON": "json",
        "a.xml": "xml",
        "a.xmlocel": "xml",
        "a.sqlite": "sqlite",
        "a.sqlite3": "sqlite",
        "a.db": "sqlite",
    }
    for name, expected in cases.items():
        f = tmp_path / name
        f.write_bytes(b"")
        assert detect_format(f) == expected, name


def test_detect_format_by_magic_bytes_json(tmp_path: Path) -> None:
    f = tmp_path / "noext"
    f.write_text('  \n  {"events": []}', encoding="utf-8")
    assert detect_format(f) == "json"


def test_detect_format_by_magic_bytes_xml(tmp_path: Path) -> None:
    f = tmp_path / "noext"
    f.write_text("<?xml version='1.0'?>\n<log/>", encoding="utf-8")
    assert detect_format(f) == "xml"


def test_detect_format_by_magic_bytes_sqlite(tmp_path: Path) -> None:
    f = tmp_path / "noext"
    f.write_bytes(b"SQLite format 3\x00" + b"\x00" * 16)
    assert detect_format(f) == "sqlite"


def test_detect_format_extension_overrides_when_known(tmp_path: Path) -> None:
    f = tmp_path / "log.json"
    f.write_text("{}", encoding="utf-8")
    assert detect_format(f) == "json"


def test_detect_format_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ParseError, match="does not exist"):
        detect_format(tmp_path / "absent.json")


def test_detect_format_directory(tmp_path: Path) -> None:
    with pytest.raises(ParseError, match="directory"):
        detect_format(tmp_path)


def test_detect_format_unknown(tmp_path: Path) -> None:
    f = tmp_path / "garbage.dat"
    f.write_bytes(b"garbage payload no magic")
    with pytest.raises(ParseError, match="could not detect format"):
        detect_format(f)


# --- load (JSON) -----------------------------------------------------------


def _write_json(tmp_path: Path, payload: dict[str, Any], name: str = "log.json") -> Path:
    p = tmp_path / name
    p.write_text(json.dumps(payload), encoding="utf-8")
    return p


def test_load_json_empty(tmp_path: Path) -> None:
    p = _write_json(tmp_path, {"eventTypes": [], "objectTypes": [], "events": [], "objects": []})
    log = load(p)
    assert log.source_format == "json"
    assert log.source_path == p
    assert len(log.events) == 0
    assert len(log.objects) == 0
    assert len(log.relations_e2o) == 0
    assert len(log.relations_o2o) == 0


def test_load_json_minimal_populated(tmp_path: Path) -> None:
    payload = {
        "eventTypes": [
            {"name": "Create Order", "attributes": [{"name": "user", "type": "string"}]}
        ],
        "objectTypes": [
            {"name": "Order", "attributes": [{"name": "amount", "type": "float"}]}
        ],
        "events": [
            {
                "id": "e1",
                "type": "Create Order",
                "time": "2026-01-01T00:00:00Z",
                "attributes": [{"name": "user", "value": "alice"}],
                "relationships": [{"objectId": "o1", "qualifier": "creates"}],
            }
        ],
        "objects": [
            {
                "id": "o1",
                "type": "Order",
                "attributes": [
                    {"name": "amount", "time": "2026-01-01T00:00:00Z", "value": 99.5},
                    {"name": "amount", "time": "2026-02-01T00:00:00Z", "value": 120.0},
                ],
                "relationships": [{"objectId": "o2", "qualifier": "contains"}],
            },
            {"id": "o2", "type": "Order", "attributes": [], "relationships": []},
        ],
    }
    p = _write_json(tmp_path, payload)
    log = load(p)

    assert len(log.events) == 1
    assert log.events.iloc[0]["eid"] == "e1"
    assert log.events.iloc[0]["attrs"] == {"user": "alice"}

    assert len(log.relations_e2o) == 1
    assert log.relations_e2o.iloc[0].to_dict() == {
        "eid": "e1",
        "oid": "o1",
        "qualifier": "creates",
    }

    assert len(log.objects) == 2
    o1_attrs = log.objects[log.objects["oid"] == "o1"].iloc[0]["attrs"]
    assert o1_attrs["amount"] == [
        ("2026-01-01T00:00:00Z", 99.5),
        ("2026-02-01T00:00:00Z", 120.0),
    ]

    assert len(log.relations_o2o) == 1
    assert log.relations_o2o.iloc[0].to_dict() == {
        "source_oid": "o1",
        "target_oid": "o2",
        "qualifier": "contains",
    }

    assert len(log.event_types) == 1
    assert len(log.object_types) == 1
    assert len(log.attribute_decls) == 2
    decl_scopes = set(log.attribute_decls["scope"].tolist())
    assert decl_scopes == {"event", "object"}


def test_load_json_malformed(tmp_path: Path) -> None:
    p = tmp_path / "bad.json"
    p.write_text("{not valid json", encoding="utf-8")
    with pytest.raises(ParseError, match="invalid JSON"):
        load(p)


def test_load_json_top_level_not_object(tmp_path: Path) -> None:
    p = tmp_path / "arr.json"
    p.write_text("[]", encoding="utf-8")
    with pytest.raises(ParseError, match="top-level"):
        load(p)


def test_load_json_missing_top_level_key(tmp_path: Path) -> None:
    p = _write_json(tmp_path, {"eventTypes": [], "objectTypes": [], "events": []})
    with pytest.raises(ParseError, match="objects"):
        load(p)


def test_load_json_event_missing_required_field(tmp_path: Path) -> None:
    payload = {
        "eventTypes": [],
        "objectTypes": [],
        "events": [{"id": "e1", "type": "Foo"}],
        "objects": [],
    }
    p = _write_json(tmp_path, payload)
    with pytest.raises(ParseError, match="time"):
        load(p)


def test_load_format_override(tmp_path: Path) -> None:
    p = tmp_path / "actually_json.dat"
    p.write_text(json.dumps({"eventTypes": [], "objectTypes": [], "events": [], "objects": []}))
    log = load(p, format="json")
    assert log.source_format == "json"


def test_load_xml_not_implemented(tmp_path: Path) -> None:
    p = tmp_path / "log.xml"
    p.write_text("<?xml version='1.0'?><log/>")
    with pytest.raises(NotImplementedError):
        load(p)


def test_load_sqlite_not_implemented(tmp_path: Path) -> None:
    p = tmp_path / "log.sqlite"
    p.write_bytes(b"SQLite format 3\x00" + b"\x00" * 16)
    with pytest.raises(NotImplementedError):
        load(p)
