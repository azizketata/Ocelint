"""Tests for ocelint.loader."""

from __future__ import annotations

import json
import sqlite3
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


# --- load (XML) ------------------------------------------------------------


_XML_EMPTY = """<?xml version='1.0' encoding='UTF-8'?>
<log>
  <object-types/>
  <event-types/>
  <objects/>
  <events/>
</log>"""


_XML_POPULATED = """<?xml version='1.0' encoding='UTF-8'?>
<log>
  <object-types>
    <object-type name="Order">
      <attributes>
        <attribute name="amount" type="float"/>
      </attributes>
    </object-type>
  </object-types>
  <event-types>
    <event-type name="Create Order">
      <attributes>
        <attribute name="user" type="string"/>
      </attributes>
    </event-type>
  </event-types>
  <objects>
    <object id="o1" type="Order">
      <attributes>
        <attribute name="amount" time="2026-01-01T00:00:00Z">99.5</attribute>
        <attribute name="amount" time="2026-02-01T00:00:00Z">120.0</attribute>
      </attributes>
      <objects>
        <relationship object-id="o2" qualifier="contains"/>
      </objects>
    </object>
    <object id="o2" type="Order">
      <attributes/>
    </object>
  </objects>
  <events>
    <event id="e1" type="Create Order" time="2026-01-01T00:00:00Z">
      <attributes>
        <attribute name="user">alice</attribute>
      </attributes>
      <objects>
        <relationship object-id="o1" qualifier="creates"/>
      </objects>
    </event>
  </events>
</log>"""


def test_load_xml_empty(tmp_path: Path) -> None:
    p = tmp_path / "log.xml"
    p.write_text(_XML_EMPTY, encoding="utf-8")
    log = load(p)

    assert log.source_format == "xml"
    assert len(log.events) == 0
    assert len(log.objects) == 0


def test_load_xml_minimal_populated(tmp_path: Path) -> None:
    p = tmp_path / "log.xml"
    p.write_text(_XML_POPULATED, encoding="utf-8")
    log = load(p)

    assert log.source_format == "xml"
    assert len(log.events) == 1
    assert log.events.iloc[0]["eid"] == "e1"
    assert log.events.iloc[0]["timestamp"] == "2026-01-01T00:00:00Z"
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
        ("2026-01-01T00:00:00Z", "99.5"),
        ("2026-02-01T00:00:00Z", "120.0"),
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


def test_load_xml_malformed(tmp_path: Path) -> None:
    p = tmp_path / "bad.xml"
    p.write_text("<log><events>", encoding="utf-8")
    with pytest.raises(ParseError, match="invalid XML"):
        load(p)


def test_load_xml_wrong_root(tmp_path: Path) -> None:
    p = tmp_path / "wrong.xml"
    p.write_text("<?xml version='1.0'?><notlog/>", encoding="utf-8")
    with pytest.raises(ParseError, match="root element"):
        load(p)


def test_load_xml_event_missing_required_attr(tmp_path: Path) -> None:
    payload = """<?xml version='1.0'?>
<log>
  <object-types/>
  <event-types/>
  <objects/>
  <events>
    <event id="e1" type="Foo">
      <attributes/>
      <objects/>
    </event>
  </events>
</log>"""
    p = tmp_path / "log.xml"
    p.write_text(payload, encoding="utf-8")
    with pytest.raises(ParseError, match="@time"):
        load(p)


def test_load_xml_missing_top_level(tmp_path: Path) -> None:
    payload = "<?xml version='1.0'?><log><object-types/><event-types/><objects/></log>"
    p = tmp_path / "log.xml"
    p.write_text(payload, encoding="utf-8")
    with pytest.raises(ParseError, match="<events>"):
        load(p)


# --- load (SQLite) ---------------------------------------------------------


def _build_sqlite_log(
    path: Path,
    *,
    extra_event_attr: bool = True,
    skip_per_type: str | None = None,
) -> None:
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE event (ocel_id TEXT, ocel_type TEXT);
        CREATE TABLE object (ocel_id TEXT, ocel_type TEXT);
        CREATE TABLE event_object (
            ocel_event_id TEXT, ocel_object_id TEXT, ocel_qualifier TEXT
        );
        CREATE TABLE object_object (
            ocel_source_id TEXT, ocel_target_id TEXT, ocel_qualifier TEXT
        );
        CREATE TABLE event_map_type (ocel_type TEXT, ocel_type_map TEXT);
        CREATE TABLE object_map_type (ocel_type TEXT, ocel_type_map TEXT);
        """
    )
    cur.execute("INSERT INTO event_map_type VALUES (?, ?)", ("Create Order", "CreateOrder"))
    cur.execute("INSERT INTO object_map_type VALUES (?, ?)", ("Order", "Order"))
    cur.execute("INSERT INTO event VALUES (?, ?)", ("e1", "Create Order"))
    cur.execute("INSERT INTO object VALUES (?, ?)", ("o1", "Order"))
    cur.execute("INSERT INTO object VALUES (?, ?)", ("o2", "Order"))
    cur.execute(
        "INSERT INTO event_object VALUES (?, ?, ?)", ("e1", "o1", "creates")
    )
    cur.execute(
        "INSERT INTO object_object VALUES (?, ?, ?)", ("o1", "o2", "contains")
    )

    if skip_per_type != "event":
        if extra_event_attr:
            cur.execute(
                'CREATE TABLE event_CreateOrder (ocel_id TEXT, ocel_time TEXT, "user" TEXT)'
            )
            cur.execute(
                'INSERT INTO event_CreateOrder VALUES (?, ?, ?)',
                ("e1", "2026-01-01 00:00:00", "alice"),
            )
        else:
            cur.execute(
                "CREATE TABLE event_CreateOrder (ocel_id TEXT, ocel_time TEXT)"
            )
            cur.execute(
                "INSERT INTO event_CreateOrder VALUES (?, ?)",
                ("e1", "2026-01-01 00:00:00"),
            )

    if skip_per_type != "object":
        cur.execute(
            "CREATE TABLE object_Order ("
            "ocel_id TEXT, ocel_time TEXT, ocel_changed_field TEXT, amount REAL"
            ")"
        )
        cur.executemany(
            "INSERT INTO object_Order VALUES (?, ?, ?, ?)",
            [
                ("o1", "2026-01-01 00:00:00", None, 99.5),
                ("o1", "2026-02-01 00:00:00", "amount", 120.0),
                ("o2", "2026-01-01 00:00:00", None, 50.0),
            ],
        )

    conn.commit()
    conn.close()


def test_load_sqlite_minimal_populated(tmp_path: Path) -> None:
    p = tmp_path / "log.sqlite"
    _build_sqlite_log(p)
    log = load(p)

    assert log.source_format == "sqlite"
    assert len(log.events) == 1
    assert log.events.iloc[0].to_dict() == {
        "eid": "e1",
        "etype": "Create Order",
        "timestamp": "2026-01-01 00:00:00",
        "attrs": {"user": "alice"},
    }
    assert len(log.objects) == 2
    o1_attrs = log.objects[log.objects["oid"] == "o1"].iloc[0]["attrs"]
    assert o1_attrs["amount"] == [
        ("2026-01-01 00:00:00", 99.5),
        ("2026-02-01 00:00:00", 120.0),
    ]

    assert len(log.relations_e2o) == 1
    assert log.relations_e2o.iloc[0].to_dict() == {
        "eid": "e1",
        "oid": "o1",
        "qualifier": "creates",
    }
    assert len(log.relations_o2o) == 1
    assert log.relations_o2o.iloc[0].to_dict() == {
        "source_oid": "o1",
        "target_oid": "o2",
        "qualifier": "contains",
    }
    assert len(log.event_types) == 1
    assert len(log.object_types) == 1
    assert len(log.attribute_decls) == 2


def test_load_sqlite_event_type_without_attrs(tmp_path: Path) -> None:
    p = tmp_path / "log.sqlite"
    _build_sqlite_log(p, extra_event_attr=False)
    log = load(p)
    assert log.events.iloc[0]["attrs"] == {}
    assert (log.attribute_decls["scope"] == "event").sum() == 0


def test_load_sqlite_missing_required_table(tmp_path: Path) -> None:
    p = tmp_path / "log.sqlite"
    conn = sqlite3.connect(p)
    conn.execute("CREATE TABLE event (ocel_id TEXT, ocel_type TEXT)")
    conn.commit()
    conn.close()
    with pytest.raises(ParseError, match=r"missing required OCEL 2\.0 tables"):
        load(p)


def test_load_sqlite_missing_per_type_table(tmp_path: Path) -> None:
    p = tmp_path / "log.sqlite"
    _build_sqlite_log(p, skip_per_type="event")
    with pytest.raises(ParseError, match="event_map_type references missing table"):
        load(p)


def test_load_sqlite_not_a_database(tmp_path: Path) -> None:
    p = tmp_path / "fake.sqlite"
    p.write_bytes(b"this is plainly not sqlite content")
    with pytest.raises(ParseError):
        load(p)


def test_load_sqlite_object_in_core_missing_from_per_type(tmp_path: Path) -> None:
    """Object listed in core 'object' but absent from object_<Type> table."""
    p = tmp_path / "log.sqlite"
    _build_sqlite_log(p)
    conn = sqlite3.connect(p)
    conn.execute("INSERT INTO object VALUES (?, ?)", ("o_ghost", "Order"))
    conn.commit()
    conn.close()

    log = load(p)
    oids = log.objects["oid"].tolist()
    assert "o_ghost" in oids
    ghost = log.objects[log.objects["oid"] == "o_ghost"].iloc[0]
    assert ghost["attrs"] == {}
    assert any("o_ghost" in w and "missing from per-type" in w for w in log.parse_warnings)


def test_load_sqlite_object_in_per_type_missing_from_core(tmp_path: Path) -> None:
    """Object listed in object_<Type> but absent from core 'object' table."""
    p = tmp_path / "log.sqlite"
    _build_sqlite_log(p)
    conn = sqlite3.connect(p)
    conn.execute(
        "INSERT INTO object_Order VALUES (?, ?, ?, ?)",
        ("o_orphan", "2026-03-01 00:00:00", None, 42.0),
    )
    conn.commit()
    conn.close()

    log = load(p)
    oids = log.objects["oid"].tolist()
    assert "o_orphan" in oids
    orphan = log.objects[log.objects["oid"] == "o_orphan"].iloc[0]
    assert orphan["attrs"] == {"amount": [("2026-03-01 00:00:00", 42.0)]}
    assert any(
        "o_orphan" in w and "missing from core" in w for w in log.parse_warnings
    )


def test_load_sqlite_event_type_mismatch(tmp_path: Path) -> None:
    """Core 'event' says one type, per-type table is for a different type."""
    p = tmp_path / "log.sqlite"
    _build_sqlite_log(p)
    conn = sqlite3.connect(p)
    conn.execute("UPDATE event SET ocel_type = 'Wrong Type' WHERE ocel_id = 'e1'")
    conn.execute(
        "INSERT INTO event_map_type VALUES (?, ?)", ("Wrong Type", "WrongType")
    )
    conn.execute("CREATE TABLE event_WrongType (ocel_id TEXT, ocel_time TEXT)")
    conn.commit()
    conn.close()

    log = load(p)
    e1 = log.events[log.events["eid"] == "e1"].iloc[0]
    assert e1["etype"] == "Wrong Type"
    assert e1["timestamp"] is None
    assert e1["attrs"] == {}
    assert any(
        "e1" in w and "does not match" in w for w in log.parse_warnings
    )


def test_load_sqlite_duplicate_id_in_core(tmp_path: Path) -> None:
    """Duplicate eid in core 'event' must be preserved (S001 will catch it later)."""
    p = tmp_path / "log.sqlite"
    _build_sqlite_log(p)
    conn = sqlite3.connect(p)
    conn.execute("INSERT INTO event VALUES (?, ?)", ("e1", "Create Order"))
    conn.commit()
    conn.close()

    log = load(p)
    assert (log.events["eid"] == "e1").sum() == 2
