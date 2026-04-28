"""OCEL 2.0 loader: format detection + parsers."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd
from lxml import etree

from ocelint.model import (
    ATTRIBUTE_DECLS_COLUMNS,
    E2O_COLUMNS,
    EVENT_TYPES_COLUMNS,
    EVENTS_COLUMNS,
    O2O_COLUMNS,
    OBJECT_TYPES_COLUMNS,
    OBJECTS_COLUMNS,
    OcelLog,
    SourceFormat,
)

_EXTENSION_FORMATS: dict[str, SourceFormat] = {
    ".json": "json",
    ".jsonocel": "json",
    ".xml": "xml",
    ".xmlocel": "xml",
    ".sqlite": "sqlite",
    ".sqlite3": "sqlite",
    ".db": "sqlite",
    ".sqliteocel": "sqlite",
}

_SQLITE_MAGIC = b"SQLite format 3\x00"


class ParseError(Exception):
    """Raised when a file cannot be parsed as a well-formed OCEL 2.0 log."""

    def __init__(self, message: str, *, path: Path, location: str | None = None) -> None:
        self.path = path
        self.location = location
        prefix = f"{path}" if location is None else f"{path}:{location}"
        super().__init__(f"{prefix}: {message}")


def detect_format(path: Path) -> SourceFormat:
    """Detect OCEL format from extension, falling back to magic bytes."""
    if not path.exists():
        raise ParseError("file does not exist", path=path)
    if path.is_dir():
        raise ParseError("path is a directory, not a file", path=path)

    suffix = path.suffix.lower()
    if suffix in _EXTENSION_FORMATS:
        return _EXTENSION_FORMATS[suffix]

    try:
        with path.open("rb") as f:
            head = f.read(16)
    except OSError as e:
        raise ParseError(f"could not read file: {e}", path=path) from e

    if head.startswith(_SQLITE_MAGIC):
        return "sqlite"
    stripped = head.lstrip(b" \t\r\n\xef\xbb\xbf")
    if stripped.startswith(b"<"):
        return "xml"
    if stripped.startswith((b"{", b"[")):
        return "json"

    raise ParseError("could not detect format from extension or magic bytes", path=path)


def load(path: Path | str, *, format: SourceFormat | None = None) -> OcelLog:
    """Load an OCEL 2.0 log from disk. Auto-detects format unless overridden."""
    path = Path(path)
    fmt = format if format is not None else detect_format(path)

    if fmt == "json":
        return _load_json(path)
    if fmt == "xml":
        return _load_xml(path)
    if fmt == "sqlite":
        return _load_sqlite(path)
    raise ParseError(f"unsupported format: {fmt!r}", path=path)


def _load_json(path: Path) -> OcelLog:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ParseError(f"invalid JSON: {e.msg}", path=path, location=f"line {e.lineno}") from e
    except OSError as e:
        raise ParseError(f"could not read file: {e}", path=path) from e

    if not isinstance(data, dict):
        raise ParseError("top-level JSON must be an object", path=path)

    try:
        event_types_data = _require_list(data, "eventTypes", path)
        object_types_data = _require_list(data, "objectTypes", path)
        events_data = _require_list(data, "events", path)
        objects_data = _require_list(data, "objects", path)
    except KeyError as e:
        raise ParseError(f"missing required top-level key: {e.args[0]!r}", path=path) from e

    event_types = pd.DataFrame(
        [{"name": _require_str(et, "name", path, "eventTypes")} for et in event_types_data],
        columns=list(EVENT_TYPES_COLUMNS),
    )
    object_types = pd.DataFrame(
        [{"name": _require_str(ot, "name", path, "objectTypes")} for ot in object_types_data],
        columns=list(OBJECT_TYPES_COLUMNS),
    )

    decl_rows: list[dict[str, str]] = []
    for et in event_types_data:
        for attr in et.get("attributes", []) or []:
            decl_rows.append(
                {
                    "scope": "event",
                    "type_name": et["name"],
                    "attribute_name": _require_str(attr, "name", path, "eventTypes.attributes"),
                    "attribute_type": _require_str(attr, "type", path, "eventTypes.attributes"),
                }
            )
    for ot in object_types_data:
        for attr in ot.get("attributes", []) or []:
            decl_rows.append(
                {
                    "scope": "object",
                    "type_name": ot["name"],
                    "attribute_name": _require_str(attr, "name", path, "objectTypes.attributes"),
                    "attribute_type": _require_str(attr, "type", path, "objectTypes.attributes"),
                }
            )
    attribute_decls = pd.DataFrame(decl_rows, columns=list(ATTRIBUTE_DECLS_COLUMNS))

    event_rows: list[dict[str, Any]] = []
    e2o_rows: list[dict[str, Any]] = []
    for idx, ev in enumerate(events_data):
        loc = f"events[{idx}]"
        event_rows.append(
            {
                "eid": _require_str(ev, "id", path, loc),
                "etype": _require_str(ev, "type", path, loc),
                "timestamp": _require_str(ev, "time", path, loc),
                "attrs": {a["name"]: a.get("value") for a in ev.get("attributes", []) or []},
            }
        )
        for rel in ev.get("relationships", []) or []:
            e2o_rows.append(
                {
                    "eid": ev["id"],
                    "oid": _require_str(rel, "objectId", path, f"{loc}.relationships"),
                    "qualifier": _require_str(rel, "qualifier", path, f"{loc}.relationships"),
                }
            )
    events = pd.DataFrame(event_rows, columns=list(EVENTS_COLUMNS))
    relations_e2o = pd.DataFrame(e2o_rows, columns=list(E2O_COLUMNS))

    object_rows: list[dict[str, Any]] = []
    o2o_rows: list[dict[str, Any]] = []
    for idx, obj in enumerate(objects_data):
        loc = f"objects[{idx}]"
        attrs_by_name: dict[str, list[tuple[str, Any]]] = {}
        for a in obj.get("attributes", []) or []:
            name = _require_str(a, "name", path, f"{loc}.attributes")
            time = _require_str(a, "time", path, f"{loc}.attributes")
            attrs_by_name.setdefault(name, []).append((time, a.get("value")))
        object_rows.append(
            {
                "oid": _require_str(obj, "id", path, loc),
                "otype": _require_str(obj, "type", path, loc),
                "attrs": attrs_by_name,
            }
        )
        for rel in obj.get("relationships", []) or []:
            o2o_rows.append(
                {
                    "source_oid": obj["id"],
                    "target_oid": _require_str(rel, "objectId", path, f"{loc}.relationships"),
                    "qualifier": _require_str(rel, "qualifier", path, f"{loc}.relationships"),
                }
            )
    objects = pd.DataFrame(object_rows, columns=list(OBJECTS_COLUMNS))
    relations_o2o = pd.DataFrame(o2o_rows, columns=list(O2O_COLUMNS))

    return OcelLog(
        events=events,
        objects=objects,
        relations_e2o=relations_e2o,
        relations_o2o=relations_o2o,
        event_types=event_types,
        object_types=object_types,
        attribute_decls=attribute_decls,
        source_format="json",
        source_path=path,
    )


def _load_xml(path: Path) -> OcelLog:
    try:
        tree = etree.parse(str(path))
    except etree.XMLSyntaxError as e:
        raise ParseError(f"invalid XML: {e.msg}", path=path, location=f"line {e.lineno}") from e
    except OSError as e:
        raise ParseError(f"could not read file: {e}", path=path) from e

    root = tree.getroot()
    if root.tag != "log":
        raise ParseError(f"root element must be <log>, got <{root.tag}>", path=path)

    object_types_el = _require_child(root, "object-types", path)
    event_types_el = _require_child(root, "event-types", path)
    objects_el = _require_child(root, "objects", path)
    events_el = _require_child(root, "events", path)

    event_type_rows: list[dict[str, str]] = []
    decl_rows: list[dict[str, str]] = []
    for et in event_types_el.iterfind("event-type"):
        name = _require_xml_attr(et, "name", path, "event-type")
        event_type_rows.append({"name": name})
        attrs_el = et.find("attributes")
        if attrs_el is not None:
            for attr in attrs_el.iterfind("attribute"):
                loc = "event-type/attribute"
                decl_rows.append(
                    {
                        "scope": "event",
                        "type_name": name,
                        "attribute_name": _require_xml_attr(attr, "name", path, loc),
                        "attribute_type": _require_xml_attr(attr, "type", path, loc),
                    }
                )
    event_types = pd.DataFrame(event_type_rows, columns=list(EVENT_TYPES_COLUMNS))

    object_type_rows: list[dict[str, str]] = []
    for ot in object_types_el.iterfind("object-type"):
        name = _require_xml_attr(ot, "name", path, "object-type")
        object_type_rows.append({"name": name})
        attrs_el = ot.find("attributes")
        if attrs_el is not None:
            for attr in attrs_el.iterfind("attribute"):
                loc = "object-type/attribute"
                decl_rows.append(
                    {
                        "scope": "object",
                        "type_name": name,
                        "attribute_name": _require_xml_attr(attr, "name", path, loc),
                        "attribute_type": _require_xml_attr(attr, "type", path, loc),
                    }
                )
    object_types = pd.DataFrame(object_type_rows, columns=list(OBJECT_TYPES_COLUMNS))
    attribute_decls = pd.DataFrame(decl_rows, columns=list(ATTRIBUTE_DECLS_COLUMNS))

    event_rows: list[dict[str, Any]] = []
    e2o_rows: list[dict[str, Any]] = []
    for idx, ev in enumerate(events_el.iterfind("event")):
        loc = f"events/event[{idx}]"
        eid = _require_xml_attr(ev, "id", path, loc)
        event_rows.append(
            {
                "eid": eid,
                "etype": _require_xml_attr(ev, "type", path, loc),
                "timestamp": _require_xml_attr(ev, "time", path, loc),
                "attrs": _read_event_attributes(ev, path, loc),
            }
        )
        for rel in _iter_relationships(ev):
            e2o_rows.append(
                {
                    "eid": eid,
                    "oid": _require_xml_attr(rel, "object-id", path, f"{loc}/relationship"),
                    "qualifier": _require_xml_attr(rel, "qualifier", path, f"{loc}/relationship"),
                }
            )
    events = pd.DataFrame(event_rows, columns=list(EVENTS_COLUMNS))
    relations_e2o = pd.DataFrame(e2o_rows, columns=list(E2O_COLUMNS))

    object_rows: list[dict[str, Any]] = []
    o2o_rows: list[dict[str, Any]] = []
    for idx, obj in enumerate(objects_el.iterfind("object")):
        loc = f"objects/object[{idx}]"
        oid = _require_xml_attr(obj, "id", path, loc)
        object_rows.append(
            {
                "oid": oid,
                "otype": _require_xml_attr(obj, "type", path, loc),
                "attrs": _read_object_attributes(obj, path, loc),
            }
        )
        for rel in _iter_relationships(obj):
            o2o_rows.append(
                {
                    "source_oid": oid,
                    "target_oid": _require_xml_attr(rel, "object-id", path, f"{loc}/relationship"),
                    "qualifier": _require_xml_attr(rel, "qualifier", path, f"{loc}/relationship"),
                }
            )
    objects = pd.DataFrame(object_rows, columns=list(OBJECTS_COLUMNS))
    relations_o2o = pd.DataFrame(o2o_rows, columns=list(O2O_COLUMNS))

    return OcelLog(
        events=events,
        objects=objects,
        relations_e2o=relations_e2o,
        relations_o2o=relations_o2o,
        event_types=event_types,
        object_types=object_types,
        attribute_decls=attribute_decls,
        source_format="xml",
        source_path=path,
    )


def _require_child(parent: Any, tag: str, path: Path) -> Any:
    el = parent.find(tag)
    if el is None:
        raise ParseError(f"missing required <{tag}> child of <{parent.tag}>", path=path)
    return el


def _require_xml_attr(el: Any, name: str, path: Path, location: str) -> str:
    value = el.get(name)
    if value is None:
        raise ParseError(f"missing required @{name} on <{el.tag}>", path=path, location=location)
    return str(value)


def _iter_relationships(parent: Any) -> Any:
    objects_el = parent.find("objects")
    if objects_el is None:
        return iter(())
    return objects_el.iterfind("relationship")


def _read_event_attributes(ev: Any, path: Path, location: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    attrs_el = ev.find("attributes")
    if attrs_el is None:
        return out
    for attr in attrs_el.iterfind("attribute"):
        name = _require_xml_attr(attr, "name", path, f"{location}/attribute")
        out[name] = attr.text
    return out


def _read_object_attributes(
    obj: Any, path: Path, location: str
) -> dict[str, list[tuple[str, Any]]]:
    out: dict[str, list[tuple[str, Any]]] = {}
    attrs_el = obj.find("attributes")
    if attrs_el is None:
        return out
    for attr in attrs_el.iterfind("attribute"):
        name = _require_xml_attr(attr, "name", path, f"{location}/attribute")
        time = _require_xml_attr(attr, "time", path, f"{location}/attribute")
        out.setdefault(name, []).append((time, attr.text))
    return out


_SQLITE_REQUIRED_TABLES = frozenset(
    ["event", "object", "event_object", "object_object", "event_map_type", "object_map_type"]
)
_OBJECT_RESERVED_COLUMNS = frozenset(["ocel_id", "ocel_time", "ocel_changed_field"])
_EVENT_RESERVED_COLUMNS = frozenset(["ocel_id", "ocel_time"])
_SQL_TO_OCEL_TYPE = {
    "TEXT": "string",
    "TIMESTAMP": "time",
    "INTEGER": "integer",
    "REAL": "float",
    "BOOLEAN": "boolean",
}


def _load_sqlite(path: Path) -> OcelLog:
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except sqlite3.Error as e:
        raise ParseError(f"could not open database: {e}", path=path) from e

    try:
        try:
            existing_tables = {
                row[0]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
        except sqlite3.DatabaseError as e:
            raise ParseError(f"not a valid SQLite database: {e}", path=path) from e

        missing = _SQLITE_REQUIRED_TABLES - existing_tables
        if missing:
            raise ParseError(
                f"missing required OCEL 2.0 tables: {sorted(missing)}", path=path
            )

        relations_e2o = _sqlite_query(
            conn,
            "SELECT ocel_event_id AS eid, ocel_object_id AS oid, "
            "ocel_qualifier AS qualifier FROM event_object",
            list(E2O_COLUMNS),
        )
        relations_o2o = _sqlite_query(
            conn,
            "SELECT ocel_source_id AS source_oid, ocel_target_id AS target_oid, "
            "ocel_qualifier AS qualifier FROM object_object",
            list(O2O_COLUMNS),
        )

        evt_map = conn.execute(
            "SELECT ocel_type, ocel_type_map FROM event_map_type"
        ).fetchall()
        obj_map = conn.execute(
            "SELECT ocel_type, ocel_type_map FROM object_map_type"
        ).fetchall()

        event_types = pd.DataFrame(
            [{"name": etype} for etype, _ in evt_map], columns=list(EVENT_TYPES_COLUMNS)
        )
        object_types = pd.DataFrame(
            [{"name": otype} for otype, _ in obj_map], columns=list(OBJECT_TYPES_COLUMNS)
        )

        decl_rows: list[dict[str, str]] = []
        parse_warnings: list[str] = []

        per_type_events, decl_e, w_e = _read_event_per_type_tables(
            conn, evt_map, existing_tables, path
        )
        decl_rows.extend(decl_e)
        parse_warnings.extend(w_e)

        per_type_objects, decl_o, w_o = _read_object_per_type_tables(
            conn, obj_map, existing_tables, path
        )
        decl_rows.extend(decl_o)
        parse_warnings.extend(w_o)

        event_rows, w_ev = _reconcile_events(conn, per_type_events)
        parse_warnings.extend(w_ev)
        object_rows, w_ob = _reconcile_objects(conn, per_type_objects)
        parse_warnings.extend(w_ob)

        events = pd.DataFrame(event_rows, columns=list(EVENTS_COLUMNS))
        objects = pd.DataFrame(object_rows, columns=list(OBJECTS_COLUMNS))
        attribute_decls = pd.DataFrame(decl_rows, columns=list(ATTRIBUTE_DECLS_COLUMNS))

        return OcelLog(
            events=events,
            objects=objects,
            relations_e2o=relations_e2o,
            relations_o2o=relations_o2o,
            event_types=event_types,
            object_types=object_types,
            attribute_decls=attribute_decls,
            source_format="sqlite",
            source_path=path,
            parse_warnings=parse_warnings,
        )
    finally:
        conn.close()


def _sqlite_query(
    conn: sqlite3.Connection, sql: str, columns: list[str]
) -> pd.DataFrame:
    cur = conn.execute(sql)
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=columns)


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _ocel_type_from_sql(sql_type: str) -> str:
    return _SQL_TO_OCEL_TYPE.get(sql_type.strip().upper(), sql_type.strip().lower())


def _read_event_per_type_tables(
    conn: sqlite3.Connection,
    evt_map: list[tuple[str, str]],
    existing_tables: set[str],
    path: Path,
) -> tuple[dict[str, tuple[str, Any, dict[str, Any]]], list[dict[str, str]], list[str]]:
    """Returns (eid -> (etype, timestamp, attrs), decl_rows, warnings)."""
    per_type: dict[str, tuple[str, Any, dict[str, Any]]] = {}
    decl_rows: list[dict[str, str]] = []
    warnings: list[str] = []

    for etype, suffix in evt_map:
        table = f"event_{suffix}"
        if table not in existing_tables:
            raise ParseError(
                f"event_map_type references missing table {table!r}",
                path=path,
                location=f"event_map_type[{etype!r}]",
            )

        cols_info = conn.execute(
            f"PRAGMA table_info({_quote_ident(table)})"
        ).fetchall()
        attr_cols = [(c[1], c[2]) for c in cols_info if c[1] not in _EVENT_RESERVED_COLUMNS]
        for name, sql_type in attr_cols:
            decl_rows.append(
                {
                    "scope": "event",
                    "type_name": etype,
                    "attribute_name": name,
                    "attribute_type": _ocel_type_from_sql(sql_type),
                }
            )

        select_cols = ["ocel_id", "ocel_time"] + [c[0] for c in attr_cols]
        sql = (
            f"SELECT {', '.join(_quote_ident(c) for c in select_cols)} "
            f"FROM {_quote_ident(table)}"
        )
        for row in conn.execute(sql).fetchall():
            eid = row[0]
            attrs = {attr_cols[i][0]: row[i + 2] for i in range(len(attr_cols))}
            if eid in per_type:
                warnings.append(
                    f"event {eid!r} appears in multiple per-type tables; keeping {table!r}"
                )
            per_type[eid] = (etype, row[1], attrs)

    return per_type, decl_rows, warnings


def _read_object_per_type_tables(
    conn: sqlite3.Connection,
    obj_map: list[tuple[str, str]],
    existing_tables: set[str],
    path: Path,
) -> tuple[
    dict[str, tuple[str, dict[str, list[tuple[Any, Any]]]]],
    list[dict[str, str]],
    list[str],
]:
    """Returns (oid -> (otype, attrs), decl_rows, warnings)."""
    decl_rows: list[dict[str, str]] = []
    obj_attrs: dict[str, dict[str, list[tuple[Any, Any]]]] = {}
    obj_types: dict[str, str] = {}
    warnings: list[str] = []

    for otype, suffix in obj_map:
        table = f"object_{suffix}"
        if table not in existing_tables:
            raise ParseError(
                f"object_map_type references missing table {table!r}",
                path=path,
                location=f"object_map_type[{otype!r}]",
            )

        cols_info = conn.execute(
            f"PRAGMA table_info({_quote_ident(table)})"
        ).fetchall()
        attr_cols = [(c[1], c[2]) for c in cols_info if c[1] not in _OBJECT_RESERVED_COLUMNS]
        for name, sql_type in attr_cols:
            decl_rows.append(
                {
                    "scope": "object",
                    "type_name": otype,
                    "attribute_name": name,
                    "attribute_type": _ocel_type_from_sql(sql_type),
                }
            )

        col_names = [c[1] for c in cols_info]
        has_changed = "ocel_changed_field" in col_names
        select_cols = ["ocel_id", "ocel_time"]
        if has_changed:
            select_cols.append("ocel_changed_field")
        select_cols.extend(c[0] for c in attr_cols)
        sql = (
            f"SELECT {', '.join(_quote_ident(c) for c in select_cols)} "
            f"FROM {_quote_ident(table)}"
        )
        attr_offset = 3 if has_changed else 2

        for row in conn.execute(sql).fetchall():
            oid = row[0]
            time = row[1]
            existing = obj_types.get(oid)
            if existing is not None and existing != otype:
                warnings.append(
                    f"object {oid!r} appears in multiple per-type tables ({existing!r}, {otype!r})"
                )
            obj_types[oid] = otype
            entry = obj_attrs.setdefault(oid, {})
            changed = row[2] if has_changed else None
            if changed is not None and isinstance(changed, str):
                try:
                    ci = next(i for i, (n, _) in enumerate(attr_cols) if n == changed)
                except StopIteration:
                    continue
                entry.setdefault(changed, []).append((time, row[attr_offset + ci]))
            else:
                for i, (name, _) in enumerate(attr_cols):
                    value = row[attr_offset + i]
                    if value is not None:
                        entry.setdefault(name, []).append((time, value))

    return {oid: (obj_types[oid], obj_attrs.get(oid, {})) for oid in obj_types}, decl_rows, warnings


def _reconcile_events(
    conn: sqlite3.Connection,
    per_type: dict[str, tuple[str, Any, dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    seen: set[str] = set()

    for eid, etype in conn.execute("SELECT ocel_id, ocel_type FROM event").fetchall():
        seen.add(eid)
        pt = per_type.get(eid)
        if pt is None:
            warnings.append(
                f"event {eid!r} listed in core 'event' table but missing from per-type table"
            )
            rows.append({"eid": eid, "etype": etype, "timestamp": None, "attrs": {}})
            continue
        pt_etype, pt_time, pt_attrs = pt
        if pt_etype != etype:
            warnings.append(
                f"event {eid!r}: core type {etype!r} does not match per-type table {pt_etype!r}"
            )
            rows.append({"eid": eid, "etype": etype, "timestamp": None, "attrs": {}})
        else:
            rows.append({"eid": eid, "etype": etype, "timestamp": pt_time, "attrs": pt_attrs})

    for eid, (etype, timestamp, attrs) in per_type.items():
        if eid not in seen:
            warnings.append(
                f"event {eid!r} listed in per-type table but missing from core 'event' table"
            )
            rows.append({"eid": eid, "etype": etype, "timestamp": timestamp, "attrs": attrs})

    return rows, warnings


def _reconcile_objects(
    conn: sqlite3.Connection,
    per_type: dict[str, tuple[str, dict[str, list[tuple[Any, Any]]]]],
) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    seen: set[str] = set()

    for oid, otype in conn.execute("SELECT ocel_id, ocel_type FROM object").fetchall():
        seen.add(oid)
        pt = per_type.get(oid)
        if pt is None:
            warnings.append(
                f"object {oid!r} listed in core 'object' table but missing from per-type table"
            )
            rows.append({"oid": oid, "otype": otype, "attrs": {}})
            continue
        pt_otype, pt_attrs = pt
        if pt_otype != otype:
            warnings.append(
                f"object {oid!r}: core type {otype!r} does not match per-type table {pt_otype!r}"
            )
            rows.append({"oid": oid, "otype": otype, "attrs": {}})
        else:
            rows.append({"oid": oid, "otype": otype, "attrs": pt_attrs})

    for oid, (otype, attrs) in per_type.items():
        if oid not in seen:
            warnings.append(
                f"object {oid!r} listed in per-type table but missing from core 'object' table"
            )
            rows.append({"oid": oid, "otype": otype, "attrs": attrs})

    return rows, warnings


def _require_list(data: dict[str, Any], key: str, path: Path) -> list[Any]:
    if key not in data:
        raise KeyError(key)
    value = data[key]
    if not isinstance(value, list):
        raise ParseError(f"top-level {key!r} must be an array", path=path)
    return value


def _require_str(record: Any, key: str, path: Path, location: str) -> str:
    if not isinstance(record, dict):
        raise ParseError(
            f"expected object, got {type(record).__name__}",
            path=path,
            location=location,
        )
    if key not in record:
        raise ParseError(f"missing required field {key!r}", path=path, location=location)
    value = record[key]
    if not isinstance(value, str):
        raise ParseError(
            f"field {key!r} must be a string, got {type(value).__name__}",
            path=path,
            location=location,
        )
    return value


__all__ = ["ParseError", "detect_format", "load"]
