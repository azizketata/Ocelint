"""OCEL 2.0 loader: format detection + parsers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

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
        raise NotImplementedError("XML loader not yet implemented (planned for Stage 0)")
    if fmt == "sqlite":
        raise NotImplementedError("SQLite loader not yet implemented (planned for Stage 0)")
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


def _require_list(data: dict[str, Any], key: str, path: Path) -> list[Any]:
    if key not in data:
        raise KeyError(key)
    value = data[key]
    if not isinstance(value, list):
        raise ParseError(f"top-level {key!r} must be an array", path=path)
    return value


def _require_str(record: Any, key: str, path: Path, location: str) -> str:
    if not isinstance(record, dict):
        raise ParseError(f"expected object, got {type(record).__name__}", path=path, location=location)
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
