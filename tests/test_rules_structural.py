"""Tests for OCEL-S rules."""

from __future__ import annotations

from pathlib import Path

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
)
from ocelint.rules.structural import S001, S002, S003, S004, S005, S006, S008


def _make_log(
    *,
    events: pd.DataFrame | None = None,
    objects: pd.DataFrame | None = None,
    event_types: pd.DataFrame | None = None,
    object_types: pd.DataFrame | None = None,
    attribute_decls: pd.DataFrame | None = None,
) -> OcelLog:
    def empty(cols: tuple[str, ...]) -> pd.DataFrame:
        return pd.DataFrame({c: pd.Series(dtype="object") for c in cols})

    return OcelLog(
        events=events if events is not None else empty(EVENTS_COLUMNS),
        objects=objects if objects is not None else empty(OBJECTS_COLUMNS),
        relations_e2o=empty(E2O_COLUMNS),
        relations_o2o=empty(O2O_COLUMNS),
        event_types=event_types if event_types is not None else empty(EVENT_TYPES_COLUMNS),
        object_types=object_types if object_types is not None else empty(OBJECT_TYPES_COLUMNS),
        attribute_decls=attribute_decls if attribute_decls is not None else empty(ATTRIBUTE_DECLS_COLUMNS),
        source_format="json",
        source_path=Path("test.json"),
    )


def test_s001_clean() -> None:
    events = pd.DataFrame(
        [
            {"eid": "e1", "etype": "T", "timestamp": "t", "attrs": {}},
            {"eid": "e2", "etype": "T", "timestamp": "t", "attrs": {}},
        ]
    )
    log = _make_log(events=events)
    assert list(S001.check(log)) == []


def test_s001_detects_duplicates() -> None:
    events = pd.DataFrame(
        [
            {"eid": "e1", "etype": "T", "timestamp": "t", "attrs": {}},
            {"eid": "e1", "etype": "T", "timestamp": "t", "attrs": {}},
            {"eid": "e2", "etype": "T", "timestamp": "t", "attrs": {}},
            {"eid": "e3", "etype": "T", "timestamp": "t", "attrs": {}},
            {"eid": "e3", "etype": "T", "timestamp": "t", "attrs": {}},
            {"eid": "e3", "etype": "T", "timestamp": "t", "attrs": {}},
        ]
    )
    log = _make_log(events=events)
    violations = list(S001.check(log))
    assert len(violations) == 2
    by_loc = {v.location: v for v in violations}
    assert "e1" in by_loc["events[eid=e1]"].message
    assert "2 times" in by_loc["events[eid=e1]"].message
    assert "3 times" in by_loc["events[eid=e3]"].message
    assert all(v.code == "S001" and v.severity == "error" for v in violations)


def test_s001_empty_log() -> None:
    assert list(S001.check(_make_log())) == []


def test_s002_clean() -> None:
    objects = pd.DataFrame(
        [
            {"oid": "o1", "otype": "Order", "attrs": {}},
            {"oid": "o2", "otype": "Order", "attrs": {}},
        ]
    )
    log = _make_log(objects=objects)
    assert list(S002.check(log)) == []


def test_s002_detects_duplicates() -> None:
    objects = pd.DataFrame(
        [
            {"oid": "o1", "otype": "Order", "attrs": {}},
            {"oid": "o1", "otype": "Order", "attrs": {}},
        ]
    )
    log = _make_log(objects=objects)
    violations = list(S002.check(log))
    assert len(violations) == 1
    assert violations[0].code == "S002"
    assert violations[0].severity == "error"
    assert "o1" in violations[0].message


def test_s002_empty_log() -> None:
    assert list(S002.check(_make_log())) == []


# --- S003 case-insensitive ID collision -----------------------------------


def test_s003_clean() -> None:
    events = pd.DataFrame(
        [
            {"eid": "Order-001", "etype": "T", "timestamp": "t", "attrs": {}},
            {"eid": "Order-002", "etype": "T", "timestamp": "t", "attrs": {}},
        ]
    )
    log = _make_log(events=events)
    assert list(S003.check(log)) == []


def test_s003_detects_event_collision() -> None:
    events = pd.DataFrame(
        [
            {"eid": "Order-001", "etype": "T", "timestamp": "t", "attrs": {}},
            {"eid": "order-001", "etype": "T", "timestamp": "t", "attrs": {}},
        ]
    )
    log = _make_log(events=events)
    violations = list(S003.check(log))
    assert len(violations) == 1
    assert violations[0].code == "S003"
    assert violations[0].severity == "warn"
    assert "Order-001" in violations[0].message
    assert "order-001" in violations[0].message


def test_s003_detects_object_collision() -> None:
    objects = pd.DataFrame(
        [
            {"oid": "X", "otype": "Order", "attrs": {}},
            {"oid": "x", "otype": "Order", "attrs": {}},
        ]
    )
    log = _make_log(objects=objects)
    violations = list(S003.check(log))
    assert len(violations) == 1


# --- S004 undeclared event type -------------------------------------------


def test_s004_clean() -> None:
    events = pd.DataFrame([{"eid": "e1", "etype": "Foo", "timestamp": "t", "attrs": {}}])
    event_types = pd.DataFrame([{"name": "Foo"}])
    log = _make_log(events=events, event_types=event_types)
    assert list(S004.check(log)) == []


def test_s004_fires_on_undeclared() -> None:
    events = pd.DataFrame([{"eid": "e1", "etype": "Bar", "timestamp": "t", "attrs": {}}])
    event_types = pd.DataFrame([{"name": "Foo"}])
    log = _make_log(events=events, event_types=event_types)
    violations = list(S004.check(log))
    assert len(violations) == 1
    assert "Bar" in violations[0].message


def test_s004_no_declarations_no_fire() -> None:
    """If event_types is empty AND events use a type, S004 still fires."""
    events = pd.DataFrame([{"eid": "e1", "etype": "Anything", "timestamp": "t", "attrs": {}}])
    log = _make_log(events=events)
    violations = list(S004.check(log))
    assert len(violations) == 1


# --- S005 undeclared object type ------------------------------------------


def test_s005_clean() -> None:
    objects = pd.DataFrame([{"oid": "o1", "otype": "Order", "attrs": {}}])
    object_types = pd.DataFrame([{"name": "Order"}])
    log = _make_log(objects=objects, object_types=object_types)
    assert list(S005.check(log)) == []


def test_s005_fires_on_undeclared() -> None:
    objects = pd.DataFrame([{"oid": "o1", "otype": "Ordr", "attrs": {}}])
    object_types = pd.DataFrame([{"name": "Order"}])
    log = _make_log(objects=objects, object_types=object_types)
    violations = list(S005.check(log))
    assert len(violations) == 1
    assert "Ordr" in violations[0].message


# --- S006 undeclared attribute --------------------------------------------


def test_s006_clean() -> None:
    events = pd.DataFrame(
        [{"eid": "e1", "etype": "Foo", "timestamp": "t", "attrs": {"user": "alice"}}]
    )
    decls = pd.DataFrame(
        [
            {
                "scope": "event",
                "type_name": "Foo",
                "attribute_name": "user",
                "attribute_type": "string",
            }
        ]
    )
    log = _make_log(events=events, attribute_decls=decls)
    assert list(S006.check(log)) == []


def test_s006_fires_on_undeclared_event_attr() -> None:
    events = pd.DataFrame(
        [{"eid": "e1", "etype": "Foo", "timestamp": "t", "attrs": {"unknown": 42}}]
    )
    decls = pd.DataFrame(
        [
            {
                "scope": "event",
                "type_name": "Foo",
                "attribute_name": "user",
                "attribute_type": "string",
            }
        ]
    )
    log = _make_log(events=events, attribute_decls=decls)
    violations = list(S006.check(log))
    assert len(violations) == 1
    assert "unknown" in violations[0].message
    assert "Foo" in violations[0].message


def test_s006_silent_when_no_declarations() -> None:
    events = pd.DataFrame(
        [{"eid": "e1", "etype": "Foo", "timestamp": "t", "attrs": {"x": 1}}]
    )
    log = _make_log(events=events)
    assert list(S006.check(log)) == []


def test_s006_silent_when_type_not_declared() -> None:
    """If a type has no decls at all, S006 stays quiet (S004 handles that)."""
    events = pd.DataFrame(
        [{"eid": "e1", "etype": "Bar", "timestamp": "t", "attrs": {"x": 1}}]
    )
    decls = pd.DataFrame(
        [
            {
                "scope": "event",
                "type_name": "Foo",
                "attribute_name": "user",
                "attribute_type": "string",
            }
        ]
    )
    log = _make_log(events=events, attribute_decls=decls)
    assert list(S006.check(log)) == []


# --- S008 non-ISO-8601 timestamp ------------------------------------------


def test_s008_clean_t_separator() -> None:
    events = pd.DataFrame(
        [{"eid": "e1", "etype": "T", "timestamp": "2026-01-01T00:00:00.000Z", "attrs": {}}]
    )
    log = _make_log(events=events)
    assert list(S008.check(log)) == []


def test_s008_clean_space_separator() -> None:
    events = pd.DataFrame(
        [{"eid": "e1", "etype": "T", "timestamp": "2023-04-03 12:08:18", "attrs": {}}]
    )
    log = _make_log(events=events)
    assert list(S008.check(log)) == []


def test_s008_fires_on_slash_format() -> None:
    events = pd.DataFrame(
        [{"eid": "e1", "etype": "T", "timestamp": "04/28/2026", "attrs": {}}]
    )
    log = _make_log(events=events)
    violations = list(S008.check(log))
    assert len(violations) == 1
    assert "04/28/2026" in violations[0].message


def test_s008_dedupes_same_bad_format() -> None:
    """Many events with the same bad format → one violation."""
    events = pd.DataFrame(
        [
            {"eid": f"e{i}", "etype": "T", "timestamp": "garbage", "attrs": {}}
            for i in range(50)
        ]
    )
    log = _make_log(events=events)
    violations = list(S008.check(log))
    assert len(violations) == 1


def test_s008_skips_none() -> None:
    events = pd.DataFrame(
        [{"eid": "e1", "etype": "T", "timestamp": None, "attrs": {}}]
    )
    log = _make_log(events=events)
    assert list(S008.check(log)) == []
