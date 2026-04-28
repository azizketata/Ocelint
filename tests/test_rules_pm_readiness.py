"""Tests for OCEL-P rules."""

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
from ocelint.rules.pm_readiness import P001, P002, P003, P004, P005, P006, P008


def _make_log(
    *,
    events: pd.DataFrame | None = None,
    objects: pd.DataFrame | None = None,
    relations_e2o: pd.DataFrame | None = None,
    attribute_decls: pd.DataFrame | None = None,
) -> OcelLog:
    def empty(cols: tuple[str, ...]) -> pd.DataFrame:
        return pd.DataFrame({c: pd.Series(dtype="object") for c in cols})

    return OcelLog(
        events=events if events is not None else empty(EVENTS_COLUMNS),
        objects=objects if objects is not None else empty(OBJECTS_COLUMNS),
        relations_e2o=relations_e2o if relations_e2o is not None else empty(E2O_COLUMNS),
        relations_o2o=empty(O2O_COLUMNS),
        event_types=empty(EVENT_TYPES_COLUMNS),
        object_types=empty(OBJECT_TYPES_COLUMNS),
        attribute_decls=attribute_decls if attribute_decls is not None else empty(ATTRIBUTE_DECLS_COLUMNS),
        source_format="json",
        source_path=Path("test.json"),
    )


def test_p001_clean() -> None:
    events = pd.DataFrame([{"eid": "e1", "etype": "T", "timestamp": "t", "attrs": {}}])
    e2o = pd.DataFrame([{"eid": "e1", "oid": "o1", "qualifier": "q"}])
    log = _make_log(events=events, relations_e2o=e2o)
    assert list(P001.check(log)) == []


def test_p001_fires_on_zero_e2o_event_type() -> None:
    events = pd.DataFrame([
        {"eid": "e1", "etype": "Heartbeat", "timestamp": "t", "attrs": {}},
        {"eid": "e2", "etype": "Heartbeat", "timestamp": "t", "attrs": {}},
        {"eid": "e3", "etype": "Order", "timestamp": "t", "attrs": {}},
    ])
    e2o = pd.DataFrame([{"eid": "e3", "oid": "o1", "qualifier": "q"}])
    log = _make_log(events=events, relations_e2o=e2o)
    violations = list(P001.check(log))
    assert len(violations) == 1
    assert "Heartbeat" in violations[0].message


def test_p002_fires_on_disconnected_subgraph() -> None:
    events = pd.DataFrame([
        {"eid": "e1", "etype": "A", "timestamp": "t", "attrs": {}},
        {"eid": "e2", "etype": "B", "timestamp": "t", "attrs": {}},
    ])
    objects = pd.DataFrame([
        {"oid": "o1", "otype": "X", "attrs": {}},
        {"oid": "o2", "otype": "Y", "attrs": {}},
        {"oid": "o3", "otype": "Z", "attrs": {}},
    ])
    e2o = pd.DataFrame([
        {"eid": "e1", "oid": "o1", "qualifier": "q"},
        {"eid": "e1", "oid": "o2", "qualifier": "q"},
        {"eid": "e2", "oid": "o3", "qualifier": "q"},
    ])
    log = _make_log(events=events, objects=objects, relations_e2o=e2o)
    violations = list(P002.check(log))
    assert len(violations) == 1


def test_p003_fires_when_under_threshold() -> None:
    events = pd.DataFrame([
        {"eid": f"e{i}", "etype": "T", "timestamp": "t", "attrs": {}} for i in range(5)
    ])
    objects = pd.DataFrame([{"oid": "o1", "otype": "Refund", "attrs": {}}])
    e2o = pd.DataFrame([{"eid": f"e{i}", "oid": "o1", "qualifier": "q"} for i in range(5)])
    log = _make_log(events=events, objects=objects, relations_e2o=e2o)
    violations = list(P003.check(log))
    assert len(violations) == 1
    assert "Refund" in violations[0].message


def test_p004_fires_on_inconsistent_start() -> None:
    """Different objects of same type have different first events."""
    events = []
    e2o = []
    for i in range(10):
        # Each object i has different first event type
        first_type = f"Start{i % 5}"  # 5 different types of first events
        events.append({"eid": f"first-{i}", "etype": first_type,
                       "timestamp": f"2026-01-01T00:00:{i:02}", "attrs": {}})
        events.append({"eid": f"end-{i}", "etype": "Done",
                       "timestamp": f"2026-02-01T00:00:{i:02}", "attrs": {}})
        e2o.append({"eid": f"first-{i}", "oid": f"o{i}", "qualifier": "q"})
        e2o.append({"eid": f"end-{i}", "oid": f"o{i}", "qualifier": "q"})
    objects = pd.DataFrame([{"oid": f"o{i}", "otype": "Shipment", "attrs": {}} for i in range(10)])
    log = _make_log(events=pd.DataFrame(events), objects=objects, relations_e2o=pd.DataFrame(e2o))
    violations = list(P004.check(log))
    assert len(violations) == 1
    assert "Shipment" in violations[0].message


def test_p005_fires_on_inconsistent_end() -> None:
    events = []
    e2o = []
    for i in range(10):
        last_type = f"End{i % 5}"
        events.append({"eid": f"start-{i}", "etype": "Begin",
                       "timestamp": f"2026-01-01T00:00:{i:02}", "attrs": {}})
        events.append({"eid": f"last-{i}", "etype": last_type,
                       "timestamp": f"2026-02-01T00:00:{i:02}", "attrs": {}})
        e2o.append({"eid": f"start-{i}", "oid": f"o{i}", "qualifier": "q"})
        e2o.append({"eid": f"last-{i}", "oid": f"o{i}", "qualifier": "q"})
    objects = pd.DataFrame([{"oid": f"o{i}", "otype": "Case", "attrs": {}} for i in range(10)])
    log = _make_log(events=pd.DataFrame(events), objects=objects, relations_e2o=pd.DataFrame(e2o))
    violations = list(P005.check(log))
    assert len(violations) == 1
    assert "Case" in violations[0].message


def test_p006_fires_on_coincident_events() -> None:
    events = pd.DataFrame([
        {"eid": "e1", "etype": "A", "timestamp": "2026-01-01T00:00:00Z", "attrs": {}},
        {"eid": "e2", "etype": "B", "timestamp": "2026-01-01T00:00:00Z", "attrs": {}},
        {"eid": "e3", "etype": "C", "timestamp": "2026-01-01T00:00:00Z", "attrs": {}},
    ])
    objects = pd.DataFrame([
        {"oid": "o1", "otype": "X",
         "attrs": {"status": [("2026-01-01T00:00:00Z", "open")]}}
    ])
    e2o = pd.DataFrame([
        {"eid": "e1", "oid": "o1", "qualifier": "q"},
        {"eid": "e2", "oid": "o1", "qualifier": "q"},
        {"eid": "e3", "oid": "o1", "qualifier": "q"},
    ])
    log = _make_log(events=events, objects=objects, relations_e2o=e2o)
    violations = list(P006.check(log))
    assert len(violations) == 1


def test_p008_fires_on_uniform_attribute() -> None:
    decls = pd.DataFrame([{"scope": "object", "type_name": "X",
                           "attribute_name": "status", "attribute_type": "string"}])
    objects = pd.DataFrame([
        {"oid": f"o{i}", "otype": "X",
         "attrs": {"status": [("t", "active")]}}
        for i in range(5)
    ])
    log = _make_log(objects=objects, attribute_decls=decls)
    violations = list(P008.check(log))
    assert len(violations) == 1
    assert "active" in violations[0].message
