"""Tests for OCEL-C (complexity) rules."""

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
from ocelint.rules.complexity import C001, C002, C003, C004, C005, C006, C007


def _make_log(
    *,
    events: pd.DataFrame | None = None,
    objects: pd.DataFrame | None = None,
    relations_e2o: pd.DataFrame | None = None,
    object_types: pd.DataFrame | None = None,
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
        object_types=object_types if object_types is not None else empty(OBJECT_TYPES_COLUMNS),
        attribute_decls=attribute_decls if attribute_decls is not None else empty(ATTRIBUTE_DECLS_COLUMNS),
        source_format="json",
        source_path=Path("test.json"),
    )


def test_c001_clean_under_threshold() -> None:
    log = _make_log(object_types=pd.DataFrame([{"name": f"T{i}"} for i in range(10)]))
    assert list(C001.check(log)) == []


def test_c001_fires_over_threshold() -> None:
    log = _make_log(object_types=pd.DataFrame([{"name": f"T{i}"} for i in range(25)]))
    violations = list(C001.check(log))
    assert len(violations) == 1
    assert "25 object types" in violations[0].message


def test_c002_clean_uniform_fanout() -> None:
    events = pd.DataFrame(
        [{"eid": f"e{i}", "etype": "T", "timestamp": "t", "attrs": {}} for i in range(10)]
    )
    e2o = pd.DataFrame(
        [{"eid": f"e{i}", "oid": f"o{i}-{j}", "qualifier": "q"}
         for i in range(10) for j in range(3)]
    )
    log = _make_log(events=events, relations_e2o=e2o)
    assert list(C002.check(log)) == []


def test_c002_fires_on_outlier() -> None:
    events = pd.DataFrame(
        [{"eid": f"e{i}", "etype": "T", "timestamp": "t", "attrs": {}} for i in range(11)]
    )
    rels = []
    for i in range(10):
        for j in range(2):
            rels.append({"eid": f"e{i}", "oid": f"o{i}-{j}", "qualifier": "q"})
    for j in range(50):
        rels.append({"eid": "e10", "oid": f"big-{j}", "qualifier": "q"})
    log = _make_log(events=events, relations_e2o=pd.DataFrame(rels))
    violations = list(C002.check(log))
    assert len(violations) >= 1
    assert "e10" in violations[0].message


def test_c003_fires_on_high_fanin_object() -> None:
    objects = pd.DataFrame([{"oid": f"o{i}", "otype": "X", "attrs": {}} for i in range(11)])
    rels = []
    for i in range(10):
        for j in range(2):
            rels.append({"eid": f"e{i}-{j}", "oid": f"o{i}", "qualifier": "q"})
    for j in range(50):
        rels.append({"eid": f"big-{j}", "oid": "o10", "qualifier": "q"})
    events = pd.DataFrame(
        [{"eid": r["eid"], "etype": "T", "timestamp": "t", "attrs": {}} for r in rels]
    )
    log = _make_log(events=events, objects=objects, relations_e2o=pd.DataFrame(rels))
    violations = list(C003.check(log))
    assert len(violations) >= 1
    assert "o10" in violations[0].message


def test_c004_fires_on_cardinality_explosion() -> None:
    decls = pd.DataFrame([{"scope": "object", "type_name": "X",
                           "attribute_name": "id_field", "attribute_type": "string"}])
    objects = pd.DataFrame(
        [{"oid": f"o{i}", "otype": "X",
          "attrs": {"id_field": [("t", f"unique-{i}")]}}
         for i in range(1500)]
    )
    log = _make_log(objects=objects, attribute_decls=decls)
    violations = list(C004.check(log))
    assert len(violations) == 1
    assert "1500 distinct" in violations[0].message


def test_c005_clean_one_object_per_type() -> None:
    events = pd.DataFrame([{"eid": "e1", "etype": "T", "timestamp": "t", "attrs": {}}])
    objects = pd.DataFrame([{"oid": "o1", "otype": "Order", "attrs": {}}])
    e2o = pd.DataFrame([{"eid": "e1", "oid": "o1", "qualifier": "q"}])
    log = _make_log(events=events, objects=objects, relations_e2o=e2o)
    assert list(C005.check(log)) == []


def test_c005_fires_on_convergence() -> None:
    events = pd.DataFrame([{"eid": "e1", "etype": "Create", "timestamp": "t", "attrs": {}}])
    objects = pd.DataFrame(
        [{"oid": f"o{i}", "otype": "Order", "attrs": {}} for i in range(3)]
    )
    e2o = pd.DataFrame(
        [{"eid": "e1", "oid": f"o{i}", "qualifier": "q"} for i in range(3)]
    )
    log = _make_log(events=events, objects=objects, relations_e2o=e2o)
    violations = list(C005.check(log))
    assert len(violations) == 1
    assert "Create" in violations[0].message
    assert "Order" in violations[0].message


def test_c006_fires_on_divergence() -> None:
    events = pd.DataFrame(
        [{"eid": f"e{i}", "etype": "Update", "timestamp": "t", "attrs": {}}
         for i in range(15)]
    )
    objects = pd.DataFrame([{"oid": "o1", "otype": "Order", "attrs": {}}])
    e2o = pd.DataFrame(
        [{"eid": f"e{i}", "oid": "o1", "qualifier": "q"} for i in range(15)]
    )
    log = _make_log(events=events, objects=objects, relations_e2o=e2o)
    violations = list(C006.check(log))
    assert len(violations) == 1
    assert "o1" in violations[0].message


def test_c007_fires_on_coverage_gap() -> None:
    events = pd.DataFrame([
        {"eid": "e1", "etype": "A", "timestamp": "t", "attrs": {}},
        {"eid": "e2", "etype": "B", "timestamp": "t", "attrs": {}},
        {"eid": "e3", "etype": "C", "timestamp": "t", "attrs": {}},
        {"eid": "e4", "etype": "D", "timestamp": "t", "attrs": {}},
        {"eid": "e5", "etype": "E", "timestamp": "t", "attrs": {}},
    ])
    objects = pd.DataFrame([
        {"oid": "o1", "otype": "Popular", "attrs": {}},
        {"oid": "o2", "otype": "Other", "attrs": {}},
    ])
    e2o = pd.DataFrame([
        {"eid": "e1", "oid": "o1", "qualifier": "q"},
        {"eid": "e2", "oid": "o1", "qualifier": "q"},
        {"eid": "e3", "oid": "o1", "qualifier": "q"},
        {"eid": "e4", "oid": "o1", "qualifier": "q"},
        {"eid": "e5", "oid": "o2", "qualifier": "q"},
    ])
    log = _make_log(events=events, objects=objects, relations_e2o=e2o)
    violations = list(C007.check(log))
    assert any("Popular" in v.message for v in violations)
