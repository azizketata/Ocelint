"""Tests for OCEL-Q rules."""

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
from ocelint.rules.qualifier import Q001, Q002, Q003, Q004, Q005, Q006


def _make_log(
    *,
    events: pd.DataFrame | None = None,
    objects: pd.DataFrame | None = None,
    relations_e2o: pd.DataFrame | None = None,
    relations_o2o: pd.DataFrame | None = None,
) -> OcelLog:
    def empty(cols: tuple[str, ...]) -> pd.DataFrame:
        return pd.DataFrame({c: pd.Series(dtype="object") for c in cols})

    return OcelLog(
        events=events if events is not None else empty(EVENTS_COLUMNS),
        objects=objects if objects is not None else empty(OBJECTS_COLUMNS),
        relations_e2o=relations_e2o if relations_e2o is not None else empty(E2O_COLUMNS),
        relations_o2o=relations_o2o if relations_o2o is not None else empty(O2O_COLUMNS),
        event_types=empty(EVENT_TYPES_COLUMNS),
        object_types=empty(OBJECT_TYPES_COLUMNS),
        attribute_decls=empty(ATTRIBUTE_DECLS_COLUMNS),
        source_format="json",
        source_path=Path("test.json"),
    )


def _e2o(*triples: tuple[str, str, object]) -> pd.DataFrame:
    return pd.DataFrame(
        [{"eid": e, "oid": o, "qualifier": q} for e, o, q in triples]
    )


# --- Q001 empty qualifier -------------------------------------------------


def test_q001_clean() -> None:
    log = _make_log(relations_e2o=_e2o(("e1", "o1", "creates")))
    assert list(Q001.check(log)) == []


def test_q001_fires_on_blank() -> None:
    log = _make_log(relations_e2o=_e2o(("e1", "o1", ""), ("e2", "o2", "  "), ("e3", "o3", None)))
    violations = list(Q001.check(log))
    assert len(violations) == 1
    assert "3" in violations[0].message
    assert violations[0].severity == "warn"


# --- Q002 vocabulary inconsistency ---------------------------------------


def test_q002_clean_distinct_qualifiers() -> None:
    log = _make_log(relations_e2o=_e2o(
        ("e1", "o1", "creates"),
        ("e2", "o2", "delivers"),
        ("e3", "o3", "approves"),
    ))
    assert list(Q002.check(log)) == []


def test_q002_fires_on_similar() -> None:
    log = _make_log(relations_e2o=_e2o(
        ("e1", "o1", "created_by"),
        ("e2", "o2", "createdBy"),
    ))
    violations = list(Q002.check(log))
    assert len(violations) >= 1
    assert any("created" in v.message for v in violations)


# --- Q003 vocabulary explosion --------------------------------------------


def test_q003_clean_under_threshold() -> None:
    log = _make_log(relations_e2o=_e2o(("e1", "o1", "q")))
    assert list(Q003.check(log)) == []


def test_q003_fires_when_over_threshold() -> None:
    rels = [(f"e{i}", f"o{i}", f"qual_{i}") for i in range(60)]
    log = _make_log(relations_e2o=_e2o(*rels))
    violations = list(Q003.check(log))
    assert len(violations) == 1
    assert "60 distinct" in violations[0].message


# --- Q004 reserved characters ---------------------------------------------


def test_q004_clean() -> None:
    log = _make_log(relations_e2o=_e2o(("e1", "o1", "created_by")))
    assert list(Q004.check(log)) == []


def test_q004_fires_on_comma() -> None:
    log = _make_log(relations_e2o=_e2o(("e1", "o1", "created, by system")))
    violations = list(Q004.check(log))
    assert len(violations) == 1
    assert violations[0].severity == "warn"


def test_q004_fires_on_newline() -> None:
    log = _make_log(relations_e2o=_e2o(("e1", "o1", "multi\nline")))
    violations = list(Q004.check(log))
    assert len(violations) == 1


# --- Q005 singleton qualifier ---------------------------------------------


def test_q005_clean_all_repeated() -> None:
    log = _make_log(relations_e2o=_e2o(
        ("e1", "o1", "creates"),
        ("e2", "o2", "creates"),
    ))
    assert list(Q005.check(log)) == []


def test_q005_fires_on_singleton() -> None:
    log = _make_log(relations_e2o=_e2o(
        ("e1", "o1", "creates"),
        ("e2", "o2", "creates"),
        ("e3", "o3", "long-free-text-that-only-appears-once"),
    ))
    violations = list(Q005.check(log))
    assert len(violations) == 1
    assert "1 qualifier" in violations[0].message


# --- Q006 missing qualifier on subset of (etype, otype) pairs -------------


def test_q006_clean_uniform() -> None:
    events = pd.DataFrame([
        {"eid": f"e{i}", "etype": "Create Order", "timestamp": "t", "attrs": {}}
        for i in range(3)
    ])
    objects = pd.DataFrame([
        {"oid": f"o{i}", "otype": "Order", "attrs": {}} for i in range(3)
    ])
    e2o = _e2o(*[(f"e{i}", f"o{i}", "creates") for i in range(3)])
    log = _make_log(events=events, objects=objects, relations_e2o=e2o)
    assert list(Q006.check(log)) == []


def test_q006_fires_on_partial_qualification() -> None:
    events = pd.DataFrame([
        {"eid": f"e{i}", "etype": "Create Order", "timestamp": "t", "attrs": {}}
        for i in range(3)
    ])
    objects = pd.DataFrame([
        {"oid": f"o{i}", "otype": "Order", "attrs": {}} for i in range(3)
    ])
    e2o = _e2o(
        ("e0", "o0", "creates"),
        ("e1", "o1", "creates"),
        ("e2", "o2", ""),
    )
    log = _make_log(events=events, objects=objects, relations_e2o=e2o)
    violations = list(Q006.check(log))
    assert len(violations) == 1
    assert "Create Order" in violations[0].message
    assert "Order" in violations[0].message
