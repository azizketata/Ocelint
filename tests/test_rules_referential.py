"""Tests for OCEL-R rules."""

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
from ocelint.rules.referential import R001, R002, R003, R004, R005, R006, R007


def _make_log(
    *,
    events: pd.DataFrame | None = None,
    objects: pd.DataFrame | None = None,
    relations_e2o: pd.DataFrame | None = None,
    relations_o2o: pd.DataFrame | None = None,
    attribute_decls: pd.DataFrame | None = None,
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
        attribute_decls=attribute_decls if attribute_decls is not None else empty(ATTRIBUTE_DECLS_COLUMNS),
        source_format="json",
        source_path=Path("test.json"),
    )


def _events(*ids: str) -> pd.DataFrame:
    return pd.DataFrame(
        [{"eid": e, "etype": "T", "timestamp": "t", "attrs": {}} for e in ids]
    )


def _objects(*pairs: tuple[str, str], attrs: dict | None = None) -> pd.DataFrame:
    return pd.DataFrame(
        [{"oid": oid, "otype": ot, "attrs": attrs or {}} for oid, ot in pairs]
    )


def _e2o(*triples: tuple[str, str, str]) -> pd.DataFrame:
    return pd.DataFrame(
        [{"eid": e, "oid": o, "qualifier": q} for e, o, q in triples]
    )


def _o2o(*triples: tuple[str, str, str]) -> pd.DataFrame:
    return pd.DataFrame(
        [{"source_oid": s, "target_oid": t, "qualifier": q} for s, t, q in triples]
    )


# --- R001 dangling E2O event ref ------------------------------------------


def test_r001_clean() -> None:
    log = _make_log(
        events=_events("e1"),
        objects=_objects(("o1", "Order")),
        relations_e2o=_e2o(("e1", "o1", "creates")),
    )
    assert list(R001.check(log)) == []


def test_r001_fires_on_dangling() -> None:
    log = _make_log(
        events=_events("e1"),
        objects=_objects(("o1", "Order")),
        relations_e2o=_e2o(("e1", "o1", "creates"), ("e_ghost", "o1", "creates")),
    )
    violations = list(R001.check(log))
    assert len(violations) == 1
    assert "e_ghost" in violations[0].message
    assert violations[0].severity == "error"


def test_r001_dedupes_repeated_dangling() -> None:
    log = _make_log(
        events=_events("e1"),
        objects=_objects(("o1", "Order"), ("o2", "Order")),
        relations_e2o=_e2o(("e_ghost", "o1", "q"), ("e_ghost", "o2", "q")),
    )
    violations = list(R001.check(log))
    assert len(violations) == 1


# --- R002 dangling E2O object ref -----------------------------------------


def test_r002_clean() -> None:
    log = _make_log(
        events=_events("e1"),
        objects=_objects(("o1", "Order")),
        relations_e2o=_e2o(("e1", "o1", "creates")),
    )
    assert list(R002.check(log)) == []


def test_r002_fires_on_dangling() -> None:
    log = _make_log(
        events=_events("e1"),
        objects=_objects(("o1", "Order")),
        relations_e2o=_e2o(("e1", "o_ghost", "creates")),
    )
    violations = list(R002.check(log))
    assert len(violations) == 1
    assert "o_ghost" in violations[0].message


# --- R003 dangling O2O ref ------------------------------------------------


def test_r003_clean() -> None:
    log = _make_log(
        objects=_objects(("o1", "Order"), ("o2", "Order")),
        relations_o2o=_o2o(("o1", "o2", "contains")),
    )
    assert list(R003.check(log)) == []


def test_r003_fires_on_bad_source() -> None:
    log = _make_log(
        objects=_objects(("o1", "Order")),
        relations_o2o=_o2o(("ghost", "o1", "contains")),
    )
    violations = list(R003.check(log))
    assert len(violations) == 1
    assert "source" in violations[0].message
    assert "ghost" in violations[0].message


def test_r003_fires_on_bad_target() -> None:
    log = _make_log(
        objects=_objects(("o1", "Order")),
        relations_o2o=_o2o(("o1", "ghost", "contains")),
    )
    violations = list(R003.check(log))
    assert len(violations) == 1
    assert "target" in violations[0].message


def test_r003_fires_on_both() -> None:
    log = _make_log(
        objects=_objects(("o1", "Order")),
        relations_o2o=_o2o(("ghost1", "ghost2", "x")),
    )
    violations = list(R003.check(log))
    assert len(violations) == 2


# --- R004 orphaned event --------------------------------------------------


def test_r004_clean() -> None:
    log = _make_log(
        events=_events("e1"),
        objects=_objects(("o1", "Order")),
        relations_e2o=_e2o(("e1", "o1", "q")),
    )
    assert list(R004.check(log)) == []


def test_r004_fires_on_orphan_event() -> None:
    log = _make_log(
        events=_events("e1", "e2"),
        objects=_objects(("o1", "Order")),
        relations_e2o=_e2o(("e1", "o1", "q")),
    )
    violations = list(R004.check(log))
    assert len(violations) == 1
    assert "e2" in violations[0].message
    assert violations[0].severity == "warn"


# --- R005 orphaned object -------------------------------------------------


def test_r005_clean() -> None:
    log = _make_log(
        events=_events("e1"),
        objects=_objects(("o1", "Order")),
        relations_e2o=_e2o(("e1", "o1", "q")),
    )
    assert list(R005.check(log)) == []


def test_r005_fires_on_orphan_object() -> None:
    log = _make_log(
        events=_events("e1"),
        objects=_objects(("o1", "Order"), ("o2", "Order")),
        relations_e2o=_e2o(("e1", "o1", "q")),
    )
    violations = list(R005.check(log))
    assert len(violations) == 1
    assert "o2" in violations[0].message
    assert violations[0].severity == "info"


# --- R006 object type inconsistency ---------------------------------------


def test_r006_clean() -> None:
    log = _make_log(objects=_objects(("o1", "Order"), ("o2", "Order")))
    assert list(R006.check(log)) == []


def test_r006_fires_on_inconsistent_types() -> None:
    objects = pd.DataFrame(
        [
            {"oid": "o1", "otype": "Order", "attrs": {}},
            {"oid": "o1", "otype": "Invoice", "attrs": {}},
        ]
    )
    log = _make_log(objects=objects)
    violations = list(R006.check(log))
    assert len(violations) == 1
    assert "Invoice" in violations[0].message
    assert "Order" in violations[0].message
    assert violations[0].severity == "error"


# --- R007 missing required attribute --------------------------------------


def test_r007_clean() -> None:
    objects = pd.DataFrame(
        [{"oid": "o1", "otype": "Order", "attrs": {"price": [("t", 9.99)]}}]
    )
    decls = pd.DataFrame(
        [{"scope": "object", "type_name": "Order", "attribute_name": "price",
          "attribute_type": "float"}]
    )
    log = _make_log(objects=objects, attribute_decls=decls)
    assert list(R007.check(log)) == []


def test_r007_fires_on_missing_object_attr() -> None:
    objects = pd.DataFrame(
        [
            {"oid": "o1", "otype": "Order", "attrs": {"price": [("t", 9.99)]}},
            {"oid": "o2", "otype": "Order", "attrs": {}},
            {"oid": "o3", "otype": "Order", "attrs": {"price": []}},
        ]
    )
    decls = pd.DataFrame(
        [{"scope": "object", "type_name": "Order", "attribute_name": "price",
          "attribute_type": "float"}]
    )
    log = _make_log(objects=objects, attribute_decls=decls)
    violations = list(R007.check(log))
    assert len(violations) == 1
    assert "2/3" in violations[0].message
    assert "price" in violations[0].message


def test_r007_fires_on_missing_event_attr() -> None:
    events = pd.DataFrame(
        [
            {"eid": "e1", "etype": "Foo", "timestamp": "t",
             "attrs": {"user": "alice"}},
            {"eid": "e2", "etype": "Foo", "timestamp": "t", "attrs": {}},
        ]
    )
    decls = pd.DataFrame(
        [{"scope": "event", "type_name": "Foo", "attribute_name": "user",
          "attribute_type": "string"}]
    )
    log = _make_log(events=events, attribute_decls=decls)
    violations = list(R007.check(log))
    assert len(violations) == 1
    assert "1/2" in violations[0].message


def test_r007_silent_when_no_decls() -> None:
    log = _make_log(events=_events("e1"))
    assert list(R007.check(log)) == []
