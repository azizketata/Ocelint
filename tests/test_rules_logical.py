"""Tests for OCEL-L (logical) rules."""

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
from ocelint.rules.logical import L001, L002, L004, L005, L006, L007


def _make_log(
    *,
    objects: pd.DataFrame | None = None,
    relations_e2o: pd.DataFrame | None = None,
    relations_o2o: pd.DataFrame | None = None,
    object_types: pd.DataFrame | None = None,
) -> OcelLog:
    def empty(cols: tuple[str, ...]) -> pd.DataFrame:
        return pd.DataFrame({c: pd.Series(dtype="object") for c in cols})

    return OcelLog(
        events=empty(EVENTS_COLUMNS),
        objects=objects if objects is not None else empty(OBJECTS_COLUMNS),
        relations_e2o=relations_e2o if relations_e2o is not None else empty(E2O_COLUMNS),
        relations_o2o=relations_o2o if relations_o2o is not None else empty(O2O_COLUMNS),
        event_types=empty(EVENT_TYPES_COLUMNS),
        object_types=object_types if object_types is not None else empty(OBJECT_TYPES_COLUMNS),
        attribute_decls=empty(ATTRIBUTE_DECLS_COLUMNS),
        source_format="json",
        source_path=Path("test.json"),
    )


def _objects(*pairs: tuple[str, str]) -> pd.DataFrame:
    return pd.DataFrame([{"oid": o, "otype": t, "attrs": {}} for o, t in pairs])


def _o2o(*triples: tuple[str, str, str]) -> pd.DataFrame:
    return pd.DataFrame(
        [{"source_oid": s, "target_oid": t, "qualifier": q} for s, t, q in triples]
    )


def test_l001_clean_with_reverse() -> None:
    log = _make_log(
        objects=_objects(("o1", "X"), ("o2", "X")),
        relations_o2o=_o2o(("o1", "o2", "sibling"), ("o2", "o1", "sibling")),
    )
    assert list(L001.check(log)) == []


def test_l001_fires_when_reverse_missing() -> None:
    log = _make_log(
        objects=_objects(("o1", "X"), ("o2", "X")),
        relations_o2o=_o2o(("o1", "o2", "sibling")),
    )
    assert len(list(L001.check(log))) == 1


def test_l002_clean_acyclic() -> None:
    log = _make_log(
        objects=_objects(("o1", "X"), ("o2", "X"), ("o3", "X")),
        relations_o2o=_o2o(("o1", "o2", "part-of"), ("o2", "o3", "part-of")),
    )
    assert list(L002.check(log)) == []


def test_l002_detects_cycle() -> None:
    log = _make_log(
        objects=_objects(("o1", "X"), ("o2", "X")),
        relations_o2o=_o2o(("o1", "o2", "part-of"), ("o2", "o1", "part-of")),
    )
    violations = list(L002.check(log))
    assert len(violations) >= 1
    assert violations[0].severity == "error"


def test_l002_ignores_non_hierarchical_cycle() -> None:
    """Cycle on a non-hierarchical qualifier shouldn't fire L002."""
    log = _make_log(
        objects=_objects(("o1", "X"), ("o2", "X")),
        relations_o2o=_o2o(("o1", "o2", "sibling"), ("o2", "o1", "sibling")),
    )
    assert list(L002.check(log)) == []


def test_l004_single_component_clean() -> None:
    log = _make_log(
        objects=_objects(("o1", "X"), ("o2", "X")),
        relations_o2o=_o2o(("o1", "o2", "q")),
    )
    assert list(L004.check(log)) == []


def test_l004_fires_on_disconnected() -> None:
    log = _make_log(
        objects=_objects(("o1", "X"), ("o2", "X"), ("o3", "Y"), ("o4", "Y")),
        relations_o2o=_o2o(("o1", "o2", "q"), ("o3", "o4", "q")),
    )
    violations = list(L004.check(log))
    assert len(violations) == 1
    assert "2 disconnected" in violations[0].message


def test_l005_clean() -> None:
    log = _make_log(
        objects=_objects(("o1", "X")),
        relations_e2o=pd.DataFrame([{"eid": "e1", "oid": "o1", "qualifier": "q"}]),
    )
    assert list(L005.check(log)) == []


def test_l005_fires_on_unused_object() -> None:
    log = _make_log(objects=_objects(("o1", "X"), ("o2", "X")))
    violations = list(L005.check(log))
    assert len(violations) == 1
    assert "2 object" in violations[0].message


def test_l006_clean() -> None:
    log = _make_log(object_types=pd.DataFrame([{"name": "Order"}, {"name": "Customer"}]))
    assert list(L006.check(log)) == []


def test_l006_detects_case_collision() -> None:
    log = _make_log(object_types=pd.DataFrame([{"name": "Order"}, {"name": "order"}]))
    violations = list(L006.check(log))
    assert len(violations) == 1
    assert "case" in violations[0].message


def test_l006_detects_plural_variants() -> None:
    log = _make_log(object_types=pd.DataFrame([{"name": "Order"}, {"name": "Orders"}]))
    violations = list(L006.check(log))
    assert len(violations) == 1
    assert "singular/plural" in violations[0].message


def test_l007_clean() -> None:
    log = _make_log(
        objects=_objects(("o1", "X"), ("o2", "X")),
        relations_o2o=_o2o(("o1", "o2", "part-of")),
    )
    assert list(L007.check(log)) == []


def test_l007_fires_on_self_loop() -> None:
    log = _make_log(
        objects=_objects(("o1", "X")),
        relations_o2o=_o2o(("o1", "o1", "part-of")),
    )
    violations = list(L007.check(log))
    assert len(violations) == 1
    assert violations[0].severity == "warn"
