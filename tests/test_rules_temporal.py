"""Tests for OCEL-T rules."""

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
from ocelint.rules.temporal import T001, T002, T003, T004, T005, T006, T007, T008


def _make_log(
    *,
    events: pd.DataFrame | None = None,
    objects: pd.DataFrame | None = None,
    relations_e2o: pd.DataFrame | None = None,
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
        attribute_decls=empty(ATTRIBUTE_DECLS_COLUMNS),
        source_format="json",
        source_path=Path("test.json"),
    )


def _events(*specs: tuple[str, str, str]) -> pd.DataFrame:
    return pd.DataFrame(
        [{"eid": e, "etype": t, "timestamp": ts, "attrs": {}} for e, t, ts in specs]
    )


# --- T001 epoch sentinel --------------------------------------------------


def test_t001_clean() -> None:
    log = _make_log(events=_events(("e1", "T", "2026-01-01T00:00:00Z")))
    assert list(T001.check(log)) == []


def test_t001_fires_on_epoch() -> None:
    log = _make_log(events=_events(
        ("e1", "T", "1970-01-01T00:00:00Z"),
        ("e2", "T", "2026-01-01T00:00:00Z"),
    ))
    violations = list(T001.check(log))
    assert len(violations) == 1
    assert "1 event" in violations[0].message
    assert violations[0].severity == "error"


# --- T002 future-dated ----------------------------------------------------


def test_t002_clean() -> None:
    log = _make_log(events=_events(("e1", "T", "2020-01-01T00:00:00Z")))
    assert list(T002.check(log)) == []


def test_t002_fires_on_future() -> None:
    log = _make_log(events=_events(("e1", "T", "2099-01-01T00:00:00Z")))
    violations = list(T002.check(log))
    assert len(violations) == 1
    assert violations[0].severity == "warn"


# --- T003 temporal impossibility ------------------------------------------


def test_t003_clean() -> None:
    events = _events(("e1", "T", "2026-02-01T00:00:00Z"))
    objects = pd.DataFrame([
        {"oid": "o1", "otype": "Order",
         "attrs": {"price": [("2026-01-01T00:00:00Z", 9.99)]}}
    ])
    e2o = pd.DataFrame([{"eid": "e1", "oid": "o1", "qualifier": "q"}])
    log = _make_log(events=events, objects=objects, relations_e2o=e2o)
    assert list(T003.check(log)) == []


def test_t003_fires_when_event_precedes_object() -> None:
    events = _events(("e1", "T", "2026-01-01T00:00:00Z"))
    objects = pd.DataFrame([
        {"oid": "o1", "otype": "Order",
         "attrs": {"price": [("2026-02-01T00:00:00Z", 9.99)]}}
    ])
    e2o = pd.DataFrame([{"eid": "e1", "oid": "o1", "qualifier": "q"}])
    log = _make_log(events=events, objects=objects, relations_e2o=e2o)
    violations = list(T003.check(log))
    assert len(violations) == 1
    assert violations[0].severity == "error"
    assert "o1" in violations[0].message


def test_t003_skips_epoch_attribute_timestamps() -> None:
    """1970 is the OCEL 2.0 sentinel for static initial values; T003 must ignore it."""
    events = _events(("e1", "T", "2026-01-01T00:00:00Z"))
    objects = pd.DataFrame([
        {"oid": "o1", "otype": "Order",
         "attrs": {"status": [("1970-01-01T00:00:00Z", "open")]}}
    ])
    e2o = pd.DataFrame([{"eid": "e1", "oid": "o1", "qualifier": "q"}])
    log = _make_log(events=events, objects=objects, relations_e2o=e2o)
    assert list(T003.check(log)) == []


# --- T004 non-monotonic per-(oid, etype) ----------------------------------


def test_t004_clean() -> None:
    events = _events(
        ("e1", "Pack", "2026-01-01T00:00:00Z"),
        ("e2", "Pack", "2026-01-02T00:00:00Z"),
    )
    e2o = pd.DataFrame([
        {"eid": "e1", "oid": "o1", "qualifier": "q"},
        {"eid": "e2", "oid": "o1", "qualifier": "q"},
    ])
    log = _make_log(events=events, relations_e2o=e2o)
    assert list(T004.check(log)) == []


def test_t004_fires_when_events_go_backwards() -> None:
    events = _events(
        ("e1", "Pack", "2026-01-02T00:00:00Z"),
        ("e2", "Pack", "2026-01-01T00:00:00Z"),
    )
    e2o = pd.DataFrame([
        {"eid": "e1", "oid": "o1", "qualifier": "q"},
        {"eid": "e2", "oid": "o1", "qualifier": "q"},
    ])
    log = _make_log(events=events, relations_e2o=e2o)
    violations = list(T004.check(log))
    assert len(violations) == 1
    assert "o1" in violations[0].message
    assert "Pack" in violations[0].message


# --- T005 sub-second clustering -------------------------------------------


def test_t005_clean() -> None:
    events = _events(*[(f"e{i}", "T", f"2026-01-01T00:00:{i:02}Z") for i in range(15)])
    log = _make_log(events=events)
    assert list(T005.check(log)) == []


def test_t005_fires_on_clustering() -> None:
    events = _events(*[(f"e{i}", "T", "2026-01-01T00:00:00Z") for i in range(12)])
    log = _make_log(events=events)
    violations = list(T005.check(log))
    assert len(violations) == 1
    assert "12 events" in violations[0].message


# --- T006 granularity mismatch --------------------------------------------


def test_t006_clean_uniform_precision() -> None:
    events = _events(
        ("e1", "A", "2026-01-01T00:00:00.000Z"),
        ("e2", "B", "2026-01-02T00:00:00.000Z"),
    )
    log = _make_log(events=events)
    assert list(T006.check(log)) == []


def test_t006_fires_on_mixed_precision() -> None:
    events = _events(
        ("e1", "A", "2026-01-01T00:00:00.123Z"),
        ("e2", "B", "2026-01-02"),
    )
    log = _make_log(events=events)
    violations = list(T006.check(log))
    assert len(violations) == 1
    assert "subsecond" in violations[0].message
    assert "day" in violations[0].message


# --- T007 timezone inconsistency ------------------------------------------


def test_t007_clean_all_utc() -> None:
    events = _events(
        ("e1", "T", "2026-01-01T00:00:00Z"),
        ("e2", "T", "2026-01-02T00:00:00Z"),
    )
    log = _make_log(events=events)
    assert list(T007.check(log)) == []


def test_t007_fires_on_mix() -> None:
    events = _events(
        ("e1", "T", "2026-01-01T00:00:00Z"),
        ("e2", "T", "2026-01-01T00:00:00+02:00"),
        ("e3", "T", "2026-01-01T00:00:00"),
    )
    log = _make_log(events=events)
    violations = list(T007.check(log))
    assert len(violations) == 1
    assert violations[0].severity == "warn"


# --- T008 suspicious gap --------------------------------------------------


def test_t008_clean_small_gaps() -> None:
    events = _events(
        ("e1", "T", "2026-01-01T00:00:00Z"),
        ("e2", "T", "2026-02-01T00:00:00Z"),
    )
    log = _make_log(events=events)
    assert list(T008.check(log)) == []


def test_t008_fires_on_huge_gap() -> None:
    events = _events(
        ("e1", "T", "2020-01-01T00:00:00Z"),
        ("e2", "T", "2026-01-01T00:00:00Z"),
    )
    log = _make_log(events=events)
    violations = list(T008.check(log))
    assert len(violations) == 1
    assert "days" in violations[0].message


def test_t008_skips_when_under_two_events() -> None:
    log = _make_log(events=_events(("e1", "T", "2026-01-01T00:00:00Z")))
    assert list(T008.check(log)) == []
