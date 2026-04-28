"""Tests for ocelint.model."""

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


def _empty(columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame({c: pd.Series(dtype="object") for c in columns})


def test_empty_log_construction() -> None:
    log = OcelLog(
        events=_empty(EVENTS_COLUMNS),
        objects=_empty(OBJECTS_COLUMNS),
        relations_e2o=_empty(E2O_COLUMNS),
        relations_o2o=_empty(O2O_COLUMNS),
        event_types=_empty(EVENT_TYPES_COLUMNS),
        object_types=_empty(OBJECT_TYPES_COLUMNS),
        attribute_decls=_empty(ATTRIBUTE_DECLS_COLUMNS),
        source_format="json",
        source_path=Path("empty.json"),
    )

    assert log.parse_warnings == []
    assert log.source_format == "json"
    assert len(log.events) == 0
    assert list(log.events.columns) == list(EVENTS_COLUMNS)


def test_minimal_populated_log() -> None:
    log = OcelLog(
        events=pd.DataFrame(
            [{"eid": "e1", "etype": "Create Order", "timestamp": "2026-01-01T00:00:00Z", "attrs": {}}]
        ),
        objects=pd.DataFrame(
            [{"oid": "o1", "otype": "Order", "attrs": {"status": [("2026-01-01T00:00:00Z", "open")]}}]
        ),
        relations_e2o=pd.DataFrame([{"eid": "e1", "oid": "o1", "qualifier": "creates"}]),
        relations_o2o=_empty(O2O_COLUMNS),
        event_types=pd.DataFrame([{"name": "Create Order"}]),
        object_types=pd.DataFrame([{"name": "Order"}]),
        attribute_decls=pd.DataFrame(
            [{"scope": "object", "type_name": "Order", "attribute_name": "status", "attribute_type": "string"}]
        ),
        source_format="sqlite",
        source_path=Path("sample.sqlite"),
        parse_warnings=["unknown global default"],
    )

    assert len(log.events) == 1
    assert len(log.relations_e2o) == 1
    assert log.parse_warnings == ["unknown global default"]
    assert log.objects.iloc[0]["attrs"]["status"][0] == ("2026-01-01T00:00:00Z", "open")
