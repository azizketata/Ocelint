"""Tests for ocelint.engine."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pandas as pd

from ocelint.engine import Rule, Violation, max_severity, run_rules
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


def _empty_log() -> OcelLog:
    cols = {
        "events": EVENTS_COLUMNS,
        "objects": OBJECTS_COLUMNS,
        "relations_e2o": E2O_COLUMNS,
        "relations_o2o": O2O_COLUMNS,
        "event_types": EVENT_TYPES_COLUMNS,
        "object_types": OBJECT_TYPES_COLUMNS,
        "attribute_decls": ATTRIBUTE_DECLS_COLUMNS,
    }
    frames = {k: pd.DataFrame({c: pd.Series(dtype="object") for c in v}) for k, v in cols.items()}
    return OcelLog(
        **frames,
        source_format="json",
        source_path=Path("/tmp/empty.json"),
    )


def _rule(code: str, severity: str, locations: list[str]) -> Rule:
    def _check(_: OcelLog) -> Iterator[Violation]:
        for loc in locations:
            yield Violation(code=code, severity=severity, message=f"{code} fired", location=loc)
    return Rule(code=code, severity=severity, description=f"test {code}", check=_check)


def test_run_rules_aggregates_and_sorts() -> None:
    log = _empty_log()
    r1 = _rule("S001", "error", ["events[1]", "events[0]"])
    r2 = _rule("R001", "warn", ["e2o[5]"])
    violations = run_rules(log, [r1, r2])

    assert [v.code for v in violations] == ["R001", "S001", "S001"]
    assert [v.location for v in violations] == ["e2o[5]", "events[0]", "events[1]"]


def test_run_rules_empty() -> None:
    assert run_rules(_empty_log(), []) == []


def test_max_severity_picks_highest() -> None:
    vs = [
        Violation(code="X", severity="info", message=""),
        Violation(code="Y", severity="error", message=""),
        Violation(code="Z", severity="warn", message=""),
    ]
    assert max_severity(vs) == "error"


def test_max_severity_warn_only() -> None:
    vs = [
        Violation(code="X", severity="info", message=""),
        Violation(code="Y", severity="warn", message=""),
    ]
    assert max_severity(vs) == "warn"


def test_max_severity_empty() -> None:
    assert max_severity([]) is None


def test_discover_plugin_rules_empty_when_no_plugins() -> None:
    """No third-party rule packs installed -> empty list, no errors."""
    from ocelint.engine import discover_plugin_rules

    assert discover_plugin_rules() == []


def test_sdk_reexports_public_types() -> None:
    """The ocelint.sdk module exposes the API needed by plugin authors."""
    from ocelint.sdk import OcelLog as SdkOcelLog
    from ocelint.sdk import Rule as SdkRule
    from ocelint.sdk import Violation as SdkViolation

    assert SdkOcelLog is OcelLog or SdkOcelLog.__name__ == "OcelLog"
    assert SdkRule is Rule or SdkRule.__name__ == "Rule"
    assert SdkViolation is Violation or SdkViolation.__name__ == "Violation"
