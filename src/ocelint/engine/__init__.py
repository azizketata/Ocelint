"""Rule engine: violations, rule registration, and execution."""

from __future__ import annotations

import importlib.metadata as _md
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, replace
from typing import Literal

from ocelint.model import OcelLog

Severity = Literal["error", "warn", "info"]
_SEVERITY_RANK: dict[Severity, int] = {"info": 0, "warn": 1, "error": 2}


@dataclass(frozen=True)
class Violation:
    code: str
    severity: Severity
    message: str
    location: str | None = None


@dataclass(frozen=True)
class Rule:
    code: str
    severity: Severity
    description: str
    check: Callable[[OcelLog], Iterator[Violation]]


def run_rules(log: OcelLog, rules: Iterable[Rule]) -> list[Violation]:
    """Execute rules against the log; return violations sorted by (code, location).

    Each emitted violation's severity is rewritten to its rule's effective
    severity, so config-time severity overrides propagate even though rules
    hard-code severity inside their check() functions.
    """
    violations: list[Violation] = []
    for rule in rules:
        for v in rule.check(log):
            if v.severity != rule.severity:
                v = replace(v, severity=rule.severity)
            violations.append(v)
    return sorted(violations, key=lambda v: (v.code, v.location or ""))


def max_severity(violations: Iterable[Violation]) -> Severity | None:
    """Return the highest severity present, or None if empty."""
    rank = -1
    found: Severity | None = None
    for v in violations:
        r = _SEVERITY_RANK[v.severity]
        if r > rank:
            rank = r
            found = v.severity
    return found


def discover_plugin_rules() -> list[Rule]:
    """Discover Rule objects registered via the 'ocelint.rules' entry point group.

    Each entry point may expose either a single Rule or a list of Rules.
    """
    out: list[Rule] = []
    for entry in _md.entry_points(group="ocelint.rules"):
        try:
            loaded = entry.load()
        except Exception:  # noqa: BLE001
            continue
        if isinstance(loaded, Rule):
            out.append(loaded)
        elif isinstance(loaded, (list, tuple)):
            out.extend(item for item in loaded if isinstance(item, Rule))
    return out


__all__ = [
    "Rule",
    "Severity",
    "Violation",
    "discover_plugin_rules",
    "max_severity",
    "run_rules",
]
