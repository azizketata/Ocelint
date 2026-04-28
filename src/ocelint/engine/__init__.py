"""Rule engine: violations, rule registration, and execution."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
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
    """Execute rules against the log; return violations sorted by (code, location)."""
    violations: list[Violation] = []
    for rule in rules:
        violations.extend(rule.check(log))
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


__all__ = ["Rule", "Severity", "Violation", "max_severity", "run_rules"]
