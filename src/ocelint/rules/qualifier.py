"""OCEL-Q: qualifier hygiene rules (Q001-Q006)."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator

from rapidfuzz import fuzz

from ocelint.engine import Rule, Violation
from ocelint.model import OcelLog

_RESERVED_CHARS: frozenset[str] = frozenset(",;\n\t\"'")


def _is_blank(q: object) -> bool:
    if q is None:
        return True
    return isinstance(q, str) and q.strip() == ""


def _check_q001(log: OcelLog) -> Iterator[Violation]:
    for label, df in (("E2O", log.relations_e2o), ("O2O", log.relations_o2o)):
        if len(df) == 0:
            continue
        count = sum(1 for q in df["qualifier"] if _is_blank(q))
        if count > 0:
            yield Violation(
                code="Q001",
                severity="warn",
                message=f"{count} {label} relation(s) have empty/blank qualifier",
                location=f"relations_{label.lower()}[qualifier]",
            )


def _distinct_qualifiers(log: OcelLog) -> list[str]:
    quals: set[str] = set()
    for df in (log.relations_e2o, log.relations_o2o):
        if len(df) == 0:
            continue
        for q in df["qualifier"]:
            if isinstance(q, str) and q.strip():
                quals.add(q)
    return sorted(quals)


def _check_q002(log: OcelLog, similarity_threshold: int = 80) -> Iterator[Violation]:
    quals = _distinct_qualifiers(log)
    if len(quals) < 2:
        return
    seen: set[tuple[str, str]] = set()
    for i, a in enumerate(quals):
        for b in quals[i + 1 :]:
            score = fuzz.ratio(a, b)
            if score < similarity_threshold:
                continue
            pair = (a, b)
            if pair in seen:
                continue
            seen.add(pair)
            yield Violation(
                code="Q002",
                severity="warn",
                message=(
                    f"Qualifier vocabulary inconsistency: {a!r} vs {b!r} "
                    f"(similarity {int(score)}%)"
                ),
                location="qualifiers",
            )


def _check_q003(log: OcelLog, threshold: int = 50) -> Iterator[Violation]:
    quals = _distinct_qualifiers(log)
    if len(quals) > threshold:
        yield Violation(
            code="Q003",
            severity="info",
            message=(
                f"{len(quals)} distinct qualifiers (threshold {threshold}); "
                "free-text values may be leaking into qualifier field"
            ),
            location="qualifiers",
        )


def _check_q004(log: OcelLog) -> Iterator[Violation]:
    seen: set[str] = set()
    for df in (log.relations_e2o, log.relations_o2o):
        if len(df) == 0:
            continue
        for q in df["qualifier"]:
            if not isinstance(q, str) or q in seen:
                continue
            if any(c in _RESERVED_CHARS for c in q):
                seen.add(q)
                yield Violation(
                    code="Q004",
                    severity="warn",
                    message=(
                        f"Qualifier {q!r} contains reserved characters "
                        "(comma/semicolon/newline/tab/quote) that break CSV round-tripping"
                    ),
                    location=f"qualifiers[{q!r}]",
                )


def _check_q005(log: OcelLog) -> Iterator[Violation]:
    counts: dict[str, int] = defaultdict(int)
    for df in (log.relations_e2o, log.relations_o2o):
        if len(df) == 0:
            continue
        for q in df["qualifier"]:
            if isinstance(q, str):
                counts[q] += 1
    singletons = sorted(q for q, c in counts.items() if c == 1)
    if singletons:
        sample = singletons[:3]
        yield Violation(
            code="Q005",
            severity="info",
            message=(
                f"{len(singletons)} qualifier(s) used exactly once "
                f"(e.g., {sample}); likely free-text or data-entry errors"
            ),
            location="qualifiers",
        )


def _check_q006(log: OcelLog) -> Iterator[Violation]:
    if len(log.relations_e2o) == 0 or len(log.events) == 0 or len(log.objects) == 0:
        return
    eid_to_etype = dict(zip(log.events["eid"], log.events["etype"], strict=False))
    oid_to_otype = dict(zip(log.objects["oid"], log.objects["otype"], strict=False))

    counts: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
    for _, rel in log.relations_e2o.iterrows():
        etype = eid_to_etype.get(rel["eid"])
        otype = oid_to_otype.get(rel["oid"])
        if etype is None or otype is None:
            continue
        bucket = counts[(etype, otype)]
        bucket[1] += 1
        q = rel["qualifier"]
        if isinstance(q, str) and q.strip():
            bucket[0] += 1

    for (etype, otype), (qual, total) in counts.items():
        if 0 < qual < total:
            yield Violation(
                code="Q006",
                severity="warn",
                message=(
                    f"({etype!r}, {otype!r}): {qual}/{total} E2O relations "
                    "have qualifiers, the rest do not"
                ),
                location=f"event_object[etype={etype},otype={otype}]",
            )


Q001 = Rule(
    code="Q001",
    severity="warn",
    description="Empty or blank qualifier on E2O/O2O relation.",
    check=_check_q001,
)

Q002 = Rule(
    code="Q002",
    severity="warn",
    description="Qualifier vocabulary inconsistency: similar spellings of same concept.",
    check=_check_q002,
)

Q003 = Rule(
    code="Q003",
    severity="info",
    description="Qualifier vocabulary explosion: too many distinct qualifier values.",
    check=_check_q003,
)

Q004 = Rule(
    code="Q004",
    severity="warn",
    description="Qualifier contains characters that break CSV/relational round-tripping.",
    check=_check_q004,
)

Q005 = Rule(
    code="Q005",
    severity="info",
    description="Qualifier used only once across all relations (likely free-text).",
    check=_check_q005,
)

Q006 = Rule(
    code="Q006",
    severity="warn",
    description=(
        "Missing qualifier on some E2O of a (event_type, object_type) pair "
        "where others have one."
    ),
    check=_check_q006,
)


__all__ = ["Q001", "Q002", "Q003", "Q004", "Q005", "Q006"]
