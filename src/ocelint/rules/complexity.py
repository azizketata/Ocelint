"""OCEL-C: complexity / convergence-divergence rules (C001-C007)."""

from __future__ import annotations

import contextlib
import statistics
from collections import defaultdict
from collections.abc import Iterator

from ocelint.engine import Rule, Violation
from ocelint.model import OcelLog


def _check_c001(log: OcelLog, threshold: int = 20) -> Iterator[Violation]:
    n = len(log.object_types)
    if n > threshold:
        yield Violation(
            code="C001",
            severity="info",
            message=f"{n} object types declared (threshold {threshold}); review for proliferation",
            location="object_types",
        )


def _check_c002(log: OcelLog, zeta: float = 3.0) -> Iterator[Violation]:
    if len(log.events) == 0 or len(log.relations_e2o) == 0:
        return
    counts_per_eid = log.relations_e2o.groupby("eid").size()
    eid_to_etype = dict(zip(log.events["eid"], log.events["etype"], strict=False))

    by_type: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for eid, count in counts_per_eid.items():
        etype = eid_to_etype.get(eid)
        if etype is not None:
            by_type[etype].append((str(eid), int(count)))

    for etype, entries in by_type.items():
        if len(entries) < 5:
            continue
        counts = [c for _, c in entries]
        mean = statistics.mean(counts)
        stdev = statistics.pstdev(counts)
        if stdev == 0:
            continue
        threshold = mean + zeta * stdev
        outliers = [(eid, c) for eid, c in entries if c > threshold]
        if not outliers:
            continue
        worst_eid, worst_count = max(outliers, key=lambda x: x[1])
        yield Violation(
            code="C002",
            severity="warn",
            message=(
                f"Event {worst_eid!r} ({etype!r}): fan-out {worst_count} E2O "
                f"(mean {mean:.1f}, threshold {threshold:.1f})"
            ),
            location=f"events[eid={worst_eid}]",
        )


def _check_c003(log: OcelLog, zeta: float = 3.0) -> Iterator[Violation]:
    if len(log.objects) == 0 or len(log.relations_e2o) == 0:
        return
    counts_per_oid = log.relations_e2o.groupby("oid").size()
    oid_to_otype = dict(zip(log.objects["oid"], log.objects["otype"], strict=False))

    by_type: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for oid, count in counts_per_oid.items():
        otype = oid_to_otype.get(oid)
        if otype is not None:
            by_type[otype].append((str(oid), int(count)))

    for otype, entries in by_type.items():
        if len(entries) < 5:
            continue
        counts = [c for _, c in entries]
        mean = statistics.mean(counts)
        stdev = statistics.pstdev(counts)
        if stdev == 0:
            continue
        threshold = mean + zeta * stdev
        outliers = [(oid, c) for oid, c in entries if c > threshold]
        if not outliers:
            continue
        worst_oid, worst_count = max(outliers, key=lambda x: x[1])
        yield Violation(
            code="C003",
            severity="warn",
            message=(
                f"Object {worst_oid!r} ({otype!r}): fan-in {worst_count} events "
                f"(mean {mean:.1f}, threshold {threshold:.1f})"
            ),
            location=f"objects[oid={worst_oid}]",
        )


def _check_c004(log: OcelLog, threshold: int = 1000) -> Iterator[Violation]:
    if len(log.attribute_decls) == 0:
        return
    for _, decl in log.attribute_decls.iterrows():
        scope = decl["scope"]
        type_name = decl["type_name"]
        attr_name = decl["attribute_name"]
        values: set[object] = set()
        if scope == "event":
            df = log.events[log.events["etype"] == type_name]
            for attrs in df["attrs"]:
                if not isinstance(attrs, dict) or attr_name not in attrs:
                    continue
                v = attrs[attr_name]
                if v is None:
                    continue
                with contextlib.suppress(TypeError):
                    values.add(v)
        else:
            df = log.objects[log.objects["otype"] == type_name]
            for attrs in df["attrs"]:
                if not isinstance(attrs, dict) or attr_name not in attrs:
                    continue
                for entry in attrs[attr_name] or []:
                    if not (isinstance(entry, tuple) and len(entry) == 2):
                        continue
                    v = entry[1]
                    if v is None:
                        continue
                    with contextlib.suppress(TypeError):
                        values.add(v)
        if len(values) > threshold:
            yield Violation(
                code="C004",
                severity="info",
                message=(
                    f"{scope.capitalize()} type {type_name!r}, attribute {attr_name!r}: "
                    f"{len(values)} distinct values (threshold {threshold}); "
                    "may be an ID stored as attribute"
                ),
                location=f"{scope}s[type={type_name}].attrs[{attr_name}]",
            )


def _check_c005(log: OcelLog) -> Iterator[Violation]:
    if len(log.relations_e2o) == 0 or len(log.events) == 0 or len(log.objects) == 0:
        return
    eid_to_etype = dict(zip(log.events["eid"], log.events["etype"], strict=False))
    oid_to_otype = dict(zip(log.objects["oid"], log.objects["otype"], strict=False))

    counts_per_event: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for _, rel in log.relations_e2o.iterrows():
        otype = oid_to_otype.get(rel["oid"])
        if otype is not None:
            counts_per_event[rel["eid"]][otype] += 1

    affected: dict[tuple[str, str], int] = defaultdict(int)
    for eid, types in counts_per_event.items():
        etype = eid_to_etype.get(eid)
        if etype is None:
            continue
        for otype, count in types.items():
            if count >= 2:
                affected[(etype, otype)] += 1

    for (etype, otype), count in affected.items():
        yield Violation(
            code="C005",
            severity="warn",
            message=(
                f"({etype!r}, {otype!r}): {count} event(s) reference 2+ objects of same type "
                "(convergence: flattening will duplicate the event)"
            ),
            location=f"event_object[etype={etype},otype={otype}]",
        )


def _check_c006(log: OcelLog, threshold: int = 10) -> Iterator[Violation]:
    if len(log.relations_e2o) == 0 or len(log.events) == 0:
        return
    eid_to_etype = dict(zip(log.events["eid"], log.events["etype"], strict=False))

    counts: dict[tuple[str, str], int] = defaultdict(int)
    for _, rel in log.relations_e2o.iterrows():
        etype = eid_to_etype.get(rel["eid"])
        if etype is None:
            continue
        counts[(rel["oid"], etype)] += 1

    high = [(oid, et, c) for (oid, et), c in counts.items() if c > threshold]
    if not high:
        return
    by_etype: dict[str, tuple[str, int]] = {}
    for oid, et, c in high:
        if et not in by_etype or c > by_etype[et][1]:
            by_etype[et] = (oid, c)
    for etype, (oid, c) in by_etype.items():
        yield Violation(
            code="C006",
            severity="warn",
            message=(
                f"Object {oid!r}: {c} events of type {etype!r} "
                "(divergence: flattening will interleave)"
            ),
            location=f"event_object[oid={oid},etype={etype}]",
        )


def _check_c007(log: OcelLog) -> Iterator[Violation]:
    if (
        len(log.relations_e2o) == 0
        or len(log.events) == 0
        or len(log.objects) == 0
    ):
        return
    eid_to_etype = dict(zip(log.events["eid"], log.events["etype"], strict=False))
    oid_to_otype = dict(zip(log.objects["oid"], log.objects["otype"], strict=False))

    etype_to_otypes: dict[str, set[str]] = defaultdict(set)
    for _, rel in log.relations_e2o.iterrows():
        etype = eid_to_etype.get(rel["eid"])
        otype = oid_to_otype.get(rel["oid"])
        if etype is not None and otype is not None:
            etype_to_otypes[etype].add(otype)

    if not etype_to_otypes:
        return

    all_etypes = set(etype_to_otypes.keys())
    otype_coverage: dict[str, set[str]] = defaultdict(set)
    for etype, otypes in etype_to_otypes.items():
        for otype in otypes:
            otype_coverage[otype].add(etype)

    for otype, touched_by in otype_coverage.items():
        if len(all_etypes) < 3:
            continue
        coverage_ratio = len(touched_by) / len(all_etypes)
        if coverage_ratio < 0.8 or len(touched_by) == len(all_etypes):
            continue
        missing = sorted(all_etypes - touched_by)
        yield Violation(
            code="C007",
            severity="info",
            message=(
                f"Object type {otype!r} touched by {len(touched_by)}/{len(all_etypes)} "
                f"event types; missing: {missing}"
            ),
            location=f"object_types[name={otype}]",
        )


C001 = Rule(
    code="C001",
    severity="info",
    description="Object-type proliferation: too many object types declared.",
    check=_check_c001,
)

C002 = Rule(
    code="C002",
    severity="warn",
    description="High E2O fan-out outlier per event type.",
    check=_check_c002,
)

C003 = Rule(
    code="C003",
    severity="warn",
    description="High E2O fan-in outlier per object type.",
    check=_check_c003,
)

C004 = Rule(
    code="C004",
    severity="info",
    description="Attribute value cardinality explosion (likely an ID stored as attribute).",
    check=_check_c004,
)

C005 = Rule(
    code="C005",
    severity="warn",
    description="Convergence risk: event references multiple objects of same type.",
    check=_check_c005,
)

C006 = Rule(
    code="C006",
    severity="warn",
    description="Divergence risk: object participates in many events of same type.",
    check=_check_c006,
)

C007 = Rule(
    code="C007",
    severity="info",
    description="Event-type to object-type coverage gap.",
    check=_check_c007,
)


__all__ = ["C001", "C002", "C003", "C004", "C005", "C006", "C007"]
