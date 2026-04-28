"""OCEL-P: process-mining readiness rules (P001-P008)."""

from __future__ import annotations

import contextlib
from collections import defaultdict
from collections.abc import Iterator

from ocelint.engine import Rule, Violation
from ocelint.model import OcelLog
from ocelint.rules.temporal import _parse_timestamp

_EPOCH_PREFIX = "1970-01-01"


def _check_p001(log: OcelLog) -> Iterator[Violation]:
    if len(log.events) == 0:
        return
    referenced_eids = (
        set(log.relations_e2o["eid"]) if len(log.relations_e2o) > 0 else set()
    )
    types_with_e2o: set[str] = set()
    types_without_count: dict[str, int] = defaultdict(int)
    for _, ev in log.events.iterrows():
        if ev["eid"] in referenced_eids:
            types_with_e2o.add(ev["etype"])
        else:
            types_without_count[ev["etype"]] += 1
    for etype in sorted(set(types_without_count) - types_with_e2o):
        yield Violation(
            code="P001",
            severity="error",
            message=(
                f"Event type {etype!r}: all {types_without_count[etype]} events have "
                "zero E2O relations (OCPN discovery cannot place this transition)"
            ),
            location=f"events[etype={etype}]",
        )


def _check_p002(log: OcelLog) -> Iterator[Violation]:
    if len(log.events) == 0 or len(log.relations_e2o) == 0:
        return
    eid_to_etype = dict(zip(log.events["eid"], log.events["etype"], strict=False))
    oid_to_otype = dict(zip(log.objects["oid"], log.objects["otype"], strict=False))

    etype_to_otypes: dict[str, set[str]] = defaultdict(set)
    for _, rel in log.relations_e2o.iterrows():
        et = eid_to_etype.get(rel["eid"])
        ot = oid_to_otype.get(rel["oid"])
        if et and ot:
            etype_to_otypes[et].add(ot)

    adj: dict[str, set[str]] = defaultdict(set)
    all_otypes: set[str] = set()
    for otypes in etype_to_otypes.values():
        all_otypes |= otypes
        otypes_list = list(otypes)
        for i, a in enumerate(otypes_list):
            for b in otypes_list[i + 1 :]:
                adj[a].add(b)
                adj[b].add(a)
    for ot in all_otypes:
        adj.setdefault(ot, set())

    if not adj:
        return

    visited: set[str] = set()
    components: list[set[str]] = []
    for start in adj:
        if start in visited:
            continue
        comp: set[str] = set()
        stack = [start]
        while stack:
            n = stack.pop()
            if n in visited:
                continue
            visited.add(n)
            comp.add(n)
            stack.extend(adj[n] - visited)
        components.append(comp)

    if len(components) > 1:
        sizes = sorted([len(c) for c in components], reverse=True)
        yield Violation(
            code="P002",
            severity="warn",
            message=(
                f"Object-type subgraph partitions into {len(components)} components "
                f"(sizes {sizes}); possibly multiple independent processes in one log"
            ),
            location="object_types",
        )


def _check_p003(log: OcelLog, threshold: int = 30) -> Iterator[Violation]:
    if len(log.relations_e2o) == 0 or len(log.objects) == 0:
        return
    oid_to_otype = dict(zip(log.objects["oid"], log.objects["otype"], strict=False))
    otype_events: dict[str, set[str]] = defaultdict(set)
    for _, rel in log.relations_e2o.iterrows():
        ot = oid_to_otype.get(rel["oid"])
        if ot is not None:
            otype_events[ot].add(rel["eid"])
    for otype, eids in sorted(otype_events.items()):
        if len(eids) < threshold:
            yield Violation(
                code="P003",
                severity="info",
                message=(
                    f"Object type {otype!r}: only {len(eids)} events reference any "
                    f"object of this type (threshold {threshold}); underpowered for discovery"
                ),
                location=f"object_types[name={otype}]",
            )


def _build_object_event_sequences(
    log: OcelLog,
) -> dict[str, list[tuple[object, str, str]]]:
    """oid -> sorted list of (time, etype, eid). Time is datetime, typed as object."""
    eid_info: dict[str, tuple[str, object]] = {}
    for _, ev in log.events.iterrows():
        ts = ev["timestamp"]
        parsed = _parse_timestamp(ts) if isinstance(ts, str) else None
        if parsed is not None:
            eid_info[ev["eid"]] = (ev["etype"], parsed)
    sequences: dict[str, list[tuple[object, str, str]]] = defaultdict(list)
    for _, rel in log.relations_e2o.iterrows():
        info = eid_info.get(rel["eid"])
        if info is None:
            continue
        etype_str, time_obj = info
        sequences[rel["oid"]].append((time_obj, etype_str, rel["eid"]))
    for seq in sequences.values():
        seq.sort(key=lambda x: x[0])  # type: ignore[arg-type, return-value]
    return sequences


def _check_p004(
    log: OcelLog, consistency: float = 0.7, min_objects: int = 5
) -> Iterator[Violation]:
    if len(log.events) == 0 or len(log.relations_e2o) == 0 or len(log.objects) == 0:
        return
    sequences = _build_object_event_sequences(log)
    oid_to_otype = dict(zip(log.objects["oid"], log.objects["otype"], strict=False))

    type_first: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    type_total: dict[str, int] = defaultdict(int)
    for oid, seq in sequences.items():
        ot = oid_to_otype.get(oid)
        if ot is None or not seq:
            continue
        type_first[ot][seq[0][1]] += 1
        type_total[ot] += 1

    for otype, firsts in type_first.items():
        if type_total[otype] < min_objects:
            continue
        dominant_count = max(firsts.values())
        if dominant_count / type_total[otype] < consistency:
            n_distinct = len(firsts)
            yield Violation(
                code="P004",
                severity="warn",
                message=(
                    f"Object type {otype!r}: {n_distinct} different first-events across "
                    f"{type_total[otype]} objects (no consistent start activity)"
                ),
                location=f"object_types[name={otype}]",
            )


def _check_p005(
    log: OcelLog, consistency: float = 0.7, min_objects: int = 5
) -> Iterator[Violation]:
    if len(log.events) == 0 or len(log.relations_e2o) == 0 or len(log.objects) == 0:
        return
    sequences = _build_object_event_sequences(log)
    oid_to_otype = dict(zip(log.objects["oid"], log.objects["otype"], strict=False))

    type_last: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    type_total: dict[str, int] = defaultdict(int)
    for oid, seq in sequences.items():
        ot = oid_to_otype.get(oid)
        if ot is None or not seq:
            continue
        type_last[ot][seq[-1][1]] += 1
        type_total[ot] += 1

    for otype, lasts in type_last.items():
        if type_total[otype] < min_objects:
            continue
        dominant_count = max(lasts.values())
        if dominant_count / type_total[otype] < consistency:
            n_distinct = len(lasts)
            yield Violation(
                code="P005",
                severity="warn",
                message=(
                    f"Object type {otype!r}: {n_distinct} different last-events across "
                    f"{type_total[otype]} objects (no consistent end activity)"
                ),
                location=f"object_types[name={otype}]",
            )


def _check_p006(log: OcelLog) -> Iterator[Violation]:
    if len(log.objects) == 0 or len(log.relations_e2o) == 0 or len(log.events) == 0:
        return
    eid_to_ts = dict(zip(log.events["eid"], log.events["timestamp"], strict=False))
    obj_event_ts: dict[str, list[tuple[str, object]]] = defaultdict(list)
    for _, rel in log.relations_e2o.iterrows():
        ts = eid_to_ts.get(rel["eid"])
        obj_event_ts[rel["oid"]].append((rel["eid"], ts))

    ambiguous_count = 0
    for _, obj in log.objects.iterrows():
        oid = obj["oid"]
        attrs = obj.get("attrs")
        if not isinstance(attrs, dict):
            continue
        for entries in attrs.values():
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not (isinstance(entry, tuple) and len(entry) == 2):
                    continue
                attr_ts = entry[0]
                if not isinstance(attr_ts, str) or attr_ts.startswith(_EPOCH_PREFIX):
                    continue
                coincident = sum(1 for _, et in obj_event_ts.get(oid, []) if et == attr_ts)
                if coincident >= 2:
                    ambiguous_count += 1

    if ambiguous_count > 0:
        yield Violation(
            code="P006",
            severity="warn",
            message=(
                f"{ambiguous_count} attribute change(s) coincide with multiple events at "
                "the same timestamp (Goossens C16: causal event undetermined)"
            ),
            location="objects[attrs]",
        )


def _collect_attr_values(
    log: OcelLog, scope: str, type_name: str, attr_name: str
) -> set[object]:
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
    return values


def _check_p008(log: OcelLog) -> Iterator[Violation]:
    if len(log.attribute_decls) == 0:
        return
    for _, decl in log.attribute_decls.iterrows():
        values = _collect_attr_values(
            log, decl["scope"], decl["type_name"], decl["attribute_name"]
        )
        if len(values) == 1:
            sole = next(iter(values))
            yield Violation(
                code="P008",
                severity="info",
                message=(
                    f"{decl['scope'].capitalize()} type {decl['type_name']!r}, "
                    f"attribute {decl['attribute_name']!r}: all values are {sole!r} "
                    "(zero information content)"
                ),
                location=(
                    f"{decl['scope']}s[type={decl['type_name']}]"
                    f".attrs[{decl['attribute_name']}]"
                ),
            )


P001 = Rule(
    code="P001",
    severity="error",
    description="Event type with zero object relations: OCPN cannot place transition.",
    check=_check_p001,
)

P002 = Rule(
    code="P002",
    severity="warn",
    description="Disconnected object-type subgraph: log packs independent processes.",
    check=_check_p002,
)

P003 = Rule(
    code="P003",
    severity="info",
    description="Insufficient events per object type: underpowered for discovery.",
    check=_check_p003,
)

P004 = Rule(
    code="P004",
    severity="warn",
    description="No identifiable start activity for object type's lifecycle.",
    check=_check_p004,
)

P005 = Rule(
    code="P005",
    severity="warn",
    description="No identifiable end activity for object type's lifecycle.",
    check=_check_p005,
)

P006 = Rule(
    code="P006",
    severity="warn",
    description="Goossens C16 ambiguity: attribute change coincides with multiple events.",
    check=_check_p006,
)

P008 = Rule(
    code="P008",
    severity="info",
    description="Uniform attribute values: zero information content.",
    check=_check_p008,
)


__all__ = ["P001", "P002", "P003", "P004", "P005", "P006", "P008"]
