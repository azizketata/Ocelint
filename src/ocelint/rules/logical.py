"""OCEL-L: logical / structural semantic rules (L001-L007)."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator

from ocelint.engine import Rule, Violation
from ocelint.model import OcelLog

_SYMMETRIC_QUALIFIERS: frozenset[str] = frozenset(
    {"sibling", "related-to", "related_to", "peer", "links-to", "links_to"}
)
_HIERARCHICAL_QUALIFIERS: frozenset[str] = frozenset(
    {
        "part-of",
        "part_of",
        "parent",
        "contains",
        "contained-in",
        "contained_in",
        "child-of",
        "child_of",
    }
)


def _check_l001(log: OcelLog) -> Iterator[Violation]:
    if len(log.relations_o2o) == 0:
        return
    edges_by_qualifier: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for _, rel in log.relations_o2o.iterrows():
        q = rel["qualifier"]
        if isinstance(q, str) and q in _SYMMETRIC_QUALIFIERS:
            edges_by_qualifier[q].add((rel["source_oid"], rel["target_oid"]))

    for q, edges in edges_by_qualifier.items():
        seen: set[tuple[str, str]] = set()
        for src, tgt in edges:
            if (tgt, src) in edges:
                continue
            pair: tuple[str, str] = (min(src, tgt), max(src, tgt))
            if pair in seen:
                continue
            seen.add(pair)
            yield Violation(
                code="L001",
                severity="warn",
                message=(
                    f"Symmetric qualifier {q!r}: {src!r} -> {tgt!r} exists but "
                    f"reverse {tgt!r} -> {src!r} is missing"
                ),
                location=f"object_object[qualifier={q}]",
            )


def _check_l002(log: OcelLog) -> Iterator[Violation]:
    if len(log.relations_o2o) == 0:
        return
    graphs: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for _, rel in log.relations_o2o.iterrows():
        q = rel["qualifier"]
        if isinstance(q, str) and q in _HIERARCHICAL_QUALIFIERS:
            graphs[q][rel["source_oid"]].add(rel["target_oid"])

    for q, graph in graphs.items():
        cycles = _find_cycles(graph)
        seen: set[frozenset[str]] = set()
        for cycle in cycles:
            key = frozenset(cycle)
            if key in seen:
                continue
            seen.add(key)
            yield Violation(
                code="L002",
                severity="error",
                message=(
                    f"Hierarchical qualifier {q!r} forms cycle: "
                    f"{' -> '.join(cycle)} -> {cycle[0]}"
                ),
                location=f"object_object[qualifier={q}]",
            )


def _find_cycles(graph: dict[str, set[str]]) -> list[list[str]]:
    """Return list of distinct cycles in a directed graph."""
    visited: set[str] = set()
    on_stack: set[str] = set()
    path: list[str] = []
    cycles: list[list[str]] = []

    def dfs(node: str) -> None:
        if node in on_stack:
            idx = path.index(node)
            cycles.append(path[idx:])
            return
        if node in visited:
            return
        visited.add(node)
        on_stack.add(node)
        path.append(node)
        for nbr in list(graph.get(node, ())):
            dfs(nbr)
        path.pop()
        on_stack.remove(node)

    for node in list(graph):
        dfs(node)
    return cycles


def _check_l004(log: OcelLog) -> Iterator[Violation]:
    if len(log.objects) == 0:
        return
    adj: dict[str, set[str]] = {oid: set() for oid in log.objects["oid"].dropna()}
    if len(log.relations_o2o) > 0:
        for _, rel in log.relations_o2o.iterrows():
            s, t = rel["source_oid"], rel["target_oid"]
            if s in adj and t in adj:
                adj[s].add(t)
                adj[t].add(s)
    if len(log.relations_e2o) > 0:
        per_event: dict[str, list[str]] = defaultdict(list)
        for _, rel in log.relations_e2o.iterrows():
            if rel["oid"] in adj:
                per_event[rel["eid"]].append(rel["oid"])
        for oids in per_event.values():
            for i, a in enumerate(oids):
                for b in oids[i + 1 :]:
                    adj[a].add(b)
                    adj[b].add(a)

    visited: set[str] = set()
    components = 0
    sizes: list[int] = []
    for start in adj:
        if start in visited:
            continue
        components += 1
        size = 0
        stack = [start]
        while stack:
            node = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            size += 1
            stack.extend(n for n in adj[node] if n not in visited)
        sizes.append(size)

    if components > 1:
        sizes.sort(reverse=True)
        yield Violation(
            code="L004",
            severity="info",
            message=(
                f"Object graph has {components} disconnected components "
                f"(largest sizes: {sizes[:5]})"
            ),
            location="objects",
        )


def _check_l005(log: OcelLog) -> Iterator[Violation]:
    if len(log.objects) == 0:
        return
    referenced = set(log.relations_e2o["oid"]) if len(log.relations_e2o) > 0 else set()
    no_lifecycle = sorted(set(log.objects["oid"].dropna()) - referenced)
    if no_lifecycle:
        sample = no_lifecycle[:3]
        yield Violation(
            code="L005",
            severity="info",
            message=(
                f"{len(no_lifecycle)} object(s) participate in zero events "
                f"(no lifecycle); e.g., {sample}"
            ),
            location="objects",
        )


def _check_l006(log: OcelLog) -> Iterator[Violation]:
    if len(log.object_types) == 0:
        return
    names = sorted(set(log.object_types["name"].dropna()))
    if len(names) < 2:
        return
    seen: set[tuple[str, str]] = set()
    for i, a in enumerate(names):
        for b in names[i + 1 :]:
            pair = (a, b)
            if pair in seen:
                continue
            al, bl = a.lower(), b.lower()
            if al == bl:
                seen.add(pair)
                yield Violation(
                    code="L006",
                    severity="warn",
                    message=f"Object types {a!r} and {b!r} differ only in case",
                    location="object_types",
                )
                continue
            if al + "s" == bl or bl + "s" == al:
                seen.add(pair)
                yield Violation(
                    code="L006",
                    severity="warn",
                    message=f"Object types {a!r} and {b!r} look like singular/plural variants",
                    location="object_types",
                )


def _check_l007(log: OcelLog) -> Iterator[Violation]:
    if len(log.relations_o2o) == 0:
        return
    self_refs = log.relations_o2o[
        log.relations_o2o["source_oid"] == log.relations_o2o["target_oid"]
    ]
    seen: set[str] = set()
    for _, rel in self_refs.iterrows():
        oid = rel["source_oid"]
        if oid in seen:
            continue
        seen.add(oid)
        q = rel["qualifier"]
        yield Violation(
            code="L007",
            severity="warn",
            message=f"Object {oid!r} has O2O qualifier {q!r} pointing to itself",
            location=f"object_object[source_oid={oid},target_oid={oid}]",
        )


L001 = Rule(
    code="L001",
    severity="warn",
    description="Symmetry violation: symmetric qualifier present in one direction only.",
    check=_check_l001,
)

L002 = Rule(
    code="L002",
    severity="error",
    description="Cycle on hierarchical qualifier (part-of/parent/contains).",
    check=_check_l002,
)

L004 = Rule(
    code="L004",
    severity="info",
    description="Disconnected object graph component: log contains independent subgraphs.",
    check=_check_l004,
)

L005 = Rule(
    code="L005",
    severity="info",
    description="Object with no events: object exists but has no lifecycle.",
    check=_check_l005,
)

L006 = Rule(
    code="L006",
    severity="warn",
    description="Object-type name synonym collision (e.g. Order vs Orders).",
    check=_check_l006,
)

L007 = Rule(
    code="L007",
    severity="warn",
    description="Self-referencing O2O: object has O2O relation pointing to itself.",
    check=_check_l007,
)


__all__ = ["L001", "L002", "L004", "L005", "L006", "L007"]
