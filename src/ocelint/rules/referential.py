"""OCEL-R: referential integrity rules (R001-R008)."""

from __future__ import annotations

from collections.abc import Iterator

from ocelint.engine import Rule, Violation
from ocelint.model import OcelLog


def _check_r001(log: OcelLog) -> Iterator[Violation]:
    if len(log.relations_e2o) == 0:
        return
    valid = set(log.events["eid"])
    bad_eids = set(log.relations_e2o["eid"]) - valid
    for eid in sorted(bad_eids):
        yield Violation(
            code="R001",
            severity="error",
            message=f"E2O relation references event ID {eid!r} not present in events",
            location=f"event_object[eid={eid}]",
        )


def _check_r002(log: OcelLog) -> Iterator[Violation]:
    if len(log.relations_e2o) == 0:
        return
    valid = set(log.objects["oid"])
    bad_oids = set(log.relations_e2o["oid"]) - valid
    for oid in sorted(bad_oids):
        yield Violation(
            code="R002",
            severity="error",
            message=f"E2O relation references object ID {oid!r} not present in objects",
            location=f"event_object[oid={oid}]",
        )


def _check_r003(log: OcelLog) -> Iterator[Violation]:
    if len(log.relations_o2o) == 0:
        return
    valid = set(log.objects["oid"])
    src_bad = set(log.relations_o2o["source_oid"]) - valid
    tgt_bad = set(log.relations_o2o["target_oid"]) - valid
    for oid in sorted(src_bad):
        yield Violation(
            code="R003",
            severity="error",
            message=f"O2O relation source {oid!r} not present in objects",
            location=f"object_object[source_oid={oid}]",
        )
    for oid in sorted(tgt_bad):
        yield Violation(
            code="R003",
            severity="error",
            message=f"O2O relation target {oid!r} not present in objects",
            location=f"object_object[target_oid={oid}]",
        )


def _check_r004(log: OcelLog) -> Iterator[Violation]:
    if len(log.events) == 0:
        return
    referenced = set(log.relations_e2o["eid"]) if len(log.relations_e2o) > 0 else set()
    orphans = set(log.events["eid"].dropna()) - referenced
    for eid in sorted(orphans):
        yield Violation(
            code="R004",
            severity="warn",
            message=f"Event {eid!r} has no E2O relations (typically dropped by flattening)",
            location=f"events[eid={eid}]",
        )


def _check_r005(log: OcelLog) -> Iterator[Violation]:
    if len(log.objects) == 0:
        return
    referenced = set(log.relations_e2o["oid"]) if len(log.relations_e2o) > 0 else set()
    orphans = set(log.objects["oid"].dropna()) - referenced
    for oid in sorted(orphans):
        yield Violation(
            code="R005",
            severity="info",
            message=f"Object {oid!r} is not referenced by any event",
            location=f"objects[oid={oid}]",
        )


def _check_r006(log: OcelLog) -> Iterator[Violation]:
    if len(log.objects) == 0:
        return
    type_counts = log.objects.groupby("oid")["otype"].nunique()
    for oid in sorted(type_counts[type_counts > 1].index):
        types = sorted(log.objects.loc[log.objects["oid"] == oid, "otype"].unique())
        yield Violation(
            code="R006",
            severity="error",
            message=f"Object {oid!r} appears with inconsistent types: {types}",
            location=f"objects[oid={oid}]",
        )


def _check_r007(log: OcelLog) -> Iterator[Violation]:
    if len(log.attribute_decls) == 0:
        return
    decls = log.attribute_decls

    for scope, df, type_col in (
        ("event", log.events, "etype"),
        ("object", log.objects, "otype"),
    ):
        if len(df) == 0:
            continue
        scope_decls = decls[decls["scope"] == scope]
        for _, decl in scope_decls.iterrows():
            type_name = decl["type_name"]
            attr_name = decl["attribute_name"]
            of_type = df[df[type_col] == type_name]
            if len(of_type) == 0:
                continue
            missing = sum(
                1 for attrs in of_type["attrs"] if _attr_missing(attrs, attr_name)
            )
            if missing > 0:
                yield Violation(
                    code="R007",
                    severity="warn",
                    message=(
                        f"{scope.capitalize()} type {type_name!r}: declared attribute "
                        f"{attr_name!r} is missing on {missing}/{len(of_type)} entries"
                    ),
                    location=f"{scope}s[type={type_name}].attrs[{attr_name}]",
                )


def _attr_missing(attrs: object, name: str) -> bool:
    if not isinstance(attrs, dict):
        return True
    if name not in attrs:
        return True
    value = attrs[name]
    if value is None:
        return True
    return isinstance(value, list) and len(value) == 0


R001 = Rule(
    code="R001",
    severity="error",
    description="Dangling E2O event reference: relation event ID not in events.",
    check=_check_r001,
)

R002 = Rule(
    code="R002",
    severity="error",
    description="Dangling E2O object reference: relation object ID not in objects.",
    check=_check_r002,
)

R003 = Rule(
    code="R003",
    severity="error",
    description="Dangling O2O reference: source or target object ID not in objects.",
    check=_check_r003,
)

R004 = Rule(
    code="R004",
    severity="warn",
    description="Orphaned event: event has zero E2O relations.",
    check=_check_r004,
)

R005 = Rule(
    code="R005",
    severity="info",
    description="Orphaned object: object is not referenced by any event.",
    check=_check_r005,
)

R006 = Rule(
    code="R006",
    severity="error",
    description="Object type inconsistency: same oid appears with different types.",
    check=_check_r006,
)

R007 = Rule(
    code="R007",
    severity="warn",
    description="Missing required attribute value: declared but absent on entries.",
    check=_check_r007,
)


__all__ = ["R001", "R002", "R003", "R004", "R005", "R006", "R007"]
