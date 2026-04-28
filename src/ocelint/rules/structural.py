"""OCEL-S: structural / serialization rules (S001-S012)."""

from __future__ import annotations

import re
from collections.abc import Iterator

import pandas as pd

from ocelint.engine import Rule, Violation
from ocelint.model import OcelLog

_ISO_8601_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}"
    r"([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?)?"
    r"(Z|[+-]\d{2}:?\d{2})?$"
)


def _check_s001(log: OcelLog) -> Iterator[Violation]:
    if len(log.events) == 0:
        return
    counts = log.events["eid"].value_counts()
    for eid, count in counts[counts > 1].items():
        yield Violation(
            code="S001",
            severity="error",
            message=f"Event ID {eid!r} occurs {int(count)} times (must be unique)",
            location=f"events[eid={eid}]",
        )


def _check_s002(log: OcelLog) -> Iterator[Violation]:
    if len(log.objects) == 0:
        return
    counts = log.objects["oid"].value_counts()
    for oid, count in counts[counts > 1].items():
        yield Violation(
            code="S002",
            severity="error",
            message=f"Object ID {oid!r} occurs {int(count)} times (must be unique)",
            location=f"objects[oid={oid}]",
        )


def _check_s003(log: OcelLog) -> Iterator[Violation]:
    for scope, df, col in (
        ("events", log.events, "eid"),
        ("objects", log.objects, "oid"),
    ):
        if len(df) == 0:
            continue
        ids = df[col].dropna().drop_duplicates()
        if len(ids) == 0:
            continue
        groups = ids.groupby(ids.str.lower())
        for low, group in groups:
            variants = sorted(group.unique())
            if len(variants) > 1:
                yield Violation(
                    code="S003",
                    severity="warn",
                    message=f"{scope}: case-insensitive ID collision: {variants}",
                    location=f"{scope}[lower={low}]",
                )


def _check_s004(log: OcelLog) -> Iterator[Violation]:
    if len(log.events) == 0:
        return
    declared = set(log.event_types["name"]) if len(log.event_types) > 0 else set()
    used_types = set(log.events["etype"].dropna().unique())
    for etype in sorted(used_types - declared):
        yield Violation(
            code="S004",
            severity="error",
            message=f"Event type {etype!r} used but not declared in event_types",
            location=f"events[etype={etype}]",
        )


def _check_s005(log: OcelLog) -> Iterator[Violation]:
    if len(log.objects) == 0:
        return
    declared = set(log.object_types["name"]) if len(log.object_types) > 0 else set()
    used_types = set(log.objects["otype"].dropna().unique())
    for otype in sorted(used_types - declared):
        yield Violation(
            code="S005",
            severity="error",
            message=f"Object type {otype!r} used but not declared in object_types",
            location=f"objects[otype={otype}]",
        )


def _check_s006(log: OcelLog) -> Iterator[Violation]:
    if len(log.attribute_decls) == 0:
        return
    decls = log.attribute_decls
    event_decls: dict[str, set[str]] = {}
    object_decls: dict[str, set[str]] = {}
    for _, row in decls.iterrows():
        target = event_decls if row["scope"] == "event" else object_decls
        target.setdefault(row["type_name"], set()).add(row["attribute_name"])

    yield from _check_attribute_decls(
        scope="event",
        df=log.events,
        type_col="etype",
        decls_by_type=event_decls,
        location_template="events[etype={t}].attrs[{n}]",
        message_template="Event attribute {n!r} not declared for event type {t!r}",
    )
    yield from _check_attribute_decls(
        scope="object",
        df=log.objects,
        type_col="otype",
        decls_by_type=object_decls,
        location_template="objects[otype={t}].attrs[{n}]",
        message_template="Object attribute {n!r} not declared for object type {t!r}",
    )


def _check_attribute_decls(
    *,
    scope: str,
    df: pd.DataFrame,
    type_col: str,
    decls_by_type: dict[str, set[str]],
    location_template: str,
    message_template: str,
) -> Iterator[Violation]:
    if len(df) == 0:
        return
    seen: set[tuple[str, str]] = set()
    for _, row in df.iterrows():
        type_name = row[type_col]
        if type_name not in decls_by_type:
            continue
        attrs = row.get("attrs") or {}
        if not isinstance(attrs, dict):
            continue
        declared = decls_by_type[type_name]
        for name in attrs:
            key = (type_name, name)
            if key in seen or name in declared:
                continue
            seen.add(key)
            yield Violation(
                code="S006",
                severity="warn",
                message=message_template.format(t=type_name, n=name),
                location=location_template.format(t=type_name, n=name),
            )


def _check_s008(log: OcelLog) -> Iterator[Violation]:
    if len(log.events) == 0:
        return
    seen: set[str] = set()
    for ts in log.events["timestamp"]:
        if ts is None or not isinstance(ts, str) or ts in seen:
            continue
        if _ISO_8601_RE.match(ts) is None:
            seen.add(ts)
            yield Violation(
                code="S008",
                severity="error",
                message=f"Timestamp {ts!r} is not a recognized ISO 8601 format",
                location=f"events[timestamp={ts}]",
            )


S001 = Rule(
    code="S001",
    severity="error",
    description="Duplicate event ID: two or more events share the same ocel:eid.",
    check=_check_s001,
)

S002 = Rule(
    code="S002",
    severity="error",
    description="Duplicate object ID: two or more objects share the same ocel:oid.",
    check=_check_s002,
)

S003 = Rule(
    code="S003",
    severity="warn",
    description="Case-insensitive ID collision: IDs differ only in case.",
    check=_check_s003,
)

S004 = Rule(
    code="S004",
    severity="error",
    description="Event references undeclared event type.",
    check=_check_s004,
)

S005 = Rule(
    code="S005",
    severity="error",
    description="Object references undeclared object type.",
    check=_check_s005,
)

S006 = Rule(
    code="S006",
    severity="warn",
    description="Attribute name not declared in type schema.",
    check=_check_s006,
)

S008 = Rule(
    code="S008",
    severity="error",
    description="Non-ISO-8601 timestamp format.",
    check=_check_s008,
)


__all__ = ["S001", "S002", "S003", "S004", "S005", "S006", "S008"]
