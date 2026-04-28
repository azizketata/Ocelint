"""OCEL-T: temporal coherence rules (T001-T008)."""

from __future__ import annotations

import datetime as _dt
import re
from collections import defaultdict
from collections.abc import Iterator

from ocelint.engine import Rule, Violation
from ocelint.model import OcelLog

_EPOCH_PREFIX = "1970-01-01"
_HAS_OFFSET_RE = re.compile(r"[+-]\d{2}:?\d{2}$")
_HAS_TIME_RE = re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}")


def _parse_timestamp(ts: str) -> _dt.datetime | None:
    """Parse ISO 8601 / RFC 3339; return None on failure."""
    if not isinstance(ts, str):
        return None
    s = ts
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    if " " in s and "T" not in s:
        s = s.replace(" ", "T", 1)
    try:
        return _dt.datetime.fromisoformat(s)
    except ValueError:
        return None


def _now_utc() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


def _check_t001(log: OcelLog) -> Iterator[Violation]:
    if len(log.events) == 0:
        return
    epoch_count = 0
    sample: str | None = None
    for ts in log.events["timestamp"]:
        if isinstance(ts, str) and ts.startswith(_EPOCH_PREFIX):
            epoch_count += 1
            if sample is None:
                sample = ts
    if epoch_count > 0:
        yield Violation(
            code="T001",
            severity="error",
            message=(
                f"{epoch_count} event(s) use Unix-epoch sentinel timestamp "
                f"(e.g., {sample!r}); the epoch is reserved for static-value markers"
            ),
            location="events[timestamp]",
        )


def _check_t002(log: OcelLog) -> Iterator[Violation]:
    if len(log.events) == 0:
        return
    now = _now_utc()
    future_count = 0
    sample: str | None = None
    for ts in log.events["timestamp"]:
        if not isinstance(ts, str):
            continue
        parsed = _parse_timestamp(ts)
        if parsed is None:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=_dt.timezone.utc)
        if parsed > now:
            future_count += 1
            if sample is None:
                sample = ts
    if future_count > 0:
        yield Violation(
            code="T002",
            severity="warn",
            message=f"{future_count} event(s) are dated in the future (e.g., {sample!r})",
            location="events[timestamp]",
        )


def _check_t003(log: OcelLog) -> Iterator[Violation]:
    if len(log.events) == 0 or len(log.relations_e2o) == 0:
        return

    oid_earliest: dict[str, _dt.datetime] = {}
    for _, obj in log.objects.iterrows():
        attrs = obj.get("attrs")
        if not isinstance(attrs, dict):
            continue
        earliest: _dt.datetime | None = None
        for entries in attrs.values():
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not (isinstance(entry, tuple) and len(entry) == 2):
                    continue
                ts_str = entry[0]
                if not isinstance(ts_str, str) or ts_str.startswith(_EPOCH_PREFIX):
                    continue
                t = _parse_timestamp(ts_str)
                if t is None:
                    continue
                if earliest is None or t < earliest:
                    earliest = t
        if earliest is not None:
            oid_earliest[obj["oid"]] = earliest

    eid_to_time: dict[str, _dt.datetime] = {}
    for _, ev in log.events.iterrows():
        ts = ev["timestamp"]
        if not isinstance(ts, str):
            continue
        t = _parse_timestamp(ts)
        if t is not None:
            eid_to_time[ev["eid"]] = t

    seen: set[tuple[str, str]] = set()
    for _, rel in log.relations_e2o.iterrows():
        eid = rel["eid"]
        oid = rel["oid"]
        e_time = eid_to_time.get(eid)
        o_earliest = oid_earliest.get(oid)
        if e_time is None or o_earliest is None:
            continue
        e_aware = e_time if e_time.tzinfo else e_time.replace(tzinfo=_dt.timezone.utc)
        o_aware = (
            o_earliest if o_earliest.tzinfo else o_earliest.replace(tzinfo=_dt.timezone.utc)
        )
        if e_aware < o_aware:
            key = (eid, oid)
            if key in seen:
                continue
            seen.add(key)
            yield Violation(
                code="T003",
                severity="error",
                message=(
                    f"Event {eid!r} at {e_time.isoformat()} precedes object {oid!r}'s "
                    f"earliest known time {o_earliest.isoformat()}"
                ),
                location=f"event_object[eid={eid},oid={oid}]",
            )


def _check_t004(log: OcelLog) -> Iterator[Violation]:
    if len(log.events) == 0 or len(log.relations_e2o) == 0:
        return

    eid_info: dict[str, tuple[int, str, _dt.datetime | None]] = {}
    for idx, ev in enumerate(log.events.itertuples(index=False)):
        ts_str = getattr(ev, "timestamp", None)
        t = _parse_timestamp(ts_str) if isinstance(ts_str, str) else None
        eid_info[str(ev.eid)] = (idx, str(ev.etype), t)

    groups: dict[tuple[str, str], list[tuple[int, _dt.datetime]]] = defaultdict(list)
    for _, rel in log.relations_e2o.iterrows():
        info = eid_info.get(rel["eid"])
        if info is None:
            continue
        idx, etype, t = info
        if t is None:
            continue
        groups[(rel["oid"], etype)].append((idx, t))

    for (oid, etype), entries in groups.items():
        if len(entries) < 2:
            continue
        entries.sort()
        for i in range(1, len(entries)):
            if entries[i][1] < entries[i - 1][1]:
                yield Violation(
                    code="T004",
                    severity="warn",
                    message=(
                        f"Object {oid!r}: events of type {etype!r} go backwards in "
                        f"time ({entries[i - 1][1].isoformat()} -> {entries[i][1].isoformat()})"
                    ),
                    location=f"objects[oid={oid}].events[etype={etype}]",
                )
                break


def _check_t005(log: OcelLog, threshold: int = 10) -> Iterator[Violation]:
    if len(log.events) == 0:
        return
    counts: dict[str, int] = defaultdict(int)
    for ts in log.events["timestamp"]:
        if not isinstance(ts, str):
            continue
        truncated = ts.split(".", 1)[0]
        counts[truncated] += 1
    for second, count in sorted(counts.items(), key=lambda x: -x[1]):
        if count < threshold:
            break
        yield Violation(
            code="T005",
            severity="warn",
            message=f"{count} events share timestamp second {second!r}",
            location=f"events[timestamp={second}]",
        )


def _detect_precision(ts: str) -> str:
    if "." in ts:
        return "subsecond"
    if ":" in ts:
        return "second"
    return "day"


def _check_t006(log: OcelLog) -> Iterator[Violation]:
    if len(log.events) == 0:
        return
    type_precisions: dict[str, set[str]] = defaultdict(set)
    for _, ev in log.events.iterrows():
        if isinstance(ev["timestamp"], str):
            type_precisions[ev["etype"]].add(_detect_precision(ev["timestamp"]))
    used: set[str] = set()
    for ps in type_precisions.values():
        used |= ps
    if len(used) > 1:
        summary = {et: sorted(ps) for et, ps in type_precisions.items()}
        yield Violation(
            code="T006",
            severity="info",
            message=f"Timestamp precision varies across event types: {summary}",
            location="events[timestamp]",
        )


def _detect_zone(ts: str) -> str:
    if ts.endswith("Z"):
        return "utc"
    if _HAS_OFFSET_RE.search(ts):
        return "offset"
    if _HAS_TIME_RE.search(ts):
        return "naive"
    return "date-only"


def _check_t007(log: OcelLog) -> Iterator[Violation]:
    if len(log.events) == 0:
        return
    zones: set[str] = set()
    for ts in log.events["timestamp"]:
        if isinstance(ts, str):
            zones.add(_detect_zone(ts))
    zones.discard("date-only")
    if len(zones) > 1:
        yield Violation(
            code="T007",
            severity="warn",
            message=f"Log mixes timezone conventions: {sorted(zones)}",
            location="events[timestamp]",
        )


def _check_t008(log: OcelLog, threshold_days: int = 365) -> Iterator[Violation]:
    if len(log.events) < 2:
        return
    times: list[_dt.datetime] = []
    for ts in log.events["timestamp"]:
        if not isinstance(ts, str):
            continue
        t = _parse_timestamp(ts)
        if t is None:
            continue
        times.append(t if t.tzinfo else t.replace(tzinfo=_dt.timezone.utc))
    if len(times) < 2:
        return
    times.sort()
    threshold = _dt.timedelta(days=threshold_days)
    max_gap = _dt.timedelta(0)
    gap_endpoints: tuple[_dt.datetime, _dt.datetime] | None = None
    for i in range(1, len(times)):
        gap = times[i] - times[i - 1]
        if gap > max_gap:
            max_gap = gap
            gap_endpoints = (times[i - 1], times[i])
    if max_gap > threshold and gap_endpoints is not None:
        yield Violation(
            code="T008",
            severity="info",
            message=(
                f"Largest gap between consecutive events is {max_gap.days} days "
                f"({gap_endpoints[0].isoformat()} -> {gap_endpoints[1].isoformat()})"
            ),
            location="events[timestamp]",
        )


T001 = Rule(
    code="T001",
    severity="error",
    description="Unix-epoch sentinel leak: timestamp 1970-01-01 used in real events.",
    check=_check_t001,
)

T002 = Rule(
    code="T002",
    severity="warn",
    description="Future-dated event: timestamp is beyond current UTC time.",
    check=_check_t002,
)

T003 = Rule(
    code="T003",
    severity="error",
    description="Temporal impossibility: event precedes related object's earliest time.",
    check=_check_t003,
)

T004 = Rule(
    code="T004",
    severity="warn",
    description="Non-monotonic event sequence: events of same type for same object go backwards.",
    check=_check_t004,
)

T005 = Rule(
    code="T005",
    severity="warn",
    description="Sub-second timestamp clustering: many events share the same second.",
    check=_check_t005,
)

T006 = Rule(
    code="T006",
    severity="info",
    description="Timestamp granularity mismatch across event types.",
    check=_check_t006,
)

T007 = Rule(
    code="T007",
    severity="warn",
    description="Timezone inconsistency: log mixes UTC, offset, and naive timestamps.",
    check=_check_t007,
)

T008 = Rule(
    code="T008",
    severity="info",
    description="Suspicious timestamp gap exceeding 365 days between consecutive events.",
    check=_check_t008,
)


__all__ = ["T001", "T002", "T003", "T004", "T005", "T006", "T007", "T008"]
