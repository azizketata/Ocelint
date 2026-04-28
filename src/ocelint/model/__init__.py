"""Internal data model for OCEL 2.0 logs."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import pandas as pd

SourceFormat = Literal["json", "xml", "sqlite"]

EVENTS_COLUMNS = ("eid", "etype", "timestamp", "attrs")
OBJECTS_COLUMNS = ("oid", "otype", "attrs")
E2O_COLUMNS = ("eid", "oid", "qualifier")
O2O_COLUMNS = ("source_oid", "target_oid", "qualifier")
EVENT_TYPES_COLUMNS = ("name",)
OBJECT_TYPES_COLUMNS = ("name",)
ATTRIBUTE_DECLS_COLUMNS = ("scope", "type_name", "attribute_name", "attribute_type")


@dataclass
class OcelLog:
    """Parsed OCEL 2.0 log in normalized DataFrame form.

    Events `attrs` cell: dict[attr_name, value] (fixed at event time).
    Objects `attrs` cell: dict[attr_name, list[tuple[timestamp, value]]]
    (OCEL 2.0 object attributes are time-varying).
    `attribute_decls.scope` is "event" or "object".
    """

    events: pd.DataFrame
    objects: pd.DataFrame
    relations_e2o: pd.DataFrame
    relations_o2o: pd.DataFrame
    event_types: pd.DataFrame
    object_types: pd.DataFrame
    attribute_decls: pd.DataFrame
    source_format: SourceFormat
    source_path: Path
    parse_warnings: list[str] = field(default_factory=list)


__all__ = [
    "ATTRIBUTE_DECLS_COLUMNS",
    "E2O_COLUMNS",
    "EVENTS_COLUMNS",
    "EVENT_TYPES_COLUMNS",
    "O2O_COLUMNS",
    "OBJECTS_COLUMNS",
    "OBJECT_TYPES_COLUMNS",
    "OcelLog",
    "SourceFormat",
]
