"""Built-in rule catalog."""

from ocelint.engine import Rule
from ocelint.rules.referential import R001, R002, R003, R004, R005, R006, R007, R008
from ocelint.rules.structural import (
    S001,
    S002,
    S003,
    S004,
    S005,
    S006,
    S008,
    S009,
    S010,
    S011,
    S012,
)

BUILTIN_RULES: list[Rule] = [
    S001, S002, S003, S004, S005, S006, S008, S009, S010, S011, S012,
    R001, R002, R003, R004, R005, R006, R007, R008,
]

__all__ = [
    "BUILTIN_RULES",
    "R001", "R002", "R003", "R004", "R005", "R006", "R007", "R008",
    "S001", "S002", "S003", "S004", "S005", "S006", "S008",
    "S009", "S010", "S011", "S012",
]
