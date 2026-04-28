"""Built-in rule catalog."""

from ocelint.engine import Rule
from ocelint.rules.referential import R001, R002, R003, R004, R005, R006, R007
from ocelint.rules.structural import S001, S002, S003, S004, S005, S006, S008

BUILTIN_RULES: list[Rule] = [
    S001, S002, S003, S004, S005, S006, S008,
    R001, R002, R003, R004, R005, R006, R007,
]

__all__ = [
    "BUILTIN_RULES",
    "R001", "R002", "R003", "R004", "R005", "R006", "R007",
    "S001", "S002", "S003", "S004", "S005", "S006", "S008",
]
