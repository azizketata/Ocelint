"""Built-in rule catalog."""

from ocelint.engine import Rule
from ocelint.rules.complexity import C001, C002, C003, C004, C005, C006, C007
from ocelint.rules.logical import L001, L002, L004, L005, L006, L007
from ocelint.rules.pm_readiness import P001, P002, P003, P004, P005, P006, P008
from ocelint.rules.qualifier import Q001, Q002, Q003, Q004, Q005, Q006
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
from ocelint.rules.temporal import T001, T002, T003, T004, T005, T006, T007, T008

BUILTIN_RULES: list[Rule] = [
    S001, S002, S003, S004, S005, S006, S008, S009, S010, S011, S012,
    R001, R002, R003, R004, R005, R006, R007, R008,
    T001, T002, T003, T004, T005, T006, T007, T008,
    Q001, Q002, Q003, Q004, Q005, Q006,
    L001, L002, L004, L005, L006, L007,
    C001, C002, C003, C004, C005, C006, C007,
    P001, P002, P003, P004, P005, P006, P008,
]

__all__ = [
    "BUILTIN_RULES",
    "C001", "C002", "C003", "C004", "C005", "C006", "C007",
    "L001", "L002", "L004", "L005", "L006", "L007",
    "P001", "P002", "P003", "P004", "P005", "P006", "P008",
    "Q001", "Q002", "Q003", "Q004", "Q005", "Q006",
    "R001", "R002", "R003", "R004", "R005", "R006", "R007", "R008",
    "S001", "S002", "S003", "S004", "S005", "S006", "S008",
    "S009", "S010", "S011", "S012",
    "T001", "T002", "T003", "T004", "T005", "T006", "T007", "T008",
]
