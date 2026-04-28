"""Public SDK for ocelint plugin authors.

Third-party packages can register custom rules via the 'ocelint.rules'
setuptools entry point. Each entry point may resolve to either a single
`Rule` instance or a list of `Rule` instances.

Example pyproject.toml for a plugin package:

    [project.entry-points."ocelint.rules"]
    sap_p2p = "my_plugin:RULES"

Where `my_plugin.RULES` is a `list[Rule]`.
"""

from ocelint.engine import Rule, Severity, Violation
from ocelint.model import OcelLog

__all__ = ["OcelLog", "Rule", "Severity", "Violation"]
