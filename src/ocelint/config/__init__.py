"""Configuration loading and rule filtering for ocelint.

Loads `[tool.ocelint]` from pyproject.toml. Supports `select`, `ignore`,
`extend-select`, `extend-ignore`, and a `severity` override mapping.

Pattern matching:
    - Exact codes match exactly (e.g., "S001").
    - Letter-only patterns (e.g., "S", "OCEL-S") prefix-match all codes that
      start with that letter sequence.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field, replace
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib  # type: ignore[import-not-found]

from ocelint.engine import Rule, Severity

_VALID_SEVERITIES: frozenset[str] = frozenset(("error", "warn", "info"))


@dataclass(frozen=True)
class Config:
    """Effective rule configuration."""

    select: list[str] | None = None
    ignore: list[str] = field(default_factory=list)
    extend_select: list[str] = field(default_factory=list)
    extend_ignore: list[str] = field(default_factory=list)
    severity: dict[str, Severity] = field(default_factory=dict)


class ConfigError(Exception):
    """Raised when pyproject.toml has an invalid `[tool.ocelint]` section."""


def load_config(path: Path | None = None) -> Config:
    """Load `[tool.ocelint]` from a pyproject.toml.

    If `path` is None, looks for `pyproject.toml` in the current directory.
    Returns the default `Config()` if the file is absent or has no
    `[tool.ocelint]` section.
    """
    target = path
    if target is None:
        candidate = Path.cwd() / "pyproject.toml"
        if not candidate.exists():
            return Config()
        target = candidate
    elif not target.exists():
        raise ConfigError(f"config file not found: {target}")

    with target.open("rb") as f:
        data = tomllib.load(f)
    section = data.get("tool", {}).get("ocelint", {})
    if not isinstance(section, dict):
        raise ConfigError(f"{target}: [tool.ocelint] must be a table")

    return Config(
        select=_str_list_or_none(section, "select", target),
        ignore=_str_list(section, "ignore", target),
        extend_select=_str_list(section, "extend-select", target),
        extend_ignore=_str_list(section, "extend-ignore", target),
        severity=_severity_map(section.get("severity", {}), target),
    )


def filter_rules(rules: list[Rule], config: Config) -> list[Rule]:
    """Apply select/ignore filters and severity overrides to a rule list."""
    out: list[Rule] = []
    for rule in rules:
        if not _is_selected(rule.code, config):
            continue
        new_severity = config.severity.get(rule.code, rule.severity)
        out.append(rule if new_severity == rule.severity else replace(rule, severity=new_severity))
    return out


def _is_selected(code: str, config: Config) -> bool:
    if config.select is None:
        in_set = True
    else:
        in_set = any(_pattern_matches(p, code) for p in config.select)
    if not in_set and any(_pattern_matches(p, code) for p in config.extend_select):
        in_set = True
    if not in_set:
        return False
    if any(_pattern_matches(p, code) for p in config.ignore):
        return False
    return not any(_pattern_matches(p, code) for p in config.extend_ignore)


def _pattern_matches(pattern: str, code: str) -> bool:
    if pattern == code:
        return True
    if all(c.isalpha() or c == "-" for c in pattern):
        return code.startswith(pattern)
    return False


def _str_list_or_none(section: dict[str, object], key: str, path: Path) -> list[str] | None:
    if key not in section:
        return None
    return _str_list(section, key, path)


def _str_list(section: dict[str, object], key: str, path: Path) -> list[str]:
    value = section.get(key, [])
    if not isinstance(value, list) or not all(isinstance(x, str) for x in value):
        raise ConfigError(f"{path}: [tool.ocelint].{key} must be a list of strings")
    return list(value)


def _severity_map(value: object, path: Path) -> dict[str, Severity]:
    if not isinstance(value, dict):
        raise ConfigError(f"{path}: [tool.ocelint.severity] must be a table")
    out: dict[str, Severity] = {}
    for code, sev in value.items():
        if not isinstance(code, str):
            raise ConfigError(f"{path}: severity keys must be strings")
        if sev not in _VALID_SEVERITIES:
            raise ConfigError(
                f"{path}: severity for {code!r} must be one of error/warn/info, got {sev!r}"
            )
        out[code] = sev
    return out


__all__ = ["Config", "ConfigError", "filter_rules", "load_config"]
