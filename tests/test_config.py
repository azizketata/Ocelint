"""Tests for ocelint.config."""

from __future__ import annotations

from pathlib import Path

import pytest

from ocelint.config import Config, ConfigError, filter_rules, load_config
from ocelint.engine import Rule, Violation
from ocelint.rules import BUILTIN_RULES


def _no_op_check(_log: object) -> "list[Violation]":
    return []


def _make_rule(code: str, severity: str = "warn") -> Rule:
    return Rule(
        code=code,
        severity=severity,
        description=f"test {code}",
        check=_no_op_check,
    )


# --- pattern matching -----------------------------------------------------


def test_filter_no_config_keeps_all() -> None:
    rules = [_make_rule("S001"), _make_rule("R001"), _make_rule("S002")]
    assert filter_rules(rules, Config()) == rules


def test_filter_select_exact_codes() -> None:
    rules = [_make_rule("S001"), _make_rule("R001"), _make_rule("S002")]
    cfg = Config(select=["S001", "R001"])
    out = filter_rules(rules, cfg)
    assert [r.code for r in out] == ["S001", "R001"]


def test_filter_select_letter_prefix() -> None:
    rules = [_make_rule("S001"), _make_rule("R001"), _make_rule("S002")]
    cfg = Config(select=["S"])
    out = filter_rules(rules, cfg)
    assert {r.code for r in out} == {"S001", "S002"}


def test_filter_ignore_subtracts() -> None:
    rules = [_make_rule("S001"), _make_rule("S002"), _make_rule("R001")]
    cfg = Config(ignore=["S001"])
    out = filter_rules(rules, cfg)
    assert {r.code for r in out} == {"S002", "R001"}


def test_filter_ignore_prefix() -> None:
    rules = [_make_rule("S001"), _make_rule("S002"), _make_rule("R001")]
    cfg = Config(ignore=["S"])
    out = filter_rules(rules, cfg)
    assert {r.code for r in out} == {"R001"}


def test_filter_extend_select_adds_to_default() -> None:
    """With no `select`, all rules are in by default; extend-select is a no-op there."""
    rules = [_make_rule("S001"), _make_rule("R001")]
    cfg = Config(extend_select=["X"])
    assert filter_rules(rules, cfg) == rules


def test_filter_extend_select_adds_to_explicit_select() -> None:
    rules = [_make_rule("S001"), _make_rule("R001"), _make_rule("S002")]
    cfg = Config(select=["S"], extend_select=["R001"])
    out = filter_rules(rules, cfg)
    assert {r.code for r in out} == {"S001", "S002", "R001"}


def test_filter_extend_ignore_stacks() -> None:
    rules = [_make_rule("S001"), _make_rule("S002"), _make_rule("R001")]
    cfg = Config(ignore=["S001"], extend_ignore=["R001"])
    out = filter_rules(rules, cfg)
    assert [r.code for r in out] == ["S002"]


def test_filter_severity_override() -> None:
    rules = [_make_rule("S001", severity="error")]
    cfg = Config(severity={"S001": "warn"})
    out = filter_rules(rules, cfg)
    assert out[0].severity == "warn"
    assert out[0].code == "S001"


def test_filter_severity_no_change_returns_same_object() -> None:
    rule = _make_rule("S001", severity="error")
    cfg = Config(severity={"S001": "error"})
    out = filter_rules([rule], cfg)
    assert out[0] is rule


# --- loading from pyproject.toml -----------------------------------------


def _write_pyproject(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "pyproject.toml"
    p.write_text(body, encoding="utf-8")
    return p


def test_load_config_full_section(tmp_path: Path) -> None:
    p = _write_pyproject(
        tmp_path,
        """
[tool.ocelint]
select = ["S", "R001"]
ignore = ["S009"]
extend-select = ["R002"]
extend-ignore = ["S012"]

[tool.ocelint.severity]
R005 = "warn"
""",
    )
    cfg = load_config(p)
    assert cfg.select == ["S", "R001"]
    assert cfg.ignore == ["S009"]
    assert cfg.extend_select == ["R002"]
    assert cfg.extend_ignore == ["S012"]
    assert cfg.severity == {"R005": "warn"}


def test_load_config_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "absent.toml")


def test_load_config_no_section_returns_defaults(tmp_path: Path) -> None:
    p = _write_pyproject(tmp_path, '[project]\nname = "x"\n')
    cfg = load_config(p)
    assert cfg == Config()


def test_load_config_invalid_select_type(tmp_path: Path) -> None:
    p = _write_pyproject(tmp_path, '[tool.ocelint]\nselect = "not-a-list"\n')
    with pytest.raises(ConfigError, match="select"):
        load_config(p)


def test_load_config_invalid_severity_value(tmp_path: Path) -> None:
    p = _write_pyproject(
        tmp_path,
        """
[tool.ocelint.severity]
S001 = "critical"
""",
    )
    with pytest.raises(ConfigError, match="severity"):
        load_config(p)


def test_load_config_no_path_no_pyproject(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """When no pyproject.toml in cwd, defaults silently."""
    monkeypatch.chdir(tmp_path)
    assert load_config() == Config()


def test_load_config_no_path_with_pyproject(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_pyproject(tmp_path, '[tool.ocelint]\nselect = ["S001"]\n')
    monkeypatch.chdir(tmp_path)
    cfg = load_config()
    assert cfg.select == ["S001"]


# --- end-to-end against BUILTIN_RULES ------------------------------------


def test_filter_select_S_keeps_only_s_rules() -> None:
    out = filter_rules(BUILTIN_RULES, Config(select=["S"]))
    assert all(r.code.startswith("S") for r in out)
    assert len(out) > 0


def test_filter_ignore_R_drops_all_r_rules() -> None:
    out = filter_rules(BUILTIN_RULES, Config(ignore=["R"]))
    assert all(not r.code.startswith("R") for r in out)
