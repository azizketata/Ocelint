"""Tests for ocelint.cli."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from ocelint import __version__
from ocelint.cli import main


@pytest.fixture
def empty_log(tmp_path: Path) -> Path:
    """Minimally-clean log: one event referencing one object, no rule violations."""
    payload = {
        "eventTypes": [{"name": "T", "attributes": []}],
        "objectTypes": [{"name": "Order", "attributes": []}],
        "events": [
            {
                "id": "e1",
                "type": "T",
                "time": "2026-01-01T00:00:00Z",
                "attributes": [],
                "relationships": [{"objectId": "o1", "qualifier": "creates"}],
            }
        ],
        "objects": [{"id": "o1", "type": "Order", "attributes": [], "relationships": []}],
    }
    p = tmp_path / "log.json"
    p.write_text(json.dumps(payload), encoding="utf-8")
    return p


def test_version_subcommand() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["version"])
    assert result.exit_code == 0
    assert result.output.strip() == __version__


def test_version_flag() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.output


def test_lint_text_clean(empty_log: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["lint", str(empty_log)])
    assert result.exit_code == 0
    assert "events:" in result.output
    assert "(json)" in result.output


def test_lint_json_output(empty_log: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["lint", str(empty_log), "--format", "json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["source_format"] == "json"
    assert payload["events"] == 1
    assert payload["objects"] == 1
    assert payload["violations"] == []
    assert payload["parse_warnings"] == []


def test_lint_sarif_output(empty_log: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["lint", str(empty_log), "--format", "sarif"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["version"] == "2.1.0"
    run = payload["runs"][0]
    assert run["tool"]["driver"]["name"] == "ocelint"
    assert run["tool"]["driver"]["version"] == __version__
    assert run["results"] == []


def test_lint_missing_file(tmp_path: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["lint", str(tmp_path / "absent.json")])
    assert result.exit_code == 2
    assert "does not exist" in result.stderr


def test_lint_malformed_returns_2(tmp_path: Path) -> None:
    p = tmp_path / "broken.json"
    p.write_text("{not json", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(main, ["lint", str(p)])
    assert result.exit_code == 2
    assert "invalid JSON" in result.stderr


def test_lint_invalid_format(empty_log: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["lint", str(empty_log), "--format", "xml"])
    assert result.exit_code != 0


@pytest.fixture
def log_with_dup_event(tmp_path: Path) -> Path:
    payload = {
        "eventTypes": [{"name": "Foo", "attributes": []}],
        "objectTypes": [],
        "events": [
            {"id": "e1", "type": "Foo", "time": "2026-01-01T00:00:00Z",
             "attributes": [], "relationships": []},
            {"id": "e1", "type": "Foo", "time": "2026-01-02T00:00:00Z",
             "attributes": [], "relationships": []},
        ],
        "objects": [],
    }
    p = tmp_path / "dup.json"
    p.write_text(json.dumps(payload), encoding="utf-8")
    return p


def test_list_rules_shows_builtin() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["list-rules"])
    assert result.exit_code == 0
    assert "S001" in result.output
    assert "S002" in result.output


def test_lint_exit_2_on_error_violation(log_with_dup_event: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["lint", str(log_with_dup_event)])
    assert result.exit_code == 2
    assert "S001" in result.output
    assert "e1" in result.output


def test_lint_text_no_violations_message(empty_log: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["lint", str(empty_log)])
    assert result.exit_code == 0
    assert "No violations" in result.output


def test_lint_json_includes_violations(log_with_dup_event: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["lint", str(log_with_dup_event), "--format", "json"])
    assert result.exit_code == 2
    payload = json.loads(result.output)
    codes = {v["code"] for v in payload["violations"]}
    assert "S001" in codes
    s001 = next(v for v in payload["violations"] if v["code"] == "S001")
    assert s001["severity"] == "error"


def test_lint_sarif_includes_results_and_rules(log_with_dup_event: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["lint", str(log_with_dup_event), "--format", "sarif"])
    assert result.exit_code == 2
    payload = json.loads(result.output)
    run = payload["runs"][0]
    rule_ids = {r["id"] for r in run["tool"]["driver"]["rules"]}
    assert {"S001", "S002", "R001"} <= rule_ids
    result_rules = {r["ruleId"] for r in run["results"]}
    assert "S001" in result_rules


def test_lint_with_config_ignore_suppresses_rule(
    tmp_path: Path, log_with_dup_event: Path
) -> None:
    cfg = tmp_path / "pyproject.toml"
    cfg.write_text('[tool.ocelint]\nignore = ["S001"]\n', encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["lint", str(log_with_dup_event), "--config", str(cfg), "--format", "json"],
    )
    payload = json.loads(result.output)
    codes = {v["code"] for v in payload["violations"]}
    assert "S001" not in codes


def test_lint_with_config_select_only_keeps_listed(
    tmp_path: Path, log_with_dup_event: Path
) -> None:
    cfg = tmp_path / "pyproject.toml"
    cfg.write_text('[tool.ocelint]\nselect = ["R001"]\n', encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["lint", str(log_with_dup_event), "--config", str(cfg), "--format", "json"],
    )
    payload = json.loads(result.output)
    codes = {v["code"] for v in payload["violations"]}
    assert codes <= {"R001"}


def test_lint_with_config_severity_override_changes_exit_code(
    tmp_path: Path, log_with_dup_event: Path
) -> None:
    """Downgrading S001 from error to info changes the rule's contribution to exit code."""
    cfg = tmp_path / "pyproject.toml"
    cfg.write_text(
        '[tool.ocelint]\nselect = ["S001"]\n[tool.ocelint.severity]\nS001 = "info"\n',
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(
        main, ["lint", str(log_with_dup_event), "--config", str(cfg), "--format", "json"]
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert all(v["severity"] == "info" for v in payload["violations"] if v["code"] == "S001")


def test_lint_with_invalid_config_exits_2(
    tmp_path: Path, log_with_dup_event: Path
) -> None:
    cfg = tmp_path / "pyproject.toml"
    cfg.write_text('[tool.ocelint]\nselect = "not-a-list"\n', encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(
        main, ["lint", str(log_with_dup_event), "--config", str(cfg)]
    )
    assert result.exit_code == 2
    assert "select" in result.stderr


# --- init subcommand -----------------------------------------------------


def test_init_creates_new_pyproject(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(main, ["init"])
    assert result.exit_code == 0
    pp = tmp_path / "pyproject.toml"
    assert pp.exists()
    body = pp.read_text(encoding="utf-8")
    assert "[tool.ocelint]" in body
    assert "[tool.ocelint.severity]" in body
    assert "S001" in body


def test_init_appends_to_existing_pyproject(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pp = tmp_path / "pyproject.toml"
    pp.write_text('[project]\nname = "x"\n', encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(main, ["init"])
    assert result.exit_code == 0
    body = pp.read_text(encoding="utf-8")
    assert '[project]' in body
    assert "[tool.ocelint]" in body


def test_init_refuses_when_section_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pp = tmp_path / "pyproject.toml"
    pp.write_text('[tool.ocelint]\nselect = ["S001"]\n', encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(main, ["init"])
    assert result.exit_code == 1
    assert "already contains" in result.stderr


# --- R008 with expected-types config -------------------------------------


@pytest.fixture
def log_with_e2o(tmp_path: Path) -> Path:
    payload = {
        "eventTypes": [{"name": "Create Order", "attributes": []}],
        "objectTypes": [
            {"name": "Order", "attributes": []},
            {"name": "Invoice", "attributes": []},
        ],
        "events": [
            {"id": "e1", "type": "Create Order", "time": "2026-01-01T00:00:00Z",
             "attributes": [],
             "relationships": [
                 {"objectId": "o1", "qualifier": "creates"},
                 {"objectId": "i1", "qualifier": "creates"},
             ]},
        ],
        "objects": [
            {"id": "o1", "type": "Order", "attributes": [], "relationships": []},
            {"id": "i1", "type": "Invoice", "attributes": [], "relationships": []},
        ],
    }
    p = tmp_path / "log.json"
    p.write_text(json.dumps(payload), encoding="utf-8")
    return p


def test_r008_silent_without_config(log_with_e2o: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["lint", str(log_with_e2o), "--format", "json"])
    payload = json.loads(result.output)
    assert all(v["code"] != "R008" for v in payload["violations"])


def test_r008_fires_on_unexpected_type(
    tmp_path: Path, log_with_e2o: Path
) -> None:
    cfg = tmp_path / "ocelint-config.toml"
    cfg.write_text(
        """
[tool.ocelint]
[tool.ocelint.expected-types]
"Create Order" = ["Order"]
""",
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(
        main, ["lint", str(log_with_e2o), "--config", str(cfg), "--format", "json"]
    )
    payload = json.loads(result.output)
    r008s = [v for v in payload["violations"] if v["code"] == "R008"]
    assert len(r008s) == 1
    assert "Invoice" in r008s[0]["message"]
    assert r008s[0]["severity"] == "info"


def test_r008_silent_when_types_match(
    tmp_path: Path, log_with_e2o: Path
) -> None:
    cfg = tmp_path / "ocelint-config.toml"
    cfg.write_text(
        """
[tool.ocelint]
[tool.ocelint.expected-types]
"Create Order" = ["Order", "Invoice"]
""",
        encoding="utf-8",
    )
    runner = CliRunner()
    result = runner.invoke(
        main, ["lint", str(log_with_e2o), "--config", str(cfg), "--format", "json"]
    )
    payload = json.loads(result.output)
    assert all(v["code"] != "R008" for v in payload["violations"])
