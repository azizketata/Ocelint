"""Click-based CLI for ocelint."""

from __future__ import annotations

import json as _json
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

import click

from ocelint import __version__
from ocelint.config import ConfigError, filter_rules, load_config, render_init_template
from ocelint.engine import Rule, Violation, max_severity, run_rules
from ocelint.loader import ParseError, load
from ocelint.model import OcelLog
from ocelint.rules import BUILTIN_RULES
from ocelint.rules.referential import make_r008_check

_SEVERITY_TO_SARIF: dict[str, str] = {
    "error": "error",
    "warn": "warning",
    "info": "note",
}
_EXIT_CODE: dict[str, int] = {"error": 2, "warn": 1, "info": 0}


@click.group(help="ocelint - semantic linter for OCEL 2.0 event logs.")
@click.version_option(__version__, "-V", "--version", prog_name="ocelint")
def main() -> None:
    pass


@main.command()
@click.argument(
    "file",
    type=click.Path(exists=False, dir_okay=False, path_type=Path),
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["text", "json", "sarif"]),
    default="text",
    show_default=True,
    help="Output format.",
)
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=False, dir_okay=False, path_type=Path),
    default=None,
    help="Path to pyproject.toml (auto-detected from cwd if omitted).",
)
def lint(file: Path, fmt: str, config_path: Path | None) -> None:
    """Lint an OCEL 2.0 file."""
    try:
        cfg = load_config(config_path)
    except ConfigError as e:
        click.echo(str(e), err=True)
        sys.exit(2)

    rules = filter_rules(BUILTIN_RULES, cfg)
    if cfg.expected_types:
        rules = [
            replace(r, check=make_r008_check(cfg.expected_types)) if r.code == "R008" else r
            for r in rules
        ]

    try:
        log = load(file)
    except ParseError as e:
        click.echo(str(e), err=True)
        sys.exit(2)

    violations = run_rules(log, rules)

    if fmt == "text":
        _print_text(log, violations, rules)
    elif fmt == "json":
        click.echo(_json.dumps(_json_envelope(log, violations), indent=2))
    else:
        click.echo(_json.dumps(_sarif_envelope(log, violations, rules), indent=2))

    sys.exit(_compute_exit_code(violations))


@main.command(name="version")
def version_cmd() -> None:
    """Print the ocelint version."""
    click.echo(__version__)


@main.command(name="list-rules")
def list_rules() -> None:
    """List built-in rules."""
    for rule in BUILTIN_RULES:
        click.echo(f"{rule.code}  {rule.severity:5}  {rule.description}")


@main.command()
@click.argument("code")
def explain(code: str) -> None:
    """Print the description and severity of a rule by code (e.g. 'S001')."""
    code_upper = code.upper()
    matches = [r for r in BUILTIN_RULES if r.code == code_upper]
    if not matches:
        click.echo(f"Unknown rule code: {code!r}. See `ocelint list-rules`.", err=True)
        sys.exit(1)
    rule = matches[0]
    click.echo(f"{rule.code}  ({rule.severity})")
    click.echo(f"  {rule.description}")


@main.command()
def init() -> None:
    """Generate a [tool.ocelint] config block in pyproject.toml."""
    pp = Path("pyproject.toml")
    template = render_init_template(BUILTIN_RULES)

    if not pp.exists():
        pp.write_text(template, encoding="utf-8")
        click.echo("Created pyproject.toml with [tool.ocelint] block.")
        return

    existing = pp.read_text(encoding="utf-8")
    if "[tool.ocelint]" in existing:
        click.echo(
            "pyproject.toml already contains a [tool.ocelint] block; remove it first.",
            err=True,
        )
        sys.exit(1)

    if not existing.endswith("\n"):
        existing += "\n"
    pp.write_text(existing + "\n" + template, encoding="utf-8")
    click.echo("Appended [tool.ocelint] block to pyproject.toml.")


def _compute_exit_code(violations: list[Violation]) -> int:
    sev = max_severity(violations)
    return _EXIT_CODE[sev] if sev is not None else 0


def _summary_dict(log: OcelLog) -> dict[str, Any]:
    return {
        "source_path": str(log.source_path),
        "source_format": log.source_format,
        "events": len(log.events),
        "objects": len(log.objects),
        "relations_e2o": len(log.relations_e2o),
        "relations_o2o": len(log.relations_o2o),
        "event_types": len(log.event_types),
        "object_types": len(log.object_types),
        "attribute_decls": len(log.attribute_decls),
        "parse_warnings": list(log.parse_warnings),
    }


def _violation_dict(v: Violation) -> dict[str, Any]:
    return {
        "code": v.code,
        "severity": v.severity,
        "message": v.message,
        "location": v.location,
    }


def _json_envelope(log: OcelLog, violations: list[Violation]) -> dict[str, Any]:
    summary = _summary_dict(log)
    summary["violations"] = [_violation_dict(v) for v in violations]
    return summary


def _print_text(log: OcelLog, violations: list[Violation], rules: list[Rule]) -> None:
    s = _summary_dict(log)
    click.echo(f"Loaded {s['source_path']} ({s['source_format']})")
    click.echo(f"  events:        {s['events']:>10,}")
    click.echo(f"  objects:       {s['objects']:>10,}")
    click.echo(f"  E2O relations: {s['relations_e2o']:>10,}")
    click.echo(f"  O2O relations: {s['relations_o2o']:>10,}")
    click.echo(f"  event types:   {s['event_types']:>10}")
    click.echo(f"  object types:  {s['object_types']:>10}")
    click.echo(f"  attr decls:    {s['attribute_decls']:>10}")
    click.echo(f"  rules enabled: {len(rules):>10} of {len(BUILTIN_RULES)}")

    warnings = s["parse_warnings"]
    if warnings:
        click.echo(f"\nParse warnings ({len(warnings)}):")
        for w in warnings:
            click.echo(f"  - {w}")

    if violations:
        click.echo(f"\nViolations ({len(violations)}):")
        for v in violations:
            loc = f" [{v.location}]" if v.location else ""
            click.echo(f"  {v.code} {v.severity:5} {v.message}{loc}")
    else:
        click.echo("\nNo violations.")


def _sarif_envelope(
    log: OcelLog, violations: list[Violation], rules: list[Rule]
) -> dict[str, Any]:
    rules_block = [
        {
            "id": rule.code,
            "shortDescription": {"text": rule.description},
            "defaultConfiguration": {"level": _SEVERITY_TO_SARIF[rule.severity]},
        }
        for rule in rules
    ]
    results = [
        {
            "ruleId": v.code,
            "level": _SEVERITY_TO_SARIF[v.severity],
            "message": {"text": v.message},
            "locations": [
                {
                    "physicalLocation": {
                        "artifactLocation": {"uri": str(log.source_path)}
                    },
                    "logicalLocations": (
                        [{"name": v.location}] if v.location else []
                    ),
                }
            ],
        }
        for v in violations
    ]
    return {
        "version": "2.1.0",
        "$schema": "https://json.schemastore.org/sarif-2.1.0.json",
        "runs": [
            {
                "tool": {
                    "driver": {
                        "name": "ocelint",
                        "version": __version__,
                        "informationUri": "https://github.com/azizketata/ocelint",
                        "rules": rules_block,
                    }
                },
                "invocations": [{"executionSuccessful": True}],
                "artifacts": [{"location": {"uri": str(log.source_path)}}],
                "results": results,
            }
        ],
    }


__all__ = ["main"]
