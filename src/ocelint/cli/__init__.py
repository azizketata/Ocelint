"""Click-based CLI for ocelint."""

from __future__ import annotations

import json as _json
import sys
from pathlib import Path
from typing import Any

import click

from ocelint import __version__
from ocelint.engine import Violation, max_severity, run_rules
from ocelint.loader import ParseError, load
from ocelint.model import OcelLog
from ocelint.rules import BUILTIN_RULES

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
def lint(file: Path, fmt: str) -> None:
    """Lint an OCEL 2.0 file."""
    try:
        log = load(file)
    except ParseError as e:
        click.echo(str(e), err=True)
        sys.exit(2)

    violations = run_rules(log, BUILTIN_RULES)

    if fmt == "text":
        _print_text(log, violations)
    elif fmt == "json":
        click.echo(_json.dumps(_json_envelope(log, violations), indent=2))
    else:
        click.echo(_json.dumps(_sarif_envelope(log, violations), indent=2))

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


def _print_text(log: OcelLog, violations: list[Violation]) -> None:
    s = _summary_dict(log)
    click.echo(f"Loaded {s['source_path']} ({s['source_format']})")
    click.echo(f"  events:        {s['events']:>10,}")
    click.echo(f"  objects:       {s['objects']:>10,}")
    click.echo(f"  E2O relations: {s['relations_e2o']:>10,}")
    click.echo(f"  O2O relations: {s['relations_o2o']:>10,}")
    click.echo(f"  event types:   {s['event_types']:>10}")
    click.echo(f"  object types:  {s['object_types']:>10}")
    click.echo(f"  attr decls:    {s['attribute_decls']:>10}")

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


def _sarif_envelope(log: OcelLog, violations: list[Violation]) -> dict[str, Any]:
    rules_block = [
        {
            "id": rule.code,
            "shortDescription": {"text": rule.description},
            "defaultConfiguration": {"level": _SEVERITY_TO_SARIF[rule.severity]},
        }
        for rule in BUILTIN_RULES
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
