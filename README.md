# ocelint

A semantic linter for [OCEL 2.0](https://www.ocel-standard.org/) (Object-Centric Event Log) files.

`ocelint` validates OCEL 2.0 logs against ~56 rules across seven categories — structural, referential, temporal, qualifier, logical, complexity, and process-mining readiness — covering issues that the JSON Schema and SQLite DDL cannot express. Output as plain text, JSON, or SARIF 2.1.0 for CI integration.

## Status

**Stage 0 — Scaffolding (v0.1.0)**

Currently in development. This release establishes the project skeleton, internal data model, and the three-format OCEL 2.0 loader (JSON, XML, SQLite). No rule logic yet — the CLI loads and echoes back well-formed OCEL files. Rule categories are implemented in subsequent stages per the implementation plan.

## Install

Not yet on PyPI. From a checkout:

```bash
pip install -e .[dev]
```

Optional extras: `[pm4py]` (alternative loader), `[graph]` (networkx for L002/L004), `[numpy]` (vectorized stats for OCEL-C).

## Usage

```bash
ocelint lint path/to/log.json
ocelint lint path/to/log.xml --format sarif
ocelint lint path/to/log.sqlite --format json
```

## Development

```bash
pytest                # run tests
mypy src/ocelint      # type-check
ruff check .          # lint
ruff format .         # format
```

## License

MIT. See `LICENSE` (forthcoming).

## Roadmap

| Stage | Scope                                   | Release | Rules     |
| ----- | --------------------------------------- | ------- | --------- |
| 0     | Scaffold + Loader                       | v0.1.0  | 0         |
| 1     | Rule engine + structural/referential    | v0.2.0  | 20        |
| 2     | Temporal + qualifier                    | v0.3.0  | 34        |
| 3     | Logical + complexity                    | v0.4.0  | 48        |
| 4     | Process-mining readiness + docs         | v0.5.0  | 56        |
| 5     | CI integration + plugin SDK + PyPI      | v1.0.0  | 56+       |

See `ocelint_implementation_plan.docx` for the full plan.
