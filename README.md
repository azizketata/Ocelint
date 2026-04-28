# ocelint

A semantic linter for [OCEL 2.0](https://www.ocel-standard.org/) (Object-Centric Event Log) files.

`ocelint` validates OCEL 2.0 logs against 39+ rules across seven categories: structural, referential, temporal, qualifier, logical, complexity, and process-mining readiness. It catches issues that the JSON Schema and SQLite DDL cannot express. Output as plain text, JSON, or SARIF 2.1.0 for CI integration.

## Install

Not yet on PyPI. From a checkout:

```bash
pip install -e .[dev]
```

Runtime deps include `click`, `lxml`, `pandas`, `rapidfuzz`, `rich`, `jsonschema`. On Python 3.10, `tomli` is added automatically for config parsing.

## Usage

```bash
ocelint lint path/to/log.json
ocelint lint path/to/log.xml --format sarif
ocelint lint path/to/log.sqlite --format json
ocelint lint --config pyproject.toml path/to/log.json
ocelint list-rules
ocelint explain S001
ocelint init   # generate [tool.ocelint] template in pyproject.toml
```

Exit codes follow ESLint/SARIF semantics: `0` (clean or info-only), `1` (warnings present), `2` (errors present, or parse failure).

## Configuration

In `pyproject.toml`:

```toml
[tool.ocelint]
# Optional explicit selection. Letter-only patterns ("S", "OCEL-S") prefix-match.
# select = ["S", "R001"]
ignore = ["S009"]
extend-ignore = ["R005"]

[tool.ocelint.severity]
S006 = "info"      # Downgrade an annoying rule

[tool.ocelint.expected-types]
"Create Order" = ["Order"]   # Powers R008
```

## Rules (39 implemented)

| Prefix | Category | Codes |
| --- | --- | --- |
| **S** | Structural / serialization | S001, S002, S003, S004, S005, S006, S008, S009, S010, S011, S012 (S007 deferred) |
| **R** | Referential integrity | R001, R002, R003, R004, R005, R006, R007, R008 |
| **T** | Temporal coherence | T001, T002, T003, T004, T005, T006, T007, T008 |
| **Q** | Qualifier hygiene | Q001, Q002, Q003, Q004, Q005, Q006 |
| **L** | Logical / structural-semantic | L001, L002, L004, L005, L006, L007 (L003 needs config) |
| **C** | Complexity / convergence-divergence | C001, C002, C003, C004, C005, C006, C007 |
| **P** | Process-mining readiness | P001, P002, P003, P004, P005, P006, P008 (P007 needs format metadata) |

`ocelint explain <code>` prints a one-liner per rule. See the implementation plan for full descriptions.

## Plugins

Custom rules ship via the `ocelint.rules` setuptools entry-point group:

```toml
# In your plugin's pyproject.toml
[project.entry-points."ocelint.rules"]
sap_p2p = "my_plugin:RULES"      # exports a list[Rule]
```

Plugin authors import from `ocelint.sdk`:

```python
from ocelint.sdk import OcelLog, Rule, Violation
```

## Pre-commit hook

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/azizketata/ocelint
    rev: <ref>
    hooks:
      - id: ocelint
```

## Development

```bash
pytest                                          # run tests
pytest --cov=ocelint --cov-fail-under=80        # tests + coverage gate
mypy src/ocelint                                # strict type-check
ruff check .                                    # lint
```

CI workflow (`.github/workflows/ci.yml`) runs all of the above on Python 3.10/3.11/3.12.

## License

MIT. See `LICENSE` (forthcoming).

## Roadmap

| Stage | Scope | Release | Rules |
| --- | --- | --- | --- |
| 0 | Scaffold + Loader (JSON/XML/SQLite) | v0.1.0 | 0 |
| 1 | Rule engine + S/R rules + config + CLI | v0.2.0 | 19 |
| 2 | Temporal + qualifier rules | v0.3.0 | 33 |
| 3 | Logical + complexity rules | v0.4.0 | 46 |
| 4 | Process-mining readiness + plugin SDK | v0.5.0 | 53 |
| 5 | Documentation, benchmarks, PyPI release | v1.0.0 | 56+ |

Currently at the end of Stage 4 (39 rules implemented; deferred rules need format-specific metadata or richer config). See `ocelint_implementation_plan.docx` for the full plan.
