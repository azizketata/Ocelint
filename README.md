# ocelint

A semantic linter for [OCEL 2.0](https://www.ocel-standard.org/) (Object-Centric Event Log) files.

`ocelint` validates OCEL 2.0 logs against 53 rules across seven categories: structural, referential, temporal, qualifier, logical, complexity, and process-mining readiness. It catches issues that the JSON Schema and SQLite DDL cannot express. Output as plain text, JSON, or SARIF 2.1.0 for CI integration.

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

## Rules (53 implemented, 3 deferred)

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

Currently at the end of Stage 4 (53 rules implemented; deferred rules need format-specific metadata or richer config). See `ocelint_implementation_plan.docx` for the full plan.

## Rule reference

Each rule has its own anchor (e.g. [`#s001`](#s001)) so the VS Code extension can deep-link from the Problems panel.

### S001

**Severity:** error

Duplicate event ID: two or more events share the same ocel:eid.

### S002

**Severity:** error

Duplicate object ID: two or more objects share the same ocel:oid.

### S003

**Severity:** warn

Case-insensitive ID collision: IDs differ only in case.

### S004

**Severity:** error

Event references undeclared event type.

### S005

**Severity:** error

Object references undeclared object type.

### S006

**Severity:** warn

Attribute name not declared in type schema.

### S008

**Severity:** error

Non-ISO-8601 timestamp format.

### S009

**Severity:** warn

Naive timestamp without timezone offset or Z suffix.

### S010

**Severity:** error

Attribute value does not match declared type.

### S011

**Severity:** error

Empty event table: log contains zero events.

### S012

**Severity:** warn

Empty object table: events exist but zero objects declared.

### R001

**Severity:** error

Dangling E2O event reference: relation event ID not in events.

### R002

**Severity:** error

Dangling E2O object reference: relation object ID not in objects.

### R003

**Severity:** error

Dangling O2O reference: source or target object ID not in objects.

### R004

**Severity:** warn

Orphaned event: event has zero E2O relations.

### R005

**Severity:** info

Orphaned object: object is not referenced by any event.

### R006

**Severity:** error

Object type inconsistency: same oid appears with different types.

### R007

**Severity:** warn

Missing required attribute value: declared but absent on entries.

### R008

**Severity:** info

E2O references object of unexpected type (requires expected-types config).

### T001

**Severity:** error

Unix-epoch sentinel leak: timestamp 1970-01-01 used in real events.

### T002

**Severity:** warn

Future-dated event: timestamp is beyond current UTC time.

### T003

**Severity:** error

Temporal impossibility: event precedes related object's earliest time.

### T004

**Severity:** warn

Non-monotonic event sequence: events of same type for same object go backwards.

### T005

**Severity:** warn

Sub-second timestamp clustering: many events share the same second.

### T006

**Severity:** info

Timestamp granularity mismatch across event types.

### T007

**Severity:** warn

Timezone inconsistency: log mixes UTC, offset, and naive timestamps.

### T008

**Severity:** info

Suspicious timestamp gap exceeding 365 days between consecutive events.

### Q001

**Severity:** warn

Empty or blank qualifier on E2O/O2O relation.

### Q002

**Severity:** warn

Qualifier vocabulary inconsistency: similar spellings of same concept.

### Q003

**Severity:** info

Qualifier vocabulary explosion: too many distinct qualifier values.

### Q004

**Severity:** warn

Qualifier contains characters that break CSV/relational round-tripping.

### Q005

**Severity:** info

Qualifier used only once across all relations (likely free-text).

### Q006

**Severity:** warn

Missing qualifier on some E2O of a (event_type, object_type) pair where others have one.

### L001

**Severity:** warn

Symmetry violation: symmetric qualifier present in one direction only.

### L002

**Severity:** error

Cycle on hierarchical qualifier (part-of/parent/contains).

### L004

**Severity:** info

Disconnected object graph component: log contains independent subgraphs.

### L005

**Severity:** info

Object with no events: object exists but has no lifecycle.

### L006

**Severity:** warn

Object-type name synonym collision (e.g. Order vs Orders).

### L007

**Severity:** warn

Self-referencing O2O: object has O2O relation pointing to itself.

### C001

**Severity:** info

Object-type proliferation: too many object types declared.

### C002

**Severity:** warn

High E2O fan-out outlier per event type.

### C003

**Severity:** warn

High E2O fan-in outlier per object type.

### C004

**Severity:** info

Attribute value cardinality explosion (likely an ID stored as attribute).

### C005

**Severity:** warn

Convergence risk: event references multiple objects of same type.

### C006

**Severity:** warn

Divergence risk: object participates in many events of same type.

### C007

**Severity:** info

Event-type to object-type coverage gap.

### P001

**Severity:** error

Event type with zero object relations: OCPN cannot place transition.

### P002

**Severity:** warn

Disconnected object-type subgraph: log packs independent processes.

### P003

**Severity:** info

Insufficient events per object type: underpowered for discovery.

### P004

**Severity:** warn

No identifiable start activity for object type's lifecycle.

### P005

**Severity:** warn

No identifiable end activity for object type's lifecycle.

### P006

**Severity:** warn

Goossens C16 ambiguity: attribute change coincides with multiple events.

### P008

**Severity:** info

Uniform attribute values: zero information content.

