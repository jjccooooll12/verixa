# DataGuard Project Structure

This document describes the implemented MVP repository structure and the responsibility of each module.

## Repository Tree

```text
.
├── .github/
│   └── workflows/
│       └── dataguard-check.yml
├── .gitignore
├── README.md
├── implementation_plan.md
├── project_structure.md
├── pyproject.toml
├── workflow.md
├── src/
│   └── dataguard/
│       ├── __init__.py
│       ├── __main__.py
│       ├── cli/
│       │   ├── __init__.py
│       │   ├── app.py
│       │   ├── check.py
│       │   ├── init.py
│       │   ├── plan.py
│       │   ├── snapshot.py
│       │   └── test.py
│       ├── config/
│       │   ├── __init__.py
│       │   ├── errors.py
│       │   └── loader.py
│       ├── connectors/
│       │   ├── __init__.py
│       │   ├── base.py
│       │   └── bigquery/
│       │       ├── __init__.py
│       │       ├── connector.py
│       │       ├── queries.py
│       │       └── types.py
│       ├── contracts/
│       │   ├── __init__.py
│       │   ├── models.py
│       │   └── normalize.py
│       ├── diff/
│       │   ├── __init__.py
│       │   ├── engine.py
│       │   ├── models.py
│       │   └── risk.py
│       ├── output/
│       │   ├── __init__.py
│       │   ├── console.py
│       │   ├── exit_codes.py
│       │   └── json.py
│       ├── rules/
│       │   ├── __init__.py
│       │   ├── accepted_values.py
│       │   ├── freshness.py
│       │   ├── no_nulls.py
│       │   ├── null_rate_change.py
│       │   ├── row_count_change.py
│       │   └── schema_drift.py
│       ├── snapshot/
│       │   ├── __init__.py
│       │   ├── models.py
│       │   └── service.py
│       └── storage/
│           ├── __init__.py
│           ├── filesystem.py
│           └── json_codec.py
└── tests/
    ├── __init__.py
    ├── cli/
    │   ├── test_check_command.py
    │   ├── test_check_runtime.py
    │   ├── test_check_warning_behavior.py
    │   └── test_init_command.py
    ├── integration/
    │   └── test_live_bigquery_smoke.py
    └── unit/
        ├── __init__.py
        ├── test_accepted_values.py
        ├── test_bigquery_connector.py
        ├── test_bigquery_queries.py
        ├── test_bigquery_types.py
        ├── test_config_loader.py
        ├── test_contract_normalize.py
        ├── test_diff_engine.py
        ├── test_freshness.py
        ├── test_no_nulls.py
        ├── test_null_rate_change.py
        ├── test_plan_command.py
        ├── test_row_count_change.py
        ├── test_schema_drift.py
        ├── test_snapshot_command.py
        ├── test_snapshot_storage.py
        └── test_support.py
```

## Runtime-Generated Files
These are created by the CLI at runtime rather than committed by default.

```text
.dataguard/
└── baseline.json

dataguard.yaml
dataguard.risk.yaml.example
```

## Module Responsibilities

### `.github/workflows/`
Repository automation and examples.

Responsibilities:
- provide a GitHub Actions example for running `dataguard check --format json`,
- show baseline and credentials prerequisites explicitly,
- upload the JSON report as a workflow artifact.

### `cli/`
Command definitions and argument parsing.

Responsibilities:
- define Typer commands,
- translate CLI inputs into service calls,
- render top-level success and failure behavior,
- support both text and JSON output,
- centralize process exit handling.

### `config/`
Configuration loading and validation.

Responsibilities:
- load YAML from disk,
- validate required fields,
- normalize configuration into typed models,
- parse project-level rule thresholds,
- merge source-level rule overrides on top of project defaults,
- return clear errors for invalid config.

### `contracts/`
Canonical contract definitions.

Responsibilities:
- represent sources, schema fields, freshness rules, test definitions, and threshold configuration,
- normalize input forms into one internal representation,
- provide stable domain types for the rest of the package.

### `connectors/`
Warehouse integration boundary.

Responsibilities:
- define the connector interface,
- isolate BigQuery-specific code,
- keep warehouse logic out of rules and output modules.

### `connectors/bigquery/`
BigQuery-specific implementation.

Responsibilities:
- initialize the official BigQuery client,
- parse table references,
- retrieve metadata,
- build low-cost aggregate queries,
- convert BigQuery responses into internal snapshot types.

### `snapshot/`
Snapshot assembly logic.

Responsibilities:
- collect current observations for configured sources,
- build snapshot models from connector responses,
- provide a stable shape for storage and diffing.

### `storage/`
Local state persistence.

Responsibilities:
- read and write baseline files,
- guarantee deterministic JSON serialization,
- keep file layout simple and inspectable.

### `rules/`
Pure validation and heuristic checks.

Responsibilities:
- implement the MVP contract checks,
- implement baseline drift heuristics,
- honor configured thresholds,
- return findings in a consistent format,
- stay isolated from CLI and connector details.

### `diff/`
Comparison orchestration.

Responsibilities:
- compare baseline, contract, and current state,
- aggregate findings per source,
- attach optional downstream risk hints.

### `output/`
Human-friendly and machine-readable rendering.

Responsibilities:
- format text output clearly,
- format JSON output deterministically,
- keep success output concise,
- centralize exit-code semantics.

### `tests/cli/`
Mocked CLI verification.

Responsibilities:
- verify command wiring,
- lock in baseline-required and warning-only CI behavior,
- lock in JSON output behavior.

### `tests/unit/`
Pure logic verification.

Responsibilities:
- cover config parsing,
- cover diff rules and aggregation,
- cover connector query construction and result mapping.

### `tests/integration/`
Opt-in live warehouse smoke validation.

Responsibilities:
- run `snapshot`, `plan`, and `check` against a real BigQuery config only when explicitly enabled,
- stay skipped by default,
- verify JSON output parses successfully in live mode.

## Structure Rules
- Keep connectors isolated.
- Keep rule logic pure where possible.
- Keep CLI modules thin.
- Prefer adding a small rule module over adding branching complexity to one large file.
- Do not let output formatting logic leak into domain logic.
- Do not let BigQuery-specific types leak outside the connector package.
