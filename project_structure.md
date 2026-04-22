# Verixa Project Structure

This document describes the implemented MVP repository structure and the responsibility of each module.

## Repository Tree

```text
.
|-- .github/
|   `-- workflows/
|       `-- verixa-check.yml
|-- .gitignore
|-- README.md
|-- implementation_plan.md
|-- project_structure.md
|-- pyproject.toml
|-- verixa.yaml
|-- verixa.risk.yaml.example
|-- workflow.md
|-- src/
|   `-- verixa/
|       |-- __init__.py
|       |-- __main__.py
|       |-- cli/
|       |   |-- __init__.py
|       |   `-- app.py
|       |-- config/
|       |   |-- __init__.py
|       |   |-- errors.py
|       |   `-- loader.py
|       |-- connectors/
|       |   |-- __init__.py
|       |   |-- base.py
|       |   `-- bigquery/
|       |       |-- __init__.py
|       |       |-- connector.py
|       |       |-- queries.py
|       |       `-- types.py
|       |-- contracts/
|       |   |-- __init__.py
|       |   |-- models.py
|       |   `-- normalize.py
|       |-- diff/
|       |   |-- __init__.py
|       |   |-- engine.py
|       |   |-- models.py
|       |   `-- risk.py
|       |-- output/
|       |   |-- __init__.py
|       |   |-- console.py
|       |   |-- exit_codes.py
|       |   `-- json.py
|       |-- rules/
|       |   |-- __init__.py
|       |   |-- accepted_values.py
|       |   |-- freshness.py
|       |   |-- no_nulls.py
|       |   |-- null_rate_change.py
|       |   |-- row_count_change.py
|       |   `-- schema_drift.py
|       |-- snapshot/
|       |   |-- __init__.py
|       |   |-- models.py
|       |   `-- service.py
|       `-- storage/
|           |-- __init__.py
|           |-- filesystem.py
|           `-- json_codec.py
|-- scripts/
|   `-- setup_mock_bigquery.py
|-- examples/
|   `-- mock_data/
|       `-- stripe_transactions.jsonl
`-- tests/
    |-- __init__.py
    |-- cli/
    |   |-- test_check_command.py
    |   |-- test_check_runtime.py
    |   |-- test_check_warning_behavior.py
    |   |-- test_init_command.py
    |   `-- test_verixa_commands.py
    |-- integration/
    |   `-- test_live_bigquery_smoke.py
    `-- unit/
        |-- __init__.py
        |-- test_accepted_values.py
        |-- test_bigquery_connector.py
        |-- test_bigquery_queries.py
        |-- test_bigquery_types.py
        |-- test_config_loader.py
        |-- test_contract_normalize.py
        |-- test_diff_engine.py
        |-- test_freshness.py
        |-- test_no_nulls.py
        |-- test_null_rate_change.py
        |-- test_plan_command.py
        |-- test_row_count_change.py
        |-- test_schema_drift.py
        |-- test_snapshot_command.py
        |-- test_snapshot_service.py
        |-- test_snapshot_storage.py
        `-- test_support.py
```

## Runtime-Generated Files
These are created by the CLI at runtime rather than committed by default.

```text
.verixa/
`-- baseline.json
```

## Public Package Layout
Public branding and entrypoints are Verixa.

This document focuses on the public package layout and command surface.

## Module Responsibilities

### `.github/workflows/`
Repository automation and examples.

Responsibilities:
- provide a GitHub Actions example for running `verixa check --format json`
- show baseline and credentials prerequisites explicitly
- upload the JSON report as a workflow artifact

### `cli/`
Command definitions and argument parsing.

Responsibilities:
- define Typer commands
- translate CLI inputs into service calls
- render top-level success and failure behavior
- support both text and JSON output
- centralize process exit handling
- keep legacy aliases behind the new workflow names

### `config/`
Configuration loading and validation.

Responsibilities:
- load YAML from disk
- validate required fields
- normalize configuration into typed models
- parse project-level rule thresholds
- merge source-level rule overrides on top of project defaults
- return clear errors for invalid config
- prefer Verixa default paths for new projects

### `contracts/`
Canonical contract definitions.

Responsibilities:
- represent sources, schema fields, freshness rules, test definitions, scan windows, warning policies, and threshold configuration
- normalize input forms into one internal representation
- provide stable domain types for the rest of the package

### `connectors/`
Warehouse integration boundary.

Responsibilities:
- define the connector interface
- isolate BigQuery-specific code
- keep warehouse logic out of rules and output modules

### `connectors/bigquery/`
BigQuery-specific implementation.

Responsibilities:
- initialize the official BigQuery client
- parse table references
- retrieve metadata
- build low-cost aggregate queries
- support dry-run byte estimation
- support auth and source-reachability checks for `status` and `doctor`
- convert BigQuery responses into internal snapshot types

### `snapshot/`
Snapshot assembly logic.

Responsibilities:
- collect current observations for configured sources
- build snapshot models from connector responses
- provide a stable shape for storage and diffing
- support conservative parallel source capture

### `storage/`
Local state persistence.

Responsibilities:
- read and write baseline files
- guarantee deterministic JSON serialization
- merge targeted snapshot updates into an existing baseline
- keep file layout simple and inspectable

### `rules/`
Pure validation and heuristic checks.

Responsibilities:
- implement the MVP contract checks
- implement baseline drift heuristics
- honor configured thresholds
- return findings in a consistent format
- stay isolated from CLI and connector details

### `diff/`
Comparison orchestration.

Responsibilities:
- compare baseline, contract, and current state
- aggregate findings per source
- attach optional downstream risk hints
- enforce stale-baseline warnings and CI warning policies

### `output/`
Human-friendly and machine-readable rendering.

Responsibilities:
- format text output clearly
- format JSON output deterministically
- keep success output concise
- centralize exit-code semantics
- expose structured output for `snapshot`, `diff`, `validate`, `check`, `status`, `doctor`, `explain`, and `cost`

### `scripts/`
Repository helper scripts.

Responsibilities:
- create or load demo data into BigQuery for live validation

### `examples/`
Example local assets.

Responsibilities:
- provide mock source data for smoke testing and demos

### `tests/cli/`
Mocked CLI verification.

Responsibilities:
- verify command wiring
- lock in baseline-required and warning-only CI behavior
- lock in JSON output behavior
- verify the Verixa command surface and compatibility aliases

### `tests/unit/`
Pure logic verification.

Responsibilities:
- cover config parsing
- cover diff rules and aggregation
- cover connector query construction and result mapping

### `tests/integration/`
Opt-in live warehouse smoke validation.

Responsibilities:
- run `snapshot`, `validate`, `diff`, `cost`, and `check` against a real BigQuery config only when explicitly enabled
- stay skipped by default
- verify JSON output parses successfully in live mode

## Structure Rules
- Keep connectors isolated.
- Keep rule logic pure where possible.
- Keep CLI modules thin.
- Prefer adding a small rule module over adding branching complexity to one large file.
- Do not let output formatting logic leak into domain logic.
- Do not let BigQuery-specific types leak outside the connector package.
