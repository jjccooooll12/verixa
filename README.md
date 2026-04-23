# Verixa

Verixa is a developer-first Data CI CLI for warehouse source tables.

It is built for pre-deploy safety, not reactive monitoring. Verixa compares declared source contracts, a stored baseline snapshot, and current warehouse state to surface likely breakages before a change ships.

## Positioning
Verixa is:
- contract-first
- diff-driven
- warehouse-native
- CLI-first
- CI-friendly

Verixa is not:
- a dashboard-first observability platform
- a lineage visualization tool
- anomaly-detection infrastructure
- just a linter

## Current Scope
Implemented now:
- BigQuery support for the full core workflow
- Snowflake support for `init`, `snapshot`, `diff`, `validate`, `check`, `status`, `doctor`, and `explain`
- local deterministic baseline snapshots
- contract checks and baseline drift heuristics
- lightweight numeric distribution summaries for declared numeric columns
- low-noise numeric p50/p95 drift detection from stored baselines
- text and JSON output
- source scoping with `--source`
- changed-file source targeting with `verixa.targets.yaml`, `--changed-file`, and `--changed-against`
- optional dbt-manifest lineage for changed-file source targeting
- BigQuery byte estimation with `verixa cost` and `--estimate-bytes`
- Snowflake query-history usage reporting with `verixa cost --history-window`
- max-bytes-billed enforcement for live BigQuery queries
- status and diagnostic commands
- CI-friendly exit codes

Current local suite status:
- `128 passed, 2 skipped`

## Install

### Development install
```bash
pip install -e .[dev]
```

### Run without installing
```bash
PYTHONPATH=src python -m verixa --help
```

## BigQuery Authentication
Verixa uses the official Google Cloud BigQuery Python client.

Supported auth paths:
- Application Default Credentials
- `GOOGLE_APPLICATION_CREDENTIALS` pointing at a service account key
- workload identity supported by the BigQuery client

The CLI does not manage auth for you.

For repeatable local testing, the repository also includes:
- `scripts/setup_mock_snowflake.py`
- `tests/integration/test_live_snowflake_smoke.py`

## Snowflake Authentication
Verixa uses the official Snowflake Python connector.

Supported auth paths:
- named Snowflake connections via `warehouse.connection_name`
- direct account/user configuration with `warehouse.account` and `warehouse.user`
- optional password lookup via `warehouse.password_env`
- optional `authenticator`, `warehouse_name`, `database`, `schema`, and `role`

The CLI does not manage auth for you.

## Core Workflow
Recommended workflow:

```bash
verixa init
verixa validate
verixa snapshot
verixa diff
verixa check --fail-on-error
```

Command intent:
- `verixa validate`: check current live data against declared contracts
- `verixa snapshot`: capture the baseline you want future diffs to compare against
- `verixa diff`: compare current live data to both the contract and the stored baseline
- `verixa check`: CI gate for `validate + diff` with exit codes

Compatibility aliases for v0.x:
- `verixa plan` -> `verixa diff`
- `verixa test` -> `verixa validate`

## Quick Start

### 1. Initialize
```bash
verixa init
```

Or start with the Snowflake starter template:

```bash
verixa init --warehouse snowflake
```

This creates:
- `verixa.yaml`
- `verixa.risk.yaml.example`
- `verixa.targets.yaml.example`
- `.verixa/`


### 2. Define source contracts
Example `verixa.yaml`:

```yaml
warehouse:
  kind: bigquery
  project: my-project
  location: US
  max_bytes_billed: 500MB

rules:
  null_rate_change:
    warning_delta: 0.02
    error_delta: 0.05
  row_count_change:
    warning_drop_ratio: 0.15
    error_drop_ratio: 0.40
    warning_growth_ratio: 0.25
    error_growth_ratio: 1.50
  numeric_distribution_change:
    warning_relative_delta: 0.25
    error_relative_delta: 0.50
    minimum_baseline_value: 1.0

baseline:
  warning_age: 168h
  path: .verixa/{environment}/baseline.json

sources:
  stripe.transactions:
    table: raw.stripe_transactions
    scan:
      timestamp_column: created_at
      lookback: 30d
    check:
      fail_on_warning: false
    rules:
      row_count_change:
        warning_drop_ratio: 0.10
        error_drop_ratio: 0.25
    freshness:
      column: created_at
      max_age: 1h
    schema:
      amount: float
      currency: string
      created_at: timestamp
    tests:
      - no_nulls: amount
      - accepted_values:
          column: currency
          values: [USD, EUR, GBP]
```

Notes:
- `schema` can be a mapping or a list of one-item mappings.
- `verixa.targets.yaml` is optional. Use it to map repo file paths to logical sources for CI targeting.
- `baseline.path` defaults to `.verixa/baseline.json`.
- `baseline.path` can use `{environment}` or `{env}` placeholders for environment-specific baselines.
- `--environment <name>` and `VERIXA_ENV` both select the active baseline environment.
- snapshot and diff runs automatically capture lightweight numeric summaries for declared numeric columns
- `freshness` is explicit in v1: `column` plus `max_age`.
- `scan` is optional. Use it to bound expensive stats queries to a recent time window.
- `scan.timestamp_column` supports `timestamp`, `datetime`, and `date` columns.
- `warehouse.max_bytes_billed` is optional. Use it to cap live BigQuery stats queries.
- Snowflake configs support either `warehouse.connection_name` or explicit `account` + `user`.
- Snowflake tables can be declared as `database.schema.table`, `schema.table` when `warehouse.database` is set, or `table` when both `warehouse.database` and `warehouse.schema` are set.
- top-level `rules` is optional.
- per-source `rules` overrides only the thresholds specified for that source.
- numeric distribution drift currently uses stored `p50` and `p95` summaries for declared numeric columns.
- `numeric_distribution_change.minimum_baseline_value` suppresses noise on near-zero baselines.
- `check.fail_on_warning` is optional at project or source scope.
- `baseline.warning_age` is optional. Set it to `null` to disable stale-baseline warnings.
- test columns must be declared in `schema`.

Example Snowflake warehouse block:

```yaml
warehouse:
  kind: snowflake
  account: xy12345.us-east-1
  user: analyst
  password_env: VERIXA_SNOWFLAKE_PASSWORD
  warehouse_name: ANALYTICS
  database: RAW
  schema: INGEST
  role: TRANSFORMER
```

### 3. Validate contracts against live data
```bash
verixa validate
```

Target only touched sources:

```bash
verixa validate --source stripe.transactions
```

Or auto-target from changed repo paths:

```bash
verixa validate --changed-file models/staging/stripe/orders.sql
verixa validate --changed-against origin/main
```

### 4. Capture a baseline
```bash
verixa snapshot
```

Refresh only selected sources while preserving the rest of the stored baseline:

```bash
verixa snapshot --source stripe.transactions
```

Or refresh only sources impacted by changed files:

```bash
verixa snapshot --changed-file models/staging/stripe/orders.sql
```

Or capture an environment-specific baseline:

```bash
verixa snapshot --environment prod
```

This writes a deterministic baseline file at:
- `.verixa/baseline.json` by default
- or your configured `baseline.path`, such as `.verixa/prod/baseline.json`

Snapshot and diff baselines also store lightweight numeric summaries for declared numeric columns:
- `min`
- `p50`
- `p95`
- `max`
- `mean`

Diff uses the stored `p50` and `p95` summaries to surface numeric drift when relative change exceeds configured thresholds.

### 5. Diff current data against contract and baseline
```bash
verixa diff
```

Target only touched sources:

```bash
verixa diff --source stripe.transactions
```

Or resolve impacted sources automatically:

```bash
verixa diff --changed-against origin/main
```

Or diff against one environment-specific baseline:

```bash
verixa diff --environment prod
```

### 6. Gate CI
```bash
verixa check --fail-on-error
```

## Commands

### `verixa init`
Creates starter files and the local state directory.

Options:
- `--warehouse bigquery`
- `--warehouse snowflake`

### `verixa snapshot`
Queries the configured warehouse and writes the baseline snapshot.

Behavior:
- targeted runs with `--source` merge into the existing baseline instead of dropping unrelated sources
- supports `--changed-file`, `--changed-against`, and `--targets-config`
- supports `--environment` for environment-specific baseline files
- explicit `--source` takes precedence over changed-file targeting
- unmatched changed files fall back to all configured sources
- captures lightweight numeric summaries for declared numeric columns
- `--format json` is supported
- `--estimate-bytes` can attach dry-run estimates for the snapshot query shape
- `--max-bytes-billed` can cap live query cost for the run on BigQuery
- Snowflake sessions are tagged with command-specific `QUERY_TAG` values such as `verixa:snapshot` and `verixa:diff`

### `verixa diff`
Requires an existing baseline snapshot and shows likely breakages before deploy, including:
- removed or changed columns
- null-rate spikes
- row-count drops or spikes
- numeric p50/p95 drift on declared numeric columns
- freshness violations
- accepted-values failures
- `no_nulls` violations
- stale baseline warnings

Behavior:
- compares current state to both the declared contract and stored baseline
- supports `--source`
- supports `--changed-file`, `--changed-against`, and `--targets-config`
- supports `--environment` for environment-specific baseline files
- explicit `--source` takes precedence over changed-file targeting
- unmatched changed files fall back to all configured sources
- refreshes numeric summaries for declared numeric columns alongside the rest of the snapshot state
- surfaces numeric drift findings from stored `p50` and `p95` summaries
- supports `--format json`
- supports `--estimate-bytes`
- supports `--max-bytes-billed` on BigQuery

### `verixa validate`
Runs contract checks against current live data without comparing to the baseline.

Behavior:
- queries only the columns needed for active contract checks
- supports `--source`
- supports `--changed-file`, `--changed-against`, and `--targets-config`
- explicit `--source` takes precedence over changed-file targeting
- unmatched changed files fall back to all configured sources
- supports `--format json`
- supports `--estimate-bytes`
- supports `--max-bytes-billed` on BigQuery

### `verixa check`
Runs the CI-friendly validation path.

Behavior:
- combines contract validation and baseline drift checks
- supports `--source`
- supports `--changed-file`, `--changed-against`, and `--targets-config`
- explicit `--source` takes precedence over changed-file targeting
- unmatched changed files fall back to all configured sources
- supports `--format json`
- supports `--estimate-bytes`
- supports `--max-bytes-billed` on BigQuery
- supports `--fail-on-error`
- supports `--fail-on-warning`
- honors `sources.<name>.check.fail_on_warning: true`

Exit codes:
- `0`: success or warnings only
- `1`: findings matched the configured failure policy
- `2`: runtime, auth, config, or storage failure

### `verixa status`
Shows:
- config path found or missing
- baseline path found or missing
- active baseline environment
- baseline age
- warehouse auth status
- configured `max_bytes_billed`
- configured sources

Example:
```bash
verixa status
```

### `verixa doctor`
Runs diagnostics for:
- config validity
- baseline readability
- baseline path resolution
- warehouse auth
- per-source metadata access
- for Snowflake: active warehouse usability plus role, database, and schema mismatches

Example:
```bash
verixa doctor
```

### `verixa explain <source>`
Shows one source contract in a human-readable form:
- schema
- freshness
- tests
- thresholds
- scan window
- effective `max_bytes_billed`
- warning policy

Examples:
```bash
verixa explain stripe.transactions
verixa explain stripe.transactions --format json
```

### `verixa cost`
Reports workflow cost information for a workflow step.

Examples:
```bash
verixa cost diff
verixa cost validate --source stripe.transactions
verixa cost diff --changed-against origin/main
verixa cost diff --max-bytes-billed 500MB
verixa cost diff --history-window 30m
verixa cost check --format json
```

Supported steps:
- `snapshot`
- `diff`
- `validate`
- `check`

Legacy aliases are also accepted:
- `plan`
- `test`

Behavior:
- on BigQuery, returns dry-run byte estimates by source
- on Snowflake, returns recent query-history usage for the command-specific `QUERY_TAG`
- `--history-window` controls the Snowflake lookback window and defaults to `1h`

## Changed-File Targeting
Verixa can auto-select sources from repo changes when `verixa.targets.yaml` is present.

Example `verixa.targets.yaml`:

```yaml
paths:
  models/staging/stripe/**/*.sql:
    - stripe.transactions
  macros/shared/:
    - stripe.transactions
    - stripe.customers

dbt:
  manifest_path: target/manifest.json
```

Behavior:
- `--source` overrides changed-file targeting when both are provided
- `--changed-file` accepts one or more repo-relative paths
- `--changed-against <ref>` uses `git diff <ref>...HEAD`
- manual path mappings and dbt manifest lineage are combined when both are configured
- dbt manifest targeting can map changed models or macros back to upstream Verixa sources
- unmatched changed files fall back to all configured sources
- missing `verixa.targets.yaml` is an error only when changed-file targeting is requested

Examples:

```bash
verixa diff --changed-file models/staging/stripe/orders.sql
verixa check --changed-against origin/main --fail-on-error
verixa cost diff --changed-against origin/main
```

## JSON Output
All runtime commands except `init` support `--format json`.

Examples:

```bash
verixa diff --format json
```

```bash
verixa check --fail-on-error --format json
```

Example JSON shape for `check`:

```json
{
  "title": "Check",
  "summary": {
    "errors": 1,
    "estimated_bytes_processed": 186,
    "findings": 1,
    "has_errors": true,
    "has_warnings": false,
    "sources_checked": 1,
    "used_baseline": true,
    "warning_policy_failures": 0,
    "warning_policy_sources": [],
    "warnings": 0
  },
  "findings": [
    {
      "code": "schema_column_missing",
      "column": "currency",
      "estimated_bytes_processed": 186,
      "message": "column removed: currency",
      "risks": [
        "likely to break finance models depending on currency"
      ],
      "severity": "error",
      "source_name": "stripe.transactions"
    }
  ]
}
```

## Output Example
Example `verixa diff` output:

```text
Diff

ERROR stripe.transactions
- column removed: currency
- null rate increased on amount: 0.8% -> 9.4%
- freshness violated: latest row is 2h 14m old
Risk:
- likely to break finance models depending on currency
- likely undercount in recent dashboards
```

## Cost and Performance Notes
Verixa is intentionally cost-conscious.

Current behavior:
- BigQuery schema and basic table metadata come from low-cost metadata APIs where possible
- Snowflake schema and table metadata come from `INFORMATION_SCHEMA`
- `validate` narrows live queries to contract-relevant columns instead of always scanning full declared schema
- accepted-values samples are deterministic for CI diffability
- source capture runs in conservative parallelism to reduce wall-clock time on multi-source runs
- scan windows can bound expensive stats queries to recent `TIMESTAMP`, `DATETIME`, or `DATE` slices
- `verixa cost` and `--estimate-bytes` use BigQuery dry runs to estimate query cost
- `warehouse.max_bytes_billed` and `--max-bytes-billed` can hard-stop live queries that would exceed a configured ceiling
- Snowflake does not yet expose pre-run byte estimation or max-bytes-billed enforcement through Verixa
- `verixa cost --history-window` reports recent Snowflake query usage from `INFORMATION_SCHEMA.QUERY_HISTORY`
- Snowflake sessions are tagged with command-specific `QUERY_TAG` values for query-history traceability

Important tradeoff:
- when `scan` is configured, row-count and null-rate drift are measured over that lookback window, not the full table

## CI Example
A minimal CI flow is:
1. install the package
2. authenticate to the configured warehouse
3. restore or check out the baseline snapshot
4. run `verixa check --fail-on-error --format json`
5. upload the JSON report if needed

Example workflow:
- `.github/workflows/verixa-check.yml`

## Live Validation
The repo includes opt-in live smoke tests for both warehouses:
- `tests/integration/test_live_bigquery_smoke.py`
- `tests/integration/test_live_snowflake_smoke.py`

Enable BigQuery live validation with:
- `VERIXA_RUN_LIVE_BIGQUERY=1`
- `VERIXA_LIVE_CONFIG=/path/to/verixa.yaml`
- working BigQuery credentials
- `google-cloud-bigquery` installed

Enable Snowflake live validation with:
- `VERIXA_RUN_LIVE_SNOWFLAKE=1`
- `VERIXA_LIVE_SNOWFLAKE_CONFIG=/path/to/verixa.yaml`
- working Snowflake credentials or a named connection in `~/.snowflake/connections.toml`
- `snowflake-connector-python` installed

Helper setup scripts:
- `scripts/setup_mock_bigquery.py`
- `scripts/setup_mock_snowflake.py`

This workspace has been validated against:
- a real BigQuery project for end-to-end `snapshot`, `validate`, `diff`, `cost`, and `check`
- a real Snowflake account for end-to-end `status`, `validate`, `snapshot`, `diff`, `check`, `cost`, and `doctor`

The Snowflake live verification covered:
- full `database.schema.table` references
- `schema.table` references with warehouse defaults
- short table names with warehouse database and schema defaults
- `DATE` scan windows
- accepted-values samples in findings
- environment-specific baseline paths
- command-specific query-tag history reporting
- Snowflake-specific `doctor` diagnostics

## Known Limitations
Current v1 limitations:
- no hosted backend
- no lineage import or graph UI
- no Snowflake pre-run byte estimation yet
- no Databricks connector yet
- no full dbt lineage ingestion beyond changed-file targeting

## Internal Layout Note
The public CLI and package branding is now Verixa.

The implementation package also lives under `src/verixa/`, so the public package name and internal module layout now match.
