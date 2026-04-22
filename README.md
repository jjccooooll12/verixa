# DataGuard

DataGuard is a developer-first Data CI tool for warehouse source tables.

The product focus is pre-deploy safety, not reactive monitoring. DataGuard compares declared source contracts, a stored baseline snapshot, and current warehouse state to surface likely breakages before a change ships.

## Positioning
DataGuard is:
- contract-first,
- diff-driven,
- warehouse-native,
- CLI-first,
- CI-friendly.

DataGuard is not:
- a dashboard-first observability platform,
- a lineage visualization tool,
- anomaly-detection infrastructure,
- just a linter.

## MVP Status
Implemented in the current repo:
- Python package scaffold with `src/` layout,
- Typer-based CLI,
- YAML config loader and typed contract models,
- BigQuery connector using the official client library,
- deterministic local baseline storage,
- contract checks and baseline drift heuristics,
- configurable project-level and source-level drift thresholds,
- text and JSON output modes,
- mocked unit and CLI workflow tests,
- an opt-in live BigQuery smoke harness,
- a GitHub Actions example workflow for JSON-based checks.

Current test status:
- `57 passed, 1 skipped` on the local suite.

## Install

### Development install
```bash
pip install -e .[dev]
```

### Run the CLI without installing
```bash
PYTHONPATH=src python -m dataguard --help
```

## BigQuery Authentication
DataGuard uses the official Google Cloud BigQuery Python client.

Expected auth for local or CI usage:
- Application Default Credentials,
- or `GOOGLE_APPLICATION_CREDENTIALS` pointing at a service account key,
- or a workload identity setup that the BigQuery client can use directly.

The CLI does not manage auth for you.

## Quick Start

### 1. Initialize
```bash
dataguard init
```

This creates:
- `dataguard.yaml`
- `dataguard.risk.yaml.example`
- `.dataguard/`

### 2. Define one or more source contracts
Example `dataguard.yaml`:

```yaml
warehouse:
  kind: bigquery
  project: my-project
  location: US

rules:
  null_rate_change:
    warning_delta: 0.02
    error_delta: 0.05
  row_count_change:
    warning_drop_ratio: 0.15
    error_drop_ratio: 0.40
    warning_growth_ratio: 0.25
    error_growth_ratio: 1.50

baseline:
  warning_age: 168h

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
- `freshness` must be explicit in v1: `column` plus `max_age`.
- `scan` is optional. Use it to bound expensive stats queries to a recent timestamp window.
- `scan.timestamp_column` can use `timestamp`, `datetime`, or `date` schema types.
- `check.fail_on_warning` is optional. Use it to make warnings fail CI for a specific source.
- `baseline.warning_age` is optional. Set it to `null` to disable stale-baseline warnings.
- test columns must be declared in `schema`.
- top-level `rules` is optional. Omit it to use the built-in defaults.
- per-source `rules` is also optional. It overrides only the thresholds specified for that source.

### 3. Capture a baseline
```bash
dataguard snapshot
```

To refresh only selected sources while keeping the rest of the stored baseline:

```bash
dataguard snapshot --source stripe.transactions
```

This stores a deterministic baseline file at:
- `.dataguard/baseline.json`

Captured fields in the baseline:
- current schema,
- row count,
- null rates for declared columns,
- freshness metadata,
- accepted-values results.

### 4. Run a pre-deploy plan
```bash
dataguard plan
```

To limit a run to touched sources:

```bash
dataguard plan --source stripe.transactions
```

If you want a byte estimate before or alongside the real run:

```bash
dataguard plan --source stripe.transactions --estimate-bytes
```

The plan compares current state to both:
- the declared contract,
- the stored baseline snapshot.

### 5. Run contract checks directly
```bash
dataguard test
```

### 6. Run CI validation
```bash
dataguard check --fail-on-error
```

## Commands

### `dataguard init`
Creates starter files and the local state directory.

### `dataguard snapshot`
Queries BigQuery and writes the baseline snapshot.
When `--source` is used, the selected sources are merged into the existing baseline instead of replacing unrelated sources.

### `dataguard plan`
Requires an existing baseline snapshot and shows likely breakages before deploy, including:
- removed or changed columns,
- null-rate spikes,
- row-count drops or spikes,
- freshness violations,
- accepted-values failures,
- `no_nulls` violations.

Common option:
- `--source <name>` can be repeated to limit `snapshot`, `plan`, `test`, and `check` to specific logical sources.
- `--estimate-bytes` adds a BigQuery dry-run estimate for the live stats queries used by `snapshot`, `plan`, `test`, and `check`.

### `dataguard test`
Runs contract checks against current live data without comparing to the baseline.
It only queries the columns needed for active contract checks, so schema-only or narrow contracts stay cheaper and faster than full drift plans.

### `dataguard check --fail-on-error`
Runs the CI-friendly validation path and exits non-zero when error-severity findings exist.

Additional behavior:
- `--fail-on-warning` makes any warning fail the command.
- `sources.<name>.check.fail_on_warning: true` makes warnings fail CI for only that source, even without the global flag.

Exit codes:
- `0`: success or warnings only,
- `1`: error findings with `--fail-on-error`, warnings alone still exit `0`,
- `2`: runtime, auth, config, or storage failure.

## JSON Output
All runtime commands except `init` support `--format json`.

Examples:

```bash
dataguard plan --format json
```

```bash
dataguard check --fail-on-error --format json
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

Runtime errors also emit JSON when `--format json` is used.

## Threshold Configuration
Thresholds can be configured at two levels:
- project-level under top-level `rules`,
- source-level under `sources.<name>.rules`.

Defaults:
- `null_rate_change.warning_delta = 0.01`
- `null_rate_change.error_delta = 0.05`
- `row_count_change.warning_drop_ratio = 0.20`
- `row_count_change.error_drop_ratio = 0.50`
- `row_count_change.warning_growth_ratio = 0.20`
- `row_count_change.error_growth_ratio = 1.00`
- `baseline.warning_age = 168h`
- `check.fail_on_warning = false`

Interpretation:
- a null-rate delta of `0.05` means a 5 percentage point increase,
- a row-count growth ratio of `1.00` means 100% growth over baseline.

## Example Plan Output

```text
stripe.transactions
- ERROR: column removed: currency
- ERROR: freshness violated: latest row is 2h 14m old
- WARNING: null rate increased on amount: 0.8% -> 9.4%
Risk:
- likely to break finance models depending on currency
- likely undercount in recent dashboards

Summary: 2 error(s), 1 warning(s), 1 source(s) checked
```

## Optional Risk Mapping
If you create `dataguard.risk.yaml`, DataGuard will attach downstream hints to relevant findings.

Example:

```yaml
sources:
  stripe.transactions:
    general:
      - likely undercount in recent dashboards
    columns:
      currency:
        - likely to break finance models depending on currency
```

## GitHub Actions Example
The repo includes an example workflow at:
- `.github/workflows/dataguard-check.yml`

It:
- installs DataGuard,
- checks that `.dataguard/baseline.json` exists,
- loads a GCP service account key from `GCP_SERVICE_ACCOUNT_KEY_JSON`,
- runs `dataguard check --fail-on-error --format json`,
- uploads `dataguard-report.json` as an artifact.

It is configured for `workflow_dispatch`, so it is manual by default.

## Live BigQuery Smoke Harness
The repo includes an opt-in smoke test at:
- `tests/integration/test_live_bigquery_smoke.py`

Enable it only when all prerequisites exist:

```bash
export DATAGUARD_RUN_LIVE_BIGQUERY=1
export DATAGUARD_LIVE_CONFIG=/path/to/dataguard.yaml
PYTHONPATH=src pytest -q tests/integration/test_live_bigquery_smoke.py
```

Requirements:
- working BigQuery credentials,
- `google-cloud-bigquery` installed,
- a real config file that points at accessible tables.

The live smoke test is skipped by default.

This repository has also been validated in this workspace against a real BigQuery project (`dataguard-494111`) using a mock dataset loaded into BigQuery for end-to-end `snapshot`, `plan`, and `check` verification.

## Cost and Query Tradeoffs
DataGuard is intentionally cost-conscious, but it still queries live warehouse data.

Current v1 behavior:
- schema comes from table metadata,
- row count comes from table metadata when no stats query is needed,
- row count becomes exact when a stats query already scans the table,
- null-rate, freshness, and accepted-values checks use lightweight aggregate queries over only the required columns.
- `dataguard test` narrows live queries to contract-relevant columns instead of always scanning the full declared schema.
- multi-source captures run in parallel with a conservative worker pool to reduce wall-clock time without overloading the default BigQuery client connection pool.
- optional source-level `scan` windows can bound stats queries to a recent timestamp range for large raw tables.
- `--estimate-bytes` issues an extra BigQuery dry-run request per source and reports `total_bytes_processed` for the live stats query shape.

Tradeoffs:
- large unpartitioned tables can still be expensive to scan,
- metadata row counts may lag for streaming-heavy tables,
- when `scan` is configured, row-count and null-rate comparisons apply to the bounded lookback window rather than the full table,
- a stale baseline warning does not fail CI by itself, but it reduces trust in drift findings until a fresh snapshot is captured,
- dry-run estimates add latency because they are extra API calls,
- v1 supports only simple column identifiers, not nested fields.

## CI Usage
A minimal CI job should:
1. install DataGuard,
2. authenticate to Google Cloud,
3. restore or check out `.dataguard/baseline.json`,
4. run `dataguard check --fail-on-error`.

If machine-readable output is needed:
```bash
dataguard check --fail-on-error --format json
```

## Development
Run the default suite:

```bash
PYTHONPATH=src pytest -q
```

Run the opt-in live smoke test only when configured:

```bash
PYTHONPATH=src pytest -q tests/integration/test_live_bigquery_smoke.py
```

## Known Limitations
- BigQuery is the only supported warehouse in v1.
- The default test suite does not hit a live BigQuery project.
- The live smoke harness is opt-in and skipped by default to avoid accidental live warehouse usage.
- There is no automatic lineage import yet.
- There is no hosted backend or shared state layer.

## Repository Docs
- `implementation_plan.md`: living implementation checklist.
- `workflow.md`: product and development workflow.
- `project_structure.md`: implemented package structure and module responsibilities.
