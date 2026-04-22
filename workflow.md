# Verixa Workflow

This document defines the implemented development and runtime workflow for the Verixa MVP.

## Product Workflow

### 1. Initialize
A user runs `verixa init` in a repository.

Expected result:
- a starter `verixa.yaml` is created
- a local state directory such as `.verixa/` is created
- an example file is created for downstream risk mapping
- an example file is created for changed-file source targeting

### 2. Declare Source Contracts
The user edits `verixa.yaml` and defines one or more source contracts.

Each source contract should declare:
- the logical source name
- the BigQuery table reference
- expected schema
- freshness SLA if required
- contract tests such as `no_nulls` and `accepted_values`

Optional source-level `scan` settings can bound stats queries to a recent time window for large partitioned raw tables. These scan windows support `TIMESTAMP`, `DATETIME`, and `DATE` columns.

Optional project-level `rules` can tune drift thresholds for:
- null-rate increases
- row-count drops
- row-count growth
- numeric p50/p95 drift

Optional source-level `rules` can override those thresholds for a specific table.
Optional project-level `baseline.warning_age` can warn when the stored snapshot is too old to trust drift output.
Optional `baseline.path` can route snapshots to environment-specific files such as `.verixa/{environment}/baseline.json`.
Optional project-level or source-level `check.fail_on_warning` can make warnings fail CI.
Optional `warehouse.max_bytes_billed` can cap live BigQuery query cost.

### 3. Validate Contracts
The user runs `verixa validate` against live BigQuery data.

Expected result:
- source contract violations are reported
- no baseline comparison is required
- only contract-relevant columns are queried

Optional machine-readable output:
- `verixa validate --format json`

Optional targeted run:
- `verixa validate --source stripe.transactions`

Optional changed-file targeting:
- `verixa validate --changed-file models/staging/stripe/orders.sql`
- `verixa validate --changed-against origin/main`

Optional live-query ceiling:
- `verixa validate --max-bytes-billed 500MB`

### 4. Capture Baseline
The user runs `verixa snapshot` against live BigQuery data.

Expected result:
- current source metadata and stats are collected
- a deterministic local baseline snapshot is written
- the snapshot becomes the comparison point for future `diff` and `check` runs
- lightweight numeric summaries are captured for declared numeric columns

Optional machine-readable output:
- `verixa snapshot --format json`

Optional targeted refresh:
- `verixa snapshot --source stripe.transactions`
- targeted snapshots merge into the existing baseline instead of removing unrelated sources

Optional changed-file targeting:
- `verixa snapshot --changed-file models/staging/stripe/orders.sql`
- `verixa snapshot --changed-against origin/main`

Optional estimate:
- `verixa snapshot --estimate-bytes`
- or `verixa cost snapshot`

Optional live-query ceiling:
- `verixa snapshot --max-bytes-billed 500MB`

Optional environment-specific baseline:
- `verixa snapshot --environment prod`
- or set `VERIXA_ENV=prod`

### 5. Run Pre-Deploy Diff
Before shipping upstream changes, the user runs `verixa diff`.

Expected result:
- current live data is queried again
- Verixa compares current state to both the baseline snapshot and declared contracts
- likely breakages are shown in a concise terminal report
- downstream risk hints are attached if mapping data exists
- numeric p50/p95 drift can be surfaced from stored summary baselines

Current behavior:
- `verixa diff` requires an existing baseline snapshot
- it does not silently downgrade to contract-only mode
- it can warn when the stored baseline is stale
- `verixa diff --format json` emits machine-readable findings for CI consumers
- `--source` can restrict the run to one or more logical sources
- `--changed-file` and `--changed-against` can auto-target sources through `verixa.targets.yaml`
- `verixa.targets.yaml` can combine manual path mappings with optional dbt manifest lineage
- `--environment` can select an environment-specific baseline path
- explicit `--source` selection takes precedence over changed-file targeting
- unmatched changed files fall back to all configured sources
- `--estimate-bytes` can attach BigQuery dry-run byte estimates to the output
- `--max-bytes-billed` can cap live query cost for the run
- `verixa cost diff` estimates the same query shape without executing the full workflow

### 6. Run CI Check
CI runs `verixa check --fail-on-error`.

Expected result:
- Verixa executes the CI-safe validation path
- actionable output is printed to logs
- exit codes make failure conditions machine-readable

Current behavior:
- error-severity findings cause exit code `1` only when `--fail-on-error` is used
- `--fail-on-warning` causes any warning to exit `1`
- source-level `check.fail_on_warning` can make warnings fail CI for selected sources
- warning-only output still exits `0` unless warning failure policy is active
- runtime, config, auth, or storage failures exit `2`
- `verixa check --format json` emits deterministic JSON output
- `--changed-file` and `--changed-against` can auto-target sources through `verixa.targets.yaml`
- `--environment` can select an environment-specific baseline path
- `--max-bytes-billed` can cap live query cost for the run
- `verixa cost check` estimates the live query shape ahead of time

### 7. Inspect Environment and Configuration
Use `verixa status` when you want a quick environment view.

It shows:
- config found or missing
- active baseline environment
- baseline found or missing
- baseline age
- warehouse auth status
- configured `max_bytes_billed`
- configured sources

Use `verixa explain <source>` when you want to inspect one contract.

It shows:
- schema
- freshness
- tests
- thresholds
- scan window
- effective `max_bytes_billed`
- warning policy

Use `verixa doctor` when you need diagnostics.

It checks:
- config validity
- baseline readability
- baseline path resolution
- auth usability
- per-source metadata access

## Compatibility Workflow
For v0.x, Verixa still supports legacy aliases:
- `verixa plan` -> `verixa diff`
- `verixa test` -> `verixa validate`

They are compatibility shims, not the preferred command names.

## Development Workflow

### Working Principles
- Build the CLI first, then fill in services behind stable command shapes.
- Keep connector code isolated from rules and output code.
- Prefer pure functions for rules and diffing so they are easy to test.
- Keep storage explicit and inspectable.
- Avoid hidden state and runtime magic.

### Delivery Order Used
1. Package scaffold and CLI entrypoint.
2. Config models and YAML loader.
3. Snapshot models and storage.
4. BigQuery connector.
5. Rules and diff engine.
6. Output formatting.
7. Tests.
8. Documentation polish.
9. Configurable thresholds and JSON output.
10. Source-level overrides, live smoke harness, and GitHub Actions example.
11. Query narrowing, parallel capture, targeted source filtering, bounded scan windows, and baseline staleness warnings.
12. DATE/DATETIME scan support, dry-run byte estimates, and warning-based CI policies.
13. Verixa rebrand, command refinement, status, doctor, explain, and cost.
14. Max-bytes-billed enforcement for live BigQuery query ceilings.
15. Automatic changed-file to source targeting for CI-oriented runs.
16. Numeric distribution summaries and p50/p95 drift detection.
17. Environment-specific baseline files and dbt-manifest-driven source targeting.

### Update Process During Implementation
When work starts on a task:
- mark it `[~]` in `implementation_plan.md`

When the task is complete:
- mark it `[x]`

If the task is intentionally postponed:
- mark it `[!]` and explain why in follow-up docs

If a decision changes scope:
- update `implementation_plan.md`, `project_structure.md`, and `README.md` together

## Runtime Data Flow
1. CLI command parses arguments.
2. Config loader reads `verixa.yaml`.
3. Contract and threshold models are normalized.
4. BigQuery connector retrieves current source observations.
5. Snapshot service assembles typed snapshot data.
6. Rules compare contract, baseline, and current state.
7. Output renderer formats text or JSON findings.
8. Exit-code handling returns machine-friendly status.

## CI Workflow
A minimal CI workflow should:
1. install the package
2. authenticate to Google Cloud
3. restore or check out the baseline snapshot
4. optionally fetch a base ref and use `verixa.targets.yaml` for changed-file targeting, including optional dbt manifest lineage
5. run `verixa check --fail-on-error --format json` if machine-readable logs are needed
6. upload the JSON report as a workflow artifact if desired

The repository includes an example workflow at:
- `.github/workflows/verixa-check.yml`

## Live Validation Workflow
The repository includes an opt-in live BigQuery smoke test at:
- `tests/integration/test_live_bigquery_smoke.py`

Enable it only when all prerequisites exist:
- `VERIXA_RUN_LIVE_BIGQUERY=1`
- `VERIXA_LIVE_CONFIG=/path/to/verixa.yaml`
- working BigQuery credentials in the environment
- `google-cloud-bigquery` installed

The workspace has been validated end-to-end against a real BigQuery project using a mock source table, while keeping this harness opt-in by default.

## Non-Goals for This Workflow
- No dashboard publishing step.
- No background monitoring daemon.
- No hosted API dependency.
- No full lineage platform or lineage UI in v1.
