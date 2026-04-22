# DataGuard Implementation Plan

This document is the working implementation checklist for the DataGuard MVP. It is intended to be updated as implementation progresses.

## Status Legend
- `[ ]` not started
- `[~]` in progress
- `[x]` completed
- `[!]` blocked or intentionally deferred

## MVP Goal
Build a local-first, developer-first Data CI CLI for BigQuery source tables that can:
- initialize a project,
- capture a baseline snapshot,
- compare current data state against baseline and declared contracts,
- report likely breakages before deploy,
- run in CI with meaningful exit codes.

## Scope Guardrails
- [x] Keep the product focused on pre-deploy source-data safety.
- [x] Avoid building dashboards, hosted services, or lineage UI in v1.
- [x] Avoid warehouse-specific abstractions that are unnecessary for BigQuery v1.
- [x] Keep state local, explicit, and deterministic.

## Phase 1: Repository and Packaging
- [x] Create Python package scaffold with `src/` layout.
- [x] Add `pyproject.toml` with CLI entrypoint.
- [x] Choose and configure core dependencies.
- [x] Add development dependencies for tests and linting.
- [x] Add package version metadata.
- [x] Add `.gitignore`.

## Phase 2: Core Domain Models
- [x] Define contract models for sources, schema fields, freshness config, and tests.
- [x] Define snapshot models for schema, row count, null rates, freshness, and accepted-values results.
- [x] Define diff and finding models for plan output.
- [x] Define severity levels and exit-code semantics.
- [x] Normalize warehouse types for comparisons.
- [x] Add project-level rule threshold models.
- [x] Add per-source rule override support.

## Phase 3: Config Loading
- [x] Add YAML config loader.
- [x] Support a top-level warehouse section for BigQuery settings.
- [x] Support optional baseline staleness settings.
- [x] Support optional project-level and source-level check warning policies.
- [x] Support source definitions keyed by logical source name.
- [x] Support schema contract declarations.
- [x] Support freshness SLA declarations.
- [x] Support optional bounded scan windows for expensive stats queries.
- [x] Support DATE and DATETIME scan-window columns in addition to TIMESTAMP.
- [x] Support test declarations: `no_nulls`.
- [x] Support test declarations: `accepted_values`.
- [x] Validate config with clear user-facing errors.
- [x] Decide and document shorthand vs explicit config forms.
- [x] Support configurable drift thresholds under `rules`.
- [x] Support source-level `rules` overrides merged on top of project defaults.

## Phase 4: CLI Surface
- [x] Implement `dataguard init`.
- [x] Implement `dataguard snapshot`.
- [x] Implement `dataguard plan`.
- [x] Implement `dataguard test`.
- [x] Implement `dataguard check`.
- [x] Add common CLI options for config path and output behavior.
- [x] Add repeatable `--source` filtering for targeted runs.
- [x] Add optional `--estimate-bytes` dry-run query estimates.
- [x] Add `--fail-on-warning` for stricter CI behavior.
- [x] Add consistent error handling and non-zero exit behavior.
- [x] Keep command handlers thin and service-oriented.
- [x] Add `--format json` support for machine-readable output.

## Phase 5: BigQuery Connector
- [x] Define connector interface boundaries so BigQuery stays isolated.
- [x] Implement BigQuery client initialization.
- [x] Implement table reference parsing.
- [x] Implement schema retrieval using metadata or `INFORMATION_SCHEMA`.
- [x] Implement row-count retrieval using low-cost metadata where possible.
- [x] Implement null-rate aggregation for declared columns.
- [x] Implement freshness query for configured timestamp column.
- [x] Implement accepted-values aggregation for configured columns.
- [x] Narrow contract-only queries to the columns required by active checks.
- [x] Keep accepted-values example output deterministic.
- [x] Estimate live stats-query bytes with BigQuery dry runs when requested.
- [x] Handle authentication and permission failures with useful messages.
- [x] Document BigQuery cost and freshness tradeoffs.

## Phase 6: Snapshot Service and Storage
- [x] Implement snapshot capture service.
- [x] Capture multiple sources in parallel with conservative defaults.
- [x] Limit queries to declared or required columns.
- [x] Store baseline snapshot locally under `.dataguard/`.
- [x] Use deterministic JSON serialization.
- [x] Implement snapshot read/write helpers.
- [x] Merge targeted snapshot refreshes into an existing baseline.
- [x] Handle missing baseline state gracefully.
- [x] Decide whether to include timestamps and run metadata in stored snapshots.

## Phase 7: Rules and Diff Engine
- [x] Implement schema drift rule.
- [x] Detect missing columns.
- [x] Detect changed column types.
- [x] Detect unexpected extra columns if configured or useful.
- [x] Implement null-rate change rule.
- [x] Define default threshold semantics for null-rate spikes.
- [x] Implement row-count change rule.
- [x] Define default row-count change thresholds.
- [x] Implement freshness SLA rule.
- [x] Implement accepted-values rule.
- [x] Implement `no_nulls` rule.
- [x] Compare contract vs current state.
- [x] Compare baseline vs current state.
- [x] Aggregate findings into a single plan result.
- [x] Assign severities consistently.
- [x] Wire project-level thresholds into drift rules.
- [x] Allow source-level overrides to replace project defaults.
- [x] Warn when the stored baseline is stale.

## Phase 8: Downstream Risk Hints
- [x] Define a simple user-maintained mapping file format.
- [x] Load downstream risk metadata if present.
- [x] Attach downstream hints to relevant findings.
- [x] Keep this optional and non-blocking.

## Phase 9: Output and UX
- [x] Design concise terminal output for snapshot results.
- [x] Design concise terminal output for plan findings.
- [x] Group findings by source.
- [x] Show before/after values for drift findings.
- [x] Include a risk summary section.
- [x] Distinguish warnings from errors clearly.
- [x] Keep output deterministic for CI and diffability.
- [x] Avoid noisy success output.
- [x] Add machine-readable JSON output for snapshot/test/plan/check.
- [x] Add machine-readable JSON runtime errors.

## Phase 10: Testing
- [x] Add unit tests for config loading.
- [x] Add unit tests for contract normalization.
- [x] Add unit tests for snapshot storage.
- [x] Add unit tests for schema drift logic.
- [x] Add unit tests for null-rate change logic.
- [x] Add unit tests for row-count change logic.
- [x] Add unit tests for freshness logic.
- [x] Add unit tests for accepted-values logic.
- [x] Add unit tests for diff aggregation.
- [x] Add CLI smoke tests for `init`.
- [x] Add mocked workflow tests for `snapshot`, `plan`, and `check --fail-on-error`.
- [x] Add connector-specific mocked tests around BigQuery query construction, query parameters, and error handling.
- [x] Add CLI tests that lock in baseline-required and warning-only CI behavior.
- [x] Add tests for configurable thresholds.
- [x] Add tests for JSON output behavior.
- [x] Add an opt-in live BigQuery smoke harness gated by environment variables.

## Phase 11: Documentation
- [x] Write README setup instructions.
- [x] Write README config example.
- [x] Write README command walkthrough.
- [x] Document BigQuery auth expectations.
- [x] Document snapshot storage behavior.
- [x] Document CI usage and exit codes.
- [x] Document known limitations and tradeoffs.
- [x] Document configurable thresholds and JSON output.
- [x] Document source-level overrides.
- [x] Add a GitHub Actions example workflow using JSON output.
- [x] Document the live smoke harness.

## Phase 12: MVP Validation
- [x] Verify `dataguard init` creates the expected files.
- [x] Verify one declared BigQuery source can load from YAML.
- [x] Verify `dataguard snapshot` writes a baseline snapshot.
- [x] Verify `dataguard plan` shows meaningful diff output.
- [x] Verify `dataguard check --fail-on-error` returns correct exit codes.
- [x] Validate output readability with at least one realistic example.
- [x] Validate JSON output shape for CI consumers.
- [x] Keep live BigQuery validation opt-in and skipped by default.

## Deferred / Post-MVP
- [ ] Numeric distribution summaries.
- [ ] Snowflake connector.
- [ ] Databricks connector.
- [ ] Automatic lineage import from dbt artifacts.
- [ ] Hosted backend or shared team state.
- [ ] Optional fail-on-warning CI behavior.
- [ ] Environment-specific snapshot files.

## Remaining Gaps
- [x] Exercise the live smoke harness against a real BigQuery project, not only in skipped mode.

## Decisions Locked In
- [x] `dataguard plan` requires a baseline snapshot and does not silently downgrade to contract-only mode.
- [x] `dataguard check --fail-on-error` fails only on error-severity findings, not warnings.
- [x] Drift thresholds default at the project level and can be overridden per source.

## Current Assumptions
- [x] BigQuery is the only supported warehouse in v1.
- [x] Baseline snapshots are local files, likely committed or restored in CI.
- [x] Row-count checks are heuristic when metadata is used and exact when a stats query already runs.
- [x] Accepted-values checks only evaluate declared columns.
- [x] First user persona is a data engineer or backend engineer with dbt downstream.
