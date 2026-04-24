"""Diff orchestration across contracts, baselines, and current snapshots."""

from __future__ import annotations

from verixa.contracts.models import BaselineConfig, ProjectConfig, SourceContract
from verixa.diff.models import DiffResult, Finding
from verixa.diff.risk import RiskConfig, SourceRiskHints
from verixa.extensions.api import (
    CustomCheckContext,
    ExtensionError,
    FindingEnrichmentContext,
)
from verixa.findings.schema import stable_code_for_internal
from verixa.rules.accepted_values import check_accepted_values
from verixa.rules.freshness import check_freshness
from verixa.rules.history_band import (
    check_null_rate_history_band,
    check_numeric_history_band,
    check_row_count_history_band,
)
from verixa.rules.no_nulls import check_no_nulls
from verixa.rules.numeric_distribution_change import check_numeric_distribution_changes
from verixa.rules.null_rate_change import check_null_rate_changes
from verixa.rules.row_count_change import check_row_count_change
from verixa.rules.schema_drift import check_schema_drift
from verixa.snapshot.models import ProjectSnapshot, SourceSnapshot


def build_test_result(
    config: ProjectConfig,
    current_snapshot: ProjectSnapshot,
    risk_config: RiskConfig | None = None,
    *,
    execution_mode: str = "bounded",
) -> DiffResult:
    """Build findings using only contract-vs-current checks."""

    findings: list[Finding] = []
    for source_name in sorted(config.sources):
        contract = config.sources[source_name]
        current = current_snapshot.sources[source_name]
        findings.extend(check_schema_drift(contract, current))
        findings.extend(check_no_nulls(contract, current))
        findings.extend(check_freshness(contract, current))
        findings.extend(check_accepted_values(contract, current))
        findings.extend(
            _run_custom_checks(
                config,
                contract,
                current=current,
                baseline=None,
                historical=(),
                execution_mode=execution_mode,
                used_baseline=False,
            )
        )
    return _finalize(
        findings,
        config=config,
        sources_checked=len(config.sources),
        used_baseline=False,
        risk_config=risk_config,
        execution_mode=execution_mode,
    )


def build_plan_result(
    config: ProjectConfig,
    baseline_snapshot: ProjectSnapshot,
    current_snapshot: ProjectSnapshot,
    risk_config: RiskConfig | None = None,
    historical_snapshots: tuple[ProjectSnapshot, ...] = (),
    *,
    execution_mode: str = "bounded",
) -> DiffResult:
    """Build findings using both contract and baseline comparisons."""

    findings: list[Finding] = _check_baseline_staleness(
        config.baseline,
        baseline_snapshot,
        current_snapshot,
    )
    for source_name in sorted(config.sources):
        contract = config.sources[source_name]
        current = current_snapshot.sources[source_name]
        baseline = baseline_snapshot.sources.get(source_name)
        source_history = _source_history(
            historical_snapshots,
            source_name,
            window=None if contract.history is None else contract.history.window,
        )

        findings.extend(check_schema_drift(contract, current))
        findings.extend(check_no_nulls(contract, current))
        findings.extend(check_freshness(contract, current))
        findings.extend(check_accepted_values(contract, current))

        if baseline is None:
            findings.extend(
                _run_custom_checks(
                    config,
                    contract,
                    current=current,
                    baseline=None,
                    historical=source_history,
                    execution_mode=execution_mode,
                    used_baseline=True,
                )
            )
            findings.append(
                Finding(
                    source_name=source_name,
                    severity="warning",
                    code="baseline_missing_for_source",
                    message="baseline missing for source; run 'verixa snapshot' to refresh it",
                )
            )
            continue

        if _use_history_mode(contract, source_history):
            history_config = contract.history
            assert history_config is not None
            if history_config.null_rate:
                findings.extend(
                    check_null_rate_history_band(
                        contract,
                        source_history,
                        current,
                        thresholds=contract.rules.null_rate_change,
                        history_config=history_config,
                    )
                )
            else:
                findings.extend(
                    check_null_rate_changes(
                        contract,
                        baseline,
                        current,
                        thresholds=contract.rules.null_rate_change,
                    )
                )

            if history_config.row_count:
                findings.extend(
                    check_row_count_history_band(
                        contract,
                        source_history,
                        current,
                        thresholds=contract.rules.row_count_change,
                        history_config=history_config,
                    )
                )
            elif not history_config.backfill_mode:
                findings.extend(
                    check_row_count_change(
                        contract,
                        baseline,
                        current,
                        thresholds=contract.rules.row_count_change,
                    )
                )

            if history_config.numeric_distribution:
                findings.extend(
                    check_numeric_history_band(
                        contract,
                        source_history,
                        current,
                        thresholds=contract.rules.numeric_distribution_change,
                        history_config=history_config,
                    )
                )
            else:
                findings.extend(
                    check_numeric_distribution_changes(
                        contract,
                        baseline,
                        current,
                        thresholds=contract.rules.numeric_distribution_change,
                    )
                )
        else:
            findings.extend(
                check_null_rate_changes(
                    contract,
                    baseline,
                    current,
                    thresholds=contract.rules.null_rate_change,
                )
            )
            if contract.history is None or not contract.history.backfill_mode:
                findings.extend(
                    check_row_count_change(
                        contract,
                        baseline,
                        current,
                        thresholds=contract.rules.row_count_change,
                    )
                )
            findings.extend(
                check_numeric_distribution_changes(
                    contract,
                    baseline,
                    current,
                    thresholds=contract.rules.numeric_distribution_change,
                )
            )
        findings.extend(
            _run_custom_checks(
                config,
                contract,
                current=current,
                baseline=baseline,
                historical=source_history,
                execution_mode=execution_mode,
                used_baseline=True,
            )
        )

    return _finalize(
        findings,
        config=config,
        sources_checked=len(config.sources),
        used_baseline=True,
        risk_config=risk_config,
        execution_mode=execution_mode,
    )


def _finalize(
    findings: list[Finding],
    *,
    config: ProjectConfig,
    sources_checked: int,
    used_baseline: bool,
    risk_config: RiskConfig | None,
    execution_mode: str,
) -> DiffResult:
    enriched = [
        _apply_custom_finding_enrichers(
            _apply_confidence_metadata(
                _apply_severity_override(
                    _attach_risks(finding, risk_config),
                    config,
                ),
                config,
                execution_mode=execution_mode,
            ),
            config,
            used_baseline=used_baseline,
            execution_mode=execution_mode,
        )
        for finding in findings
    ]
    ordered = tuple(sorted(enriched, key=_finding_sort_key))
    return DiffResult(
        findings=ordered,
        sources_checked=sources_checked,
        used_baseline=used_baseline,
        warning_policy_sources=_warning_policy_sources(config, list(ordered)),
        advisory_mode_enabled=config.check.advisory,
        advisory_sources=_advisory_sources(config),
        execution_mode=execution_mode,
    )


def _attach_risks(finding: Finding, risk_config: RiskConfig | None) -> Finding:
    if risk_config is None:
        return finding
    hints = risk_config.sources.get(finding.source_name)
    if hints is None:
        return finding

    risks = list(hints.general)
    if finding.column is not None:
        risks.extend(hints.columns.get(finding.column, ()))

    unique_risks = tuple(dict.fromkeys(risks))
    return Finding(
        source_name=finding.source_name,
        severity=finding.severity,
        code=finding.code,
        message=finding.message,
        column=finding.column,
        risks=unique_risks,
        owners=hints.owners,
        source_criticality=hints.criticality,
        downstream_models=hints.downstream_models,
        confidence_override=finding.confidence_override,
        confidence_reason=finding.confidence_reason,
        history_metric=finding.history_metric,
        history_window=finding.history_window,
        history_sample_size=finding.history_sample_size,
        history_center_value=finding.history_center_value,
        history_lower_bound=finding.history_lower_bound,
        history_upper_bound=finding.history_upper_bound,
    )


def _apply_severity_override(
    finding: Finding,
    config: ProjectConfig,
) -> Finding:
    contract = config.sources.get(finding.source_name)
    if contract is None:
        return finding

    override = contract.severity_overrides.get(stable_code_for_internal(finding.code))
    if override is None:
        return finding

    return Finding(
        source_name=finding.source_name,
        severity=override,
        code=finding.code,
        message=finding.message,
        column=finding.column,
        risks=finding.risks,
        owners=finding.owners,
        source_criticality=finding.source_criticality,
        downstream_models=finding.downstream_models,
        confidence_override=finding.confidence_override,
        confidence_reason=finding.confidence_reason,
        history_metric=finding.history_metric,
        history_window=finding.history_window,
        history_sample_size=finding.history_sample_size,
        history_center_value=finding.history_center_value,
        history_lower_bound=finding.history_lower_bound,
        history_upper_bound=finding.history_upper_bound,
    )


def _apply_confidence_metadata(
    finding: Finding,
    config: ProjectConfig,
    *,
    execution_mode: str,
) -> Finding:
    contract = config.sources.get(finding.source_name)
    if contract is None or execution_mode == "full":
        return finding

    confidence_override = finding.confidence_override
    confidence_reason = finding.confidence_reason

    if contract.scan is not None:
        confidence_reason = (
            f"{execution_mode} mode evaluated a bounded window of "
            f"{contract.scan.lookback} on {contract.scan.timestamp_column}"
        )
        confidence_override = _downgrade_confidence(
            finding.confidence_override or _default_confidence_for_code(finding.code)
        )
    elif execution_mode == "cheap" and finding.code == "row_count_changed":
        confidence_reason = "cheap mode relies on warehouse metadata row counts instead of exact counts"
        confidence_override = "low"

    if confidence_override == finding.confidence_override and confidence_reason == finding.confidence_reason:
        return finding

    return Finding(
        source_name=finding.source_name,
        severity=finding.severity,
        code=finding.code,
        message=finding.message,
        column=finding.column,
        risks=finding.risks,
        owners=finding.owners,
        source_criticality=finding.source_criticality,
        downstream_models=finding.downstream_models,
        confidence_override=confidence_override,
        confidence_reason=confidence_reason,
        history_metric=finding.history_metric,
        history_window=finding.history_window,
        history_sample_size=finding.history_sample_size,
        history_center_value=finding.history_center_value,
        history_lower_bound=finding.history_lower_bound,
        history_upper_bound=finding.history_upper_bound,
    )


def _run_custom_checks(
    config: ProjectConfig,
    contract: SourceContract,
    *,
    current: SourceSnapshot,
    baseline: SourceSnapshot | None,
    historical: tuple[SourceSnapshot, ...],
    execution_mode: str,
    used_baseline: bool,
) -> tuple[Finding, ...]:
    findings: list[Finding] = []
    context = CustomCheckContext(
        config=config,
        contract=contract,
        current=current,
        baseline=baseline,
        historical=historical,
        execution_mode=execution_mode,
        used_baseline=used_baseline,
    )
    for hook in config.extensions.checks:
        try:
            hook_findings = hook(context)
        except Exception as exc:  # pragma: no cover - hook-specific failures are environment specific
            raise ExtensionError(
                f"Custom check hook '{_hook_name(hook)}' failed for source '{contract.name}': {exc}"
            ) from exc
        if not isinstance(hook_findings, tuple):
            raise ExtensionError(
                f"Custom check hook '{_hook_name(hook)}' must return a tuple[Finding, ...]."
            )
        for finding in hook_findings:
            if not isinstance(finding, Finding):
                raise ExtensionError(
                    f"Custom check hook '{_hook_name(hook)}' returned a non-Finding value."
                )
            findings.append(finding)
    return tuple(findings)


def _apply_custom_finding_enrichers(
    finding: Finding,
    config: ProjectConfig,
    *,
    used_baseline: bool,
    execution_mode: str,
) -> Finding:
    enriched = finding
    context = FindingEnrichmentContext(
        config=config,
        used_baseline=used_baseline,
        execution_mode=execution_mode,
    )
    for hook in config.extensions.finding_enrichers:
        try:
            enriched = hook(enriched, context)
        except Exception as exc:  # pragma: no cover - hook-specific failures are environment specific
            raise ExtensionError(
                f"Finding enricher hook '{_hook_name(hook)}' failed for source '{finding.source_name}': {exc}"
            ) from exc
        if not isinstance(enriched, Finding):
            raise ExtensionError(
                f"Finding enricher hook '{_hook_name(hook)}' must return a Finding."
            )
    return enriched


def _finding_sort_key(finding: Finding) -> tuple[str, int, str, str]:
    severity_rank = {"error": 0, "warning": 1, "info": 2}[finding.severity]
    source_rank = "" if finding.source_name == "baseline" else finding.source_name
    return source_rank, severity_rank, finding.code, finding.column or ""


def _check_baseline_staleness(
    baseline_config: BaselineConfig,
    baseline_snapshot: ProjectSnapshot,
    current_snapshot: ProjectSnapshot,
) -> list[Finding]:
    if baseline_config.warning_age_seconds is None:
        return []

    age_seconds = int(
        (current_snapshot.generated_at - baseline_snapshot.generated_at).total_seconds()
    )
    if age_seconds < baseline_config.warning_age_seconds:
        return []

    return [
        Finding(
            source_name="baseline",
            severity="warning",
            code="baseline_stale",
            message=(
                "baseline snapshot is "
                f"{_format_duration(age_seconds)} old; refresh it with 'verixa snapshot'"
            ),
        )
    ]


def _warning_policy_sources(
    config: ProjectConfig,
    findings: list[Finding],
) -> tuple[str, ...]:
    warning_sources = {
        finding.source_name
        for finding in findings
        if finding.severity == "warning"
        and finding.source_name in config.sources
        and config.sources[finding.source_name].check.fail_on_warning
    }
    return tuple(sorted(warning_sources))


def _advisory_sources(config: ProjectConfig) -> tuple[str, ...]:
    return tuple(
        sorted(
            source_name
            for source_name, contract in config.sources.items()
            if contract.check.advisory
        )
    )


def _format_duration(seconds: int) -> str:
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, _ = divmod(remainder, 60)

    parts: list[str] = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes and len(parts) < 2:
        parts.append(f"{minutes}m")
    if not parts:
        parts.append("0m")
    return " ".join(parts[:2])


def _source_history(
    historical_snapshots: tuple[ProjectSnapshot, ...],
    source_name: str,
    *,
    window: int | None,
) -> tuple[SourceSnapshot, ...]:
    snapshots = [
        snapshot.sources[source_name]
        for snapshot in historical_snapshots
        if source_name in snapshot.sources
    ]
    if window is None:
        return tuple(snapshots)
    return tuple(snapshots[-window:])


def _use_history_mode(
    contract: SourceContract,
    source_history: tuple[SourceSnapshot, ...],
) -> bool:
    history_config = contract.history
    if history_config is None:
        return False
    return len(source_history) >= history_config.minimum_snapshots


def _default_confidence_for_code(code: str) -> str:
    if code in {
        "schema_column_missing",
        "schema_type_changed",
        "schema_column_added",
        "no_nulls_violation",
        "accepted_values_violation",
        "freshness_missing",
        "freshness_violated",
        "null_rate_changed",
        "numeric_p50_changed",
        "numeric_p95_changed",
    }:
        return "high"
    if code in {
        "row_count_changed",
        "row_count_history_band",
        "null_rate_history_band",
        "numeric_p50_history_band",
        "numeric_p95_history_band",
        "baseline_missing",
        "baseline_missing_for_environment",
        "baseline_missing_for_source",
        "baseline_stale",
    }:
        return "medium"
    return "medium"


def _downgrade_confidence(confidence: str) -> str:
    if confidence == "high":
        return "medium"
    if confidence == "medium":
        return "low"
    return "low"


def _hook_name(hook: object) -> str:
    hook_type = type(hook)
    module = getattr(hook, "__module__", hook_type.__module__)
    qualname = getattr(hook, "__qualname__", hook_type.__qualname__)
    return f"{module}.{qualname}"
