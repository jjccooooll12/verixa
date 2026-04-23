"""Diff orchestration across contracts, baselines, and current snapshots."""

from __future__ import annotations

from verixa.contracts.models import BaselineConfig, ProjectConfig
from verixa.diff.models import DiffResult, Finding
from verixa.diff.risk import RiskConfig, SourceRiskHints
from verixa.findings.schema import stable_code_for_internal
from verixa.rules.accepted_values import check_accepted_values
from verixa.rules.freshness import check_freshness
from verixa.rules.no_nulls import check_no_nulls
from verixa.rules.numeric_distribution_change import check_numeric_distribution_changes
from verixa.rules.null_rate_change import check_null_rate_changes
from verixa.rules.row_count_change import check_row_count_change
from verixa.rules.schema_drift import check_schema_drift
from verixa.snapshot.models import ProjectSnapshot


def build_test_result(
    config: ProjectConfig,
    current_snapshot: ProjectSnapshot,
    risk_config: RiskConfig | None = None,
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
    return _finalize(
        findings,
        config=config,
        sources_checked=len(config.sources),
        used_baseline=False,
        risk_config=risk_config,
    )


def build_plan_result(
    config: ProjectConfig,
    baseline_snapshot: ProjectSnapshot,
    current_snapshot: ProjectSnapshot,
    risk_config: RiskConfig | None = None,
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

        findings.extend(check_schema_drift(contract, current))
        findings.extend(check_no_nulls(contract, current))
        findings.extend(check_freshness(contract, current))
        findings.extend(check_accepted_values(contract, current))

        if baseline is None:
            findings.append(
                Finding(
                    source_name=source_name,
                    severity="warning",
                    code="baseline_missing_for_source",
                    message="baseline missing for source; run 'verixa snapshot' to refresh it",
                )
            )
            continue

        findings.extend(
            check_null_rate_changes(
                contract,
                baseline,
                current,
                thresholds=contract.rules.null_rate_change,
            )
        )
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

    return _finalize(
        findings,
        config=config,
        sources_checked=len(config.sources),
        used_baseline=True,
        risk_config=risk_config,
    )


def _finalize(
    findings: list[Finding],
    *,
    config: ProjectConfig,
    sources_checked: int,
    used_baseline: bool,
    risk_config: RiskConfig | None,
) -> DiffResult:
    enriched = [
        _apply_severity_override(
            _attach_risks(finding, risk_config),
            config,
        )
        for finding in findings
    ]
    ordered = tuple(sorted(enriched, key=_finding_sort_key))
    return DiffResult(
        findings=ordered,
        sources_checked=sources_checked,
        used_baseline=used_baseline,
        warning_policy_sources=_warning_policy_sources(config, list(ordered)),
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
    )


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
