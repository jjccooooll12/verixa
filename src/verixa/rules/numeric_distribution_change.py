"""Numeric summary drift checks."""

from __future__ import annotations

from verixa.contracts.models import NumericDistributionChangeThresholds, SourceContract
from verixa.diff.models import Finding
from verixa.snapshot.models import SourceSnapshot


def check_numeric_distribution_changes(
    contract: SourceContract,
    baseline: SourceSnapshot,
    current: SourceSnapshot,
    thresholds: NumericDistributionChangeThresholds,
) -> list[Finding]:
    """Detect meaningful p50/p95 drift for numeric columns since baseline."""

    findings: list[Finding] = []
    for column in contract.numeric_summary_columns:
        baseline_summary = baseline.numeric_summaries.get(column)
        current_summary = current.numeric_summaries.get(column)
        if baseline_summary is None or current_summary is None:
            continue

        findings.extend(
            _check_metric(
                contract_name=contract.name,
                column=column,
                metric_name="p50",
                code="numeric_p50_changed",
                baseline_value=baseline_summary.p50_value,
                current_value=current_summary.p50_value,
                thresholds=thresholds,
            )
        )
        findings.extend(
            _check_metric(
                contract_name=contract.name,
                column=column,
                metric_name="p95",
                code="numeric_p95_changed",
                baseline_value=baseline_summary.p95_value,
                current_value=current_summary.p95_value,
                thresholds=thresholds,
            )
        )
    return findings


def _check_metric(
    *,
    contract_name: str,
    column: str,
    metric_name: str,
    code: str,
    baseline_value: float | None,
    current_value: float | None,
    thresholds: NumericDistributionChangeThresholds,
) -> list[Finding]:
    if baseline_value is None or current_value is None:
        return []
    if abs(baseline_value) < thresholds.minimum_baseline_value:
        return []

    relative_delta = abs(current_value - baseline_value) / abs(baseline_value)
    if relative_delta < thresholds.warning_relative_delta:
        return []

    severity = (
        "error"
        if relative_delta >= thresholds.error_relative_delta
        else "warning"
    )
    return [
        Finding(
            source_name=contract_name,
            severity=severity,
            code=code,
            column=column,
            message=(
                f"{metric_name} changed on {column}: "
                f"{_format_number(baseline_value)} -> {_format_number(current_value)} "
                f"({relative_delta * 100:.1f}% delta)"
            ),
        )
    ]


def _format_number(value: float) -> str:
    formatted = f"{value:.4f}"
    return formatted.rstrip("0").rstrip(".")
