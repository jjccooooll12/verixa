"""Null-rate drift checks."""

from __future__ import annotations

from verixa.contracts.models import NullRateChangeThresholds, SourceContract
from verixa.diff.models import Finding
from verixa.snapshot.models import SourceSnapshot


def check_null_rate_changes(
    contract: SourceContract,
    baseline: SourceSnapshot,
    current: SourceSnapshot,
    thresholds: NullRateChangeThresholds,
) -> list[Finding]:
    """Detect meaningful increases in null rates since the baseline snapshot."""

    findings: list[Finding] = []
    for column in contract.declared_columns:
        baseline_rate = baseline.null_rates.get(column)
        current_rate = current.null_rates.get(column)
        if baseline_rate is None or current_rate is None:
            continue
        delta = current_rate - baseline_rate
        if delta < thresholds.warning_delta:
            continue
        severity = "error" if delta >= thresholds.error_delta else "warning"
        findings.append(
            Finding(
                source_name=contract.name,
                severity=severity,
                code="null_rate_changed",
                column=column,
                message=(
                    f"null rate increased on {column}: "
                    f"{_format_percent(baseline_rate)} -> {_format_percent(current_rate)}"
                ),
            )
        )
    return findings


def _format_percent(value: float) -> str:
    return f"{value * 100:.1f}%"
