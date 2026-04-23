"""History-aware drift checks for noisy sources."""

from __future__ import annotations

from statistics import median

from verixa.contracts.models import (
    HistoryDriftConfig,
    NullRateChangeThresholds,
    NumericDistributionChangeThresholds,
    RowCountChangeThresholds,
    SourceContract,
)
from verixa.diff.models import Finding
from verixa.snapshot.models import SourceSnapshot


def check_row_count_history_band(
    contract: SourceContract,
    history: tuple[SourceSnapshot, ...],
    current: SourceSnapshot,
    *,
    thresholds: RowCountChangeThresholds,
    history_config: HistoryDriftConfig,
) -> list[Finding]:
    """Detect row-count values outside a rolling historical band."""

    if history_config.backfill_mode:
        return []

    values = [float(item.row_count) for item in history if item.row_count is not None]
    if len(values) < history_config.minimum_snapshots or current.row_count is None:
        return []

    center = median(values)
    if center <= 0:
        return []

    warning_lower = center * (1 - thresholds.warning_drop_ratio)
    error_lower = center * (1 - thresholds.error_drop_ratio)
    warning_upper = center * (1 + thresholds.warning_growth_ratio)
    error_upper = center * (1 + thresholds.error_growth_ratio)
    current_value = float(current.row_count)

    severity: str | None = None
    if current_value < warning_lower:
        severity = "error" if current_value <= error_lower else "warning"
    elif current_value > warning_upper:
        severity = "error" if current_value >= error_upper else "warning"
    if severity is None:
        return []

    return [
        Finding(
            source_name=contract.name,
            severity=severity,
            code="row_count_history_band",
            message=(
                f"row count outside historical band: {current.row_count} vs median {center:.0f} "
                f"(expected {_format_number(warning_lower)}-{_format_number(warning_upper)} "
                f"from last {len(values)} runs)"
            ),
            history_metric="row_count",
            history_window=history_config.window,
            history_sample_size=len(values),
            history_center_value=center,
            history_lower_bound=warning_lower,
            history_upper_bound=warning_upper,
        )
    ]


def check_null_rate_history_band(
    contract: SourceContract,
    history: tuple[SourceSnapshot, ...],
    current: SourceSnapshot,
    *,
    thresholds: NullRateChangeThresholds,
    history_config: HistoryDriftConfig,
) -> list[Finding]:
    """Detect null-rate increases relative to recent history."""

    findings: list[Finding] = []
    for column in contract.declared_columns:
        values = [
            value
            for snapshot in history
            if (value := snapshot.null_rates.get(column)) is not None
        ]
        current_value = current.null_rates.get(column)
        if len(values) < history_config.minimum_snapshots or current_value is None:
            continue

        center = median(values)
        warning_upper = center + thresholds.warning_delta
        error_upper = center + thresholds.error_delta
        if current_value < warning_upper:
            continue

        severity = "error" if current_value >= error_upper else "warning"
        findings.append(
            Finding(
                source_name=contract.name,
                severity=severity,
                code="null_rate_history_band",
                column=column,
                message=(
                    f"null rate outside historical band on {column}: "
                    f"{_format_percent(current_value)} vs median {_format_percent(center)} "
                    f"(expected <= {_format_percent(warning_upper)} from last {len(values)} runs)"
                ),
                history_metric=f"null_rate:{column}",
                history_window=history_config.window,
                history_sample_size=len(values),
                history_center_value=center,
                history_lower_bound=0.0,
                history_upper_bound=warning_upper,
            )
        )
    return findings


def check_numeric_history_band(
    contract: SourceContract,
    history: tuple[SourceSnapshot, ...],
    current: SourceSnapshot,
    *,
    thresholds: NumericDistributionChangeThresholds,
    history_config: HistoryDriftConfig,
) -> list[Finding]:
    """Detect numeric p50/p95 values outside recent historical bands."""

    findings: list[Finding] = []
    for column in contract.numeric_summary_columns:
        findings.extend(
            _check_numeric_metric(
                contract=contract,
                history=history,
                current=current,
                column=column,
                metric_name="p50",
                code="numeric_p50_history_band",
                thresholds=thresholds,
                history_config=history_config,
            )
        )
        findings.extend(
            _check_numeric_metric(
                contract=contract,
                history=history,
                current=current,
                column=column,
                metric_name="p95",
                code="numeric_p95_history_band",
                thresholds=thresholds,
                history_config=history_config,
            )
        )
    return findings


def _check_numeric_metric(
    *,
    contract: SourceContract,
    history: tuple[SourceSnapshot, ...],
    current: SourceSnapshot,
    column: str,
    metric_name: str,
    code: str,
    thresholds: NumericDistributionChangeThresholds,
    history_config: HistoryDriftConfig,
) -> list[Finding]:
    values = []
    for snapshot in history:
        summary = snapshot.numeric_summaries.get(column)
        if summary is None:
            continue
        value = getattr(summary, f"{metric_name}_value")
        if value is not None:
            values.append(value)

    current_summary = current.numeric_summaries.get(column)
    current_value = None if current_summary is None else getattr(current_summary, f"{metric_name}_value")
    if len(values) < history_config.minimum_snapshots or current_value is None:
        return []

    center = float(median(values))
    if abs(center) < thresholds.minimum_baseline_value:
        return []

    warning_delta = abs(center) * thresholds.warning_relative_delta
    error_delta = abs(center) * thresholds.error_relative_delta
    warning_lower = center - warning_delta
    warning_upper = center + warning_delta
    error_lower = center - error_delta
    error_upper = center + error_delta

    severity: str | None = None
    if current_value < warning_lower:
        severity = "error" if current_value <= error_lower else "warning"
    elif current_value > warning_upper:
        severity = "error" if current_value >= error_upper else "warning"
    if severity is None:
        return []

    return [
        Finding(
            source_name=contract.name,
            severity=severity,
            code=code,
            column=column,
            message=(
                f"{metric_name} outside historical band on {column}: "
                f"{_format_number(current_value)} vs median {_format_number(center)} "
                f"(expected {_format_number(warning_lower)}-{_format_number(warning_upper)} "
                f"from last {len(values)} runs)"
            ),
            history_metric=f"{metric_name}:{column}",
            history_window=history_config.window,
            history_sample_size=len(values),
            history_center_value=center,
            history_lower_bound=warning_lower,
            history_upper_bound=warning_upper,
        )
    ]


def _format_percent(value: float) -> str:
    return f"{value * 100:.1f}%"


def _format_number(value: float) -> str:
    if abs(value) >= 100:
        formatted = f"{value:.0f}"
    else:
        formatted = f"{value:.4f}"
    return formatted.rstrip("0").rstrip(".")
