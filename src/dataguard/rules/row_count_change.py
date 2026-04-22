"""Row-count drift checks."""

from __future__ import annotations

from dataguard.contracts.models import RowCountChangeThresholds, SourceContract
from dataguard.diff.models import Finding
from dataguard.snapshot.models import SourceSnapshot


def check_row_count_change(
    contract: SourceContract,
    baseline: SourceSnapshot,
    current: SourceSnapshot,
    thresholds: RowCountChangeThresholds,
) -> list[Finding]:
    """Detect substantial row-count differences from the baseline snapshot."""

    if baseline.row_count is None or current.row_count is None:
        return []
    if baseline.row_count == 0:
        return []

    ratio = current.row_count / baseline.row_count
    if ratio < 1:
        drop_ratio = 1 - ratio
        if drop_ratio < thresholds.warning_drop_ratio:
            return []
        severity = (
            "error" if drop_ratio >= thresholds.error_drop_ratio else "warning"
        )
    else:
        growth_ratio = ratio - 1
        if growth_ratio < thresholds.warning_growth_ratio:
            return []
        severity = (
            "error" if growth_ratio >= thresholds.error_growth_ratio else "warning"
        )

    return [
        Finding(
            source_name=contract.name,
            severity=severity,
            code="row_count_changed",
            message=(
                f"row count changed: {baseline.row_count} -> {current.row_count} "
                f"({ratio:.2f}x baseline)"
            ),
        )
    ]
