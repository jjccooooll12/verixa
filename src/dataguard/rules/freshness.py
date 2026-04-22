"""Freshness SLA checks."""

from __future__ import annotations

from dataguard.contracts.models import SourceContract
from dataguard.diff.models import Finding
from dataguard.snapshot.models import SourceSnapshot


def check_freshness(contract: SourceContract, current: SourceSnapshot) -> list[Finding]:
    """Validate freshness requirements for the current snapshot."""

    if contract.freshness is None or current.freshness is None:
        return []
    if current.freshness.latest_value is None or current.freshness.age_seconds is None:
        return [
            Finding(
                source_name=contract.name,
                severity="error",
                code="freshness_missing",
                column=contract.freshness.column,
                message=(
                    f"freshness violated on {contract.freshness.column}: no latest timestamp found"
                ),
            )
        ]
    if current.freshness.age_seconds <= contract.freshness.max_age_seconds:
        return []
    return [
        Finding(
            source_name=contract.name,
            severity="error",
            code="freshness_violated",
            column=contract.freshness.column,
            message=(
                "freshness violated: latest row is "
                f"{_format_duration(current.freshness.age_seconds)} old"
            ),
        )
    ]


def _format_duration(seconds: int) -> str:
    hours, remainder = divmod(seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"
