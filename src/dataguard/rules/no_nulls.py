"""No-nulls contract checks."""

from __future__ import annotations

from dataguard.contracts.models import SourceContract
from dataguard.diff.models import Finding
from dataguard.snapshot.models import SourceSnapshot


def check_no_nulls(contract: SourceContract, current: SourceSnapshot) -> list[Finding]:
    """Validate ``no_nulls`` tests against current null-rate data."""

    findings: list[Finding] = []
    for column in contract.no_null_columns:
        null_rate = current.null_rates.get(column)
        if null_rate is None or null_rate <= 0:
            continue
        findings.append(
            Finding(
                source_name=contract.name,
                severity="error",
                code="no_nulls_violation",
                column=column,
                message=(
                    f"no_nulls violated on {column}: {_format_percent(null_rate)} NULL values"
                ),
            )
        )
    return findings


def _format_percent(value: float) -> str:
    return f"{value * 100:.1f}%"
