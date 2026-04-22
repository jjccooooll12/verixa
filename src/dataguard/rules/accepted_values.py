"""Accepted-values contract checks."""

from __future__ import annotations

from dataguard.contracts.models import SourceContract
from dataguard.diff.models import Finding
from dataguard.snapshot.models import SourceSnapshot


def check_accepted_values(contract: SourceContract, current: SourceSnapshot) -> list[Finding]:
    """Validate accepted-values tests against current warehouse data."""

    findings: list[Finding] = []
    for test in contract.accepted_values_tests:
        stats = current.accepted_values.get(test.column)
        if stats is None or stats.invalid_count <= 0:
            continue
        examples = ""
        if stats.invalid_examples:
            examples = f" (samples: {', '.join(stats.invalid_examples)})"
        findings.append(
            Finding(
                source_name=contract.name,
                severity="error",
                code="accepted_values_violation",
                column=test.column,
                message=(
                    f"accepted values violated on {test.column}: "
                    f"{stats.invalid_count} invalid rows{examples}"
                ),
            )
        )
    return findings
