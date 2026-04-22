"""Schema drift checks."""

from __future__ import annotations

from dataguard.contracts.models import SourceContract
from dataguard.diff.models import Finding
from dataguard.snapshot.models import SourceSnapshot


def check_schema_drift(
    contract: SourceContract,
    current: SourceSnapshot,
) -> list[Finding]:
    """Detect schema changes between the contract and current warehouse state."""

    findings: list[Finding] = []
    expected_schema = contract.schema
    current_schema = current.schema

    for column, expected_type in sorted(expected_schema.items()):
        if column not in current_schema:
            findings.append(
                Finding(
                    source_name=contract.name,
                    severity="error",
                    code="schema_column_missing",
                    column=column,
                    message=f"column removed: {column}",
                )
            )
            continue
        current_type = current_schema[column]
        if current_type != expected_type:
            findings.append(
                Finding(
                    source_name=contract.name,
                    severity="error",
                    code="schema_type_changed",
                    column=column,
                    message=(
                        f"column type changed on {column}: {expected_type} -> {current_type}"
                    ),
                )
            )

    for column in sorted(set(current_schema) - set(expected_schema)):
        findings.append(
            Finding(
                source_name=contract.name,
                severity="warning",
                code="schema_column_added",
                column=column,
                message=f"column added: {column}",
            )
        )

    return findings
