from __future__ import annotations

from verixa.contracts.models import SourceContract
from verixa.rules.schema_drift import check_schema_drift
from tests.unit.test_support import make_source_snapshot


def test_schema_drift_detects_removed_changed_and_added_columns() -> None:
    contract = SourceContract(
        name="stripe.transactions",
        table="raw.stripe_transactions",
        schema={"amount": "FLOAT64", "currency": "STRING"},
        freshness=None,
        tests=(),
    )
    current = make_source_snapshot(
        schema={"amount": "NUMERIC", "extra": "STRING"},
    )

    findings = check_schema_drift(contract, current)

    assert [finding.code for finding in findings] == [
        "schema_type_changed",
        "schema_column_missing",
        "schema_column_added",
    ]
