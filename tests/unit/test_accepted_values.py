from __future__ import annotations

from verixa.contracts.models import AcceptedValuesTest, SourceContract
from verixa.rules.accepted_values import check_accepted_values
from tests.unit.test_support import make_accepted_values_snapshot, make_source_snapshot


def test_accepted_values_reports_invalid_rows() -> None:
    contract = SourceContract(
        name="stripe.transactions",
        table="raw.stripe_transactions",
        schema={"currency": "STRING"},
        freshness=None,
        tests=(AcceptedValuesTest(column="currency", values=("USD", "EUR")),),
    )
    current = make_source_snapshot(
        accepted_values={
            "currency": make_accepted_values_snapshot("currency", invalid_count=3, examples=("AUD", "CAD"))
        }
    )

    findings = check_accepted_values(contract, current)

    assert len(findings) == 1
    assert findings[0].severity == "error"
    assert "AUD, CAD" in findings[0].message
