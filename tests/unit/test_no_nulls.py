from __future__ import annotations

from verixa.contracts.models import NoNullsTest, SourceContract
from verixa.rules.no_nulls import check_no_nulls
from tests.unit.test_support import make_source_snapshot


def test_no_nulls_reports_error_when_nulls_exist() -> None:
    contract = SourceContract(
        name="stripe.transactions",
        table="raw.stripe_transactions",
        schema={"amount": "FLOAT64"},
        freshness=None,
        tests=(NoNullsTest(column="amount"),),
    )
    current = make_source_snapshot(null_rates={"amount": 0.125})

    findings = check_no_nulls(contract, current)

    assert len(findings) == 1
    assert findings[0].severity == "error"
    assert "12.5%" in findings[0].message
