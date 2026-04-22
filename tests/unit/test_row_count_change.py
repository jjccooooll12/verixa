from __future__ import annotations

from dataguard.contracts.models import RowCountChangeThresholds, SourceContract
from dataguard.rules.row_count_change import check_row_count_change
from tests.unit.test_support import make_source_snapshot


def test_row_count_change_reports_large_drops() -> None:
    contract = SourceContract(
        name="stripe.transactions",
        table="raw.stripe_transactions",
        schema={"amount": "FLOAT64"},
        freshness=None,
        tests=(),
    )
    baseline = make_source_snapshot(row_count=1000)
    current = make_source_snapshot(row_count=400)

    findings = check_row_count_change(
        contract,
        baseline,
        current,
        thresholds=RowCountChangeThresholds(),
    )

    assert len(findings) == 1
    assert findings[0].severity == "error"
    assert "1000 -> 400" in findings[0].message


def test_row_count_change_uses_configured_thresholds() -> None:
    contract = SourceContract(
        name="stripe.transactions",
        table="raw.stripe_transactions",
        schema={"amount": "FLOAT64"},
        freshness=None,
        tests=(),
    )
    baseline = make_source_snapshot(row_count=1000)
    current = make_source_snapshot(row_count=780)

    findings = check_row_count_change(
        contract,
        baseline,
        current,
        thresholds=RowCountChangeThresholds(
            warning_drop_ratio=0.10,
            error_drop_ratio=0.20,
            warning_growth_ratio=0.10,
            error_growth_ratio=0.50,
        ),
    )

    assert len(findings) == 1
    assert findings[0].severity == "error"
