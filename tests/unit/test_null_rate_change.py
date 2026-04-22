from __future__ import annotations

from verixa.contracts.models import NullRateChangeThresholds, SourceContract
from verixa.rules.null_rate_change import check_null_rate_changes
from tests.unit.test_support import make_source_snapshot


def test_null_rate_change_reports_warning_and_error_thresholds() -> None:
    contract = SourceContract(
        name="stripe.transactions",
        table="raw.stripe_transactions",
        schema={"amount": "FLOAT64", "currency": "STRING"},
        freshness=None,
        tests=(),
    )
    baseline = make_source_snapshot(null_rates={"amount": 0.00, "currency": 0.01})
    current = make_source_snapshot(null_rates={"amount": 0.02, "currency": 0.08})

    findings = check_null_rate_changes(
        contract,
        baseline,
        current,
        thresholds=NullRateChangeThresholds(),
    )

    assert [finding.severity for finding in findings] == ["warning", "error"]


def test_null_rate_change_uses_configured_thresholds() -> None:
    contract = SourceContract(
        name="stripe.transactions",
        table="raw.stripe_transactions",
        schema={"amount": "FLOAT64"},
        freshness=None,
        tests=(),
    )
    baseline = make_source_snapshot(null_rates={"amount": 0.00})
    current = make_source_snapshot(null_rates={"amount": 0.03})

    findings = check_null_rate_changes(
        contract,
        baseline,
        current,
        thresholds=NullRateChangeThresholds(warning_delta=0.02, error_delta=0.03),
    )

    assert len(findings) == 1
    assert findings[0].severity == "error"
