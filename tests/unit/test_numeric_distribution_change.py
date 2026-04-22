from __future__ import annotations

from verixa.contracts.models import NumericDistributionChangeThresholds, SourceContract
from verixa.rules.numeric_distribution_change import check_numeric_distribution_changes
from tests.unit.test_support import make_numeric_summary_snapshot, make_source_snapshot


def test_numeric_distribution_change_reports_warning_and_error_thresholds() -> None:
    contract = SourceContract(
        name="stripe.transactions",
        table="raw.stripe_transactions",
        schema={"amount": "FLOAT64"},
        freshness=None,
        tests=(),
    )
    baseline = make_source_snapshot(
        numeric_summaries={
            "amount": make_numeric_summary_snapshot(
                "amount",
                p50_value=100.0,
                p95_value=100.0,
            )
        }
    )
    current = make_source_snapshot(
        numeric_summaries={
            "amount": make_numeric_summary_snapshot(
                "amount",
                p50_value=130.0,
                p95_value=170.0,
            )
        }
    )

    findings = check_numeric_distribution_changes(
        contract,
        baseline,
        current,
        thresholds=NumericDistributionChangeThresholds(),
    )

    assert [finding.code for finding in findings] == [
        "numeric_p50_changed",
        "numeric_p95_changed",
    ]
    assert [finding.severity for finding in findings] == ["warning", "error"]


def test_numeric_distribution_change_skips_small_baseline_values() -> None:
    contract = SourceContract(
        name="stripe.transactions",
        table="raw.stripe_transactions",
        schema={"amount": "FLOAT64"},
        freshness=None,
        tests=(),
    )
    baseline = make_source_snapshot(
        numeric_summaries={
            "amount": make_numeric_summary_snapshot(
                "amount",
                p50_value=0.5,
                p95_value=0.5,
            )
        }
    )
    current = make_source_snapshot(
        numeric_summaries={
            "amount": make_numeric_summary_snapshot(
                "amount",
                p50_value=5.0,
                p95_value=5.0,
            )
        }
    )

    findings = check_numeric_distribution_changes(
        contract,
        baseline,
        current,
        thresholds=NumericDistributionChangeThresholds(minimum_baseline_value=1.0),
    )

    assert findings == []
