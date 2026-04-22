from __future__ import annotations

from verixa.contracts.models import FreshnessConfig, SourceContract
from verixa.rules.freshness import check_freshness
from tests.unit.test_support import make_freshness_snapshot, make_source_snapshot


def test_freshness_reports_stale_data() -> None:
    contract = SourceContract(
        name="stripe.transactions",
        table="raw.stripe_transactions",
        schema={"created_at": "TIMESTAMP"},
        freshness=FreshnessConfig(column="created_at", max_age="1h", max_age_seconds=3600),
        tests=(),
    )
    current = make_source_snapshot(
        freshness=make_freshness_snapshot(age_seconds=7200),
    )

    findings = check_freshness(contract, current)

    assert len(findings) == 1
    assert findings[0].severity == "error"
    assert "2h 0m" in findings[0].message
