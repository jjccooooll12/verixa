from __future__ import annotations

from datetime import datetime, timezone

from verixa.contracts.models import (
    BaselineConfig,
    CheckConfig,
    ProjectConfig,
    RowCountChangeThresholds,
    RulesConfig,
    SourceContract,
    WarehouseConfig,
)
from verixa.diff.engine import build_plan_result
from verixa.diff.risk import RiskConfig, SourceRiskHints
from verixa.snapshot.models import ProjectSnapshot
from tests.unit.test_support import make_numeric_summary_snapshot, make_source_snapshot


def test_build_plan_result_attaches_risks_and_baseline_findings() -> None:
    contract = SourceContract(
        name="stripe.transactions",
        table="raw.stripe_transactions",
        schema={"amount": "FLOAT64", "currency": "STRING"},
        freshness=None,
        tests=(),
    )
    config = ProjectConfig(
        warehouse=WarehouseConfig(kind="bigquery", project="demo"),
        sources={"stripe.transactions": contract},
    )
    baseline = ProjectSnapshot(
        warehouse_kind="bigquery",
        generated_at=datetime(2026, 4, 22, 11, 0, tzinfo=timezone.utc),
        sources={
            "stripe.transactions": make_source_snapshot(
                schema={"amount": "FLOAT64", "currency": "STRING"},
                row_count=100,
                null_rates={"amount": 0.0, "currency": 0.0},
            )
        },
    )
    current = ProjectSnapshot(
        warehouse_kind="bigquery",
        generated_at=datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc),
        sources={
            "stripe.transactions": make_source_snapshot(
                schema={"amount": "FLOAT64"},
                row_count=40,
                null_rates={"amount": 0.06, "currency": 0.0},
            )
        },
    )
    risk_config = RiskConfig(
        sources={
            "stripe.transactions": SourceRiskHints(
                general=("likely undercount in dashboards",),
                columns={"currency": ("likely to break finance models",)},
            )
        }
    )

    result = build_plan_result(config, baseline, current, risk_config=risk_config)

    assert result.error_count >= 2
    assert any(finding.code == "schema_column_missing" for finding in result.findings)
    schema_finding = next(
        finding for finding in result.findings if finding.code == "schema_column_missing"
    )
    assert schema_finding.risks == (
        "likely undercount in dashboards",
        "likely to break finance models",
    )


def test_build_plan_result_uses_source_level_rule_overrides() -> None:
    contract = SourceContract(
        name="stripe.transactions",
        table="raw.stripe_transactions",
        schema={"amount": "FLOAT64"},
        freshness=None,
        tests=(),
        rules=RulesConfig(
            row_count_change=RowCountChangeThresholds(
                warning_drop_ratio=0.10,
                error_drop_ratio=0.20,
                warning_growth_ratio=0.20,
                error_growth_ratio=1.0,
            )
        ),
    )
    config = ProjectConfig(
        warehouse=WarehouseConfig(kind="bigquery", project="demo"),
        sources={"stripe.transactions": contract},
    )
    baseline = ProjectSnapshot(
        warehouse_kind="bigquery",
        generated_at=datetime(2026, 4, 22, 11, 0, tzinfo=timezone.utc),
        sources={"stripe.transactions": make_source_snapshot(row_count=100)},
    )
    current = ProjectSnapshot(
        warehouse_kind="bigquery",
        generated_at=datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc),
        sources={"stripe.transactions": make_source_snapshot(row_count=85)},
    )

    result = build_plan_result(config, baseline, current)

    assert any(finding.code == "row_count_changed" for finding in result.findings)


def test_build_plan_result_warns_when_baseline_is_stale() -> None:
    contract = SourceContract(
        name="stripe.transactions",
        table="raw.stripe_transactions",
        schema={"amount": "FLOAT64"},
        freshness=None,
        tests=(),
    )
    config = ProjectConfig(
        warehouse=WarehouseConfig(kind="bigquery", project="demo"),
        sources={"stripe.transactions": contract},
        baseline=BaselineConfig(warning_age="24h", warning_age_seconds=24 * 3600),
    )
    baseline = ProjectSnapshot(
        warehouse_kind="bigquery",
        generated_at=datetime(2026, 4, 20, 12, 0, tzinfo=timezone.utc),
        sources={"stripe.transactions": make_source_snapshot(row_count=100)},
    )
    current = ProjectSnapshot(
        warehouse_kind="bigquery",
        generated_at=datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc),
        sources={"stripe.transactions": make_source_snapshot(row_count=100)},
    )

    result = build_plan_result(config, baseline, current)

    stale_finding = next(
        finding for finding in result.findings if finding.code == "baseline_stale"
    )
    assert stale_finding.source_name == "baseline"
    assert "refresh it with 'verixa snapshot'" in stale_finding.message


def test_build_plan_result_tracks_warning_policy_sources() -> None:
    contract = SourceContract(
        name="stripe.transactions",
        table="raw.stripe_transactions",
        schema={"amount": "FLOAT64"},
        freshness=None,
        tests=(),
        check=CheckConfig(fail_on_warning=True),
    )
    config = ProjectConfig(
        warehouse=WarehouseConfig(kind="bigquery", project="demo"),
        sources={"stripe.transactions": contract},
    )
    baseline = ProjectSnapshot(
        warehouse_kind="bigquery",
        generated_at=datetime(2026, 4, 22, 11, 0, tzinfo=timezone.utc),
        sources={"stripe.transactions": make_source_snapshot(row_count=100)},
    )
    current = ProjectSnapshot(
        warehouse_kind="bigquery",
        generated_at=datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc),
        sources={"stripe.transactions": make_source_snapshot(row_count=80)},
    )

    result = build_plan_result(config, baseline, current)

    assert result.warning_policy_sources == ("stripe.transactions",)


def test_build_plan_result_includes_numeric_distribution_findings() -> None:
    contract = SourceContract(
        name="stripe.transactions",
        table="raw.stripe_transactions",
        schema={"amount": "FLOAT64"},
        freshness=None,
        tests=(),
    )
    config = ProjectConfig(
        warehouse=WarehouseConfig(kind="bigquery", project="demo"),
        sources={"stripe.transactions": contract},
    )
    baseline = ProjectSnapshot(
        warehouse_kind="bigquery",
        generated_at=datetime(2026, 4, 22, 11, 0, tzinfo=timezone.utc),
        sources={
            "stripe.transactions": make_source_snapshot(
                numeric_summaries={
                    "amount": make_numeric_summary_snapshot(
                        "amount",
                        p50_value=100.0,
                        p95_value=100.0,
                    )
                }
            )
        },
    )
    current = ProjectSnapshot(
        warehouse_kind="bigquery",
        generated_at=datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc),
        sources={
            "stripe.transactions": make_source_snapshot(
                numeric_summaries={
                    "amount": make_numeric_summary_snapshot(
                        "amount",
                        p50_value=100.0,
                        p95_value=180.0,
                    )
                }
            )
        },
    )

    result = build_plan_result(config, baseline, current)

    finding = next(
        finding for finding in result.findings if finding.code == "numeric_p95_changed"
    )
    assert finding.severity == "error"
    assert "p95 changed on amount" in finding.message
