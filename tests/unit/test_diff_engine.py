from __future__ import annotations

from datetime import datetime, timezone

import pytest

from verixa.contracts.models import (
    BaselineConfig,
    CheckConfig,
    ExtensionsConfig,
    HistoryDriftConfig,
    ProjectConfig,
    RowCountChangeThresholds,
    RulesConfig,
    ScanConfig,
    SourceContract,
    WarehouseConfig,
)
from verixa.diff.engine import build_plan_result, build_test_result
from verixa.diff.risk import RiskConfig, SourceRiskHints
from verixa.extensions.api import ExtensionError
from verixa.findings.schema import normalize_diff_result
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
                owners=("finance", "data-platform"),
                criticality="high",
                downstream_models=("stg_orders", "fct_orders"),
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
    assert schema_finding.owners == ("finance", "data-platform")
    assert schema_finding.source_criticality == "high"
    assert schema_finding.downstream_models == ("stg_orders", "fct_orders")


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
        sources={"stripe.transactions": make_source_snapshot(row_count=79)},
    )

    result = build_plan_result(config, baseline, current)

    assert result.warning_policy_sources == ("stripe.transactions",)


def test_build_plan_result_applies_severity_overrides_by_stable_code() -> None:
    contract = SourceContract(
        name="stripe.transactions",
        table="raw.stripe_transactions",
        schema={"amount": "FLOAT64"},
        freshness=None,
        tests=(),
        severity_overrides={"drift.row_count_changed": "error"},
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
        sources={"stripe.transactions": make_source_snapshot(row_count=79)},
    )

    result = build_plan_result(config, baseline, current)

    finding = next(finding for finding in result.findings if finding.code == "row_count_changed")
    assert finding.severity == "error"


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


def test_build_plan_result_marks_bounded_scan_findings_with_confidence_note() -> None:
    contract = SourceContract(
        name="stripe.transactions",
        table="raw.stripe_transactions",
        schema={"amount": "FLOAT64", "created_at": "TIMESTAMP"},
        freshness=None,
        tests=(),
        scan=ScanConfig(
            timestamp_column="created_at",
            column_type="TIMESTAMP",
            lookback="30d",
            lookback_seconds=30 * 24 * 3600,
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
        sources={"stripe.transactions": make_source_snapshot(row_count=79)},
    )

    result = build_plan_result(config, baseline, current, execution_mode="bounded")
    normalized = normalize_diff_result(result)

    finding = next(item for item in normalized if item.code == "row_count_changed")
    assert finding.confidence == "low"
    assert finding.confidence_reason == "bounded mode evaluated a bounded window of 30d on created_at"


def test_build_plan_result_marks_cheap_row_count_from_metadata() -> None:
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
        sources={"stripe.transactions": make_source_snapshot(row_count=100)},
    )
    current = ProjectSnapshot(
        warehouse_kind="bigquery",
        generated_at=datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc),
        sources={"stripe.transactions": make_source_snapshot(row_count=79)},
    )

    result = build_plan_result(config, baseline, current, execution_mode="cheap")
    normalized = normalize_diff_result(result)

    finding = next(item for item in normalized if item.code == "row_count_changed")
    assert finding.confidence == "low"
    assert finding.confidence_reason == "cheap mode relies on warehouse metadata row counts instead of exact counts"


def test_build_plan_result_uses_history_band_for_row_count_when_enabled() -> None:
    contract = SourceContract(
        name="stripe.transactions",
        table="raw.stripe_transactions",
        schema={"amount": "FLOAT64"},
        freshness=None,
        tests=(),
        history=HistoryDriftConfig(window=5, minimum_snapshots=3),
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
        sources={"stripe.transactions": make_source_snapshot(row_count=140)},
    )
    history = (
        ProjectSnapshot(
            warehouse_kind="bigquery",
            generated_at=datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc),
            sources={"stripe.transactions": make_source_snapshot(row_count=100)},
        ),
        ProjectSnapshot(
            warehouse_kind="bigquery",
            generated_at=datetime(2026, 4, 20, 12, 0, tzinfo=timezone.utc),
            sources={"stripe.transactions": make_source_snapshot(row_count=102)},
        ),
        ProjectSnapshot(
            warehouse_kind="bigquery",
            generated_at=datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc),
            sources={"stripe.transactions": make_source_snapshot(row_count=98)},
        ),
    )

    result = build_plan_result(
        config,
        baseline,
        current,
        historical_snapshots=history,
    )

    assert any(finding.code == "row_count_history_band" for finding in result.findings)
    assert not any(finding.code == "row_count_changed" for finding in result.findings)
    normalized = normalize_diff_result(result)
    finding = next(item for item in normalized if item.code == "row_count_history_band")
    assert finding.change_type == "historical_drift"
    assert finding.history_metric == "row_count"
    assert finding.history_sample_size == 3


def test_build_plan_result_skips_row_count_drift_in_backfill_mode() -> None:
    contract = SourceContract(
        name="stripe.transactions",
        table="raw.stripe_transactions",
        schema={"amount": "FLOAT64"},
        freshness=None,
        tests=(),
        history=HistoryDriftConfig(backfill_mode=True),
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
        sources={"stripe.transactions": make_source_snapshot(row_count=300)},
    )

    result = build_plan_result(config, baseline, current)

    assert not any(
        finding.code in {"row_count_changed", "row_count_history_band"}
        for finding in result.findings
    )


def test_build_test_result_runs_custom_checks_and_enrichers() -> None:
    from tests.unit.extensions_demo import custom_check, finding_enricher

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
        extensions=ExtensionsConfig(
            checks=(custom_check,),
            finding_enrichers=(finding_enricher,),
        ),
    )
    current = ProjectSnapshot(
        warehouse_kind="bigquery",
        generated_at=datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc),
        sources={"stripe.transactions": make_source_snapshot(row_count=42)},
    )

    result = build_test_result(config, current, execution_mode="cheap")

    finding = next(finding for finding in result.findings if finding.code == "custom_demo_check")
    assert finding.message == "custom demo check saw 42 rows"
    assert finding.risks == ("enriched:cheap",)


def test_build_plan_result_runs_custom_checks_when_baseline_missing_for_source() -> None:
    from tests.unit.extensions_demo import custom_check

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
        extensions=ExtensionsConfig(checks=(custom_check,)),
    )
    baseline = ProjectSnapshot(
        warehouse_kind="bigquery",
        generated_at=datetime(2026, 4, 22, 11, 0, tzinfo=timezone.utc),
        sources={},
    )
    current = ProjectSnapshot(
        warehouse_kind="bigquery",
        generated_at=datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc),
        sources={"stripe.transactions": make_source_snapshot(row_count=42)},
    )

    result = build_plan_result(config, baseline, current)

    assert any(finding.code == "baseline_missing_for_source" for finding in result.findings)
    assert any(finding.code == "custom_demo_check" for finding in result.findings)


def test_build_test_result_raises_extension_error_for_invalid_custom_check_return() -> None:
    from tests.unit.extensions_demo import invalid_custom_check

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
        extensions=ExtensionsConfig(checks=(invalid_custom_check,)),
    )
    current = ProjectSnapshot(
        warehouse_kind="bigquery",
        generated_at=datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc),
        sources={"stripe.transactions": make_source_snapshot(row_count=1)},
    )

    with pytest.raises(ExtensionError, match="returned a non-Finding value"):
        build_test_result(config, current)


def test_build_test_result_raises_extension_error_for_invalid_finding_enricher_return() -> None:
    from tests.unit.extensions_demo import invalid_finding_enricher

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
        extensions=ExtensionsConfig(finding_enrichers=(invalid_finding_enricher,)),
    )
    current = ProjectSnapshot(
        warehouse_kind="bigquery",
        generated_at=datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc),
        sources={
            "stripe.transactions": make_source_snapshot(
                null_rates={"amount": 0.1, "currency": 0.0}
            )
        },
    )

    with pytest.raises(ExtensionError, match="must return a Finding"):
        build_test_result(config, current)
