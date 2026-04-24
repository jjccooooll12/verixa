from __future__ import annotations

from datetime import datetime, timezone

from verixa.diff.models import DiffResult, Finding
from verixa.output.github_markdown import render_diff_result_github_markdown
from verixa.runtime_impact import RuntimeImpact, RuntimeImpactRecord
from verixa.targeting import SourceSelectionReason, SourceSelectionReport


def test_render_diff_result_github_markdown_groups_findings() -> None:
    result = DiffResult(
        findings=(
            Finding(
                source_name="stripe.transactions",
                severity="error",
                code="accepted_values_violation",
                message="accepted values violated on currency",
                column="currency",
                risks=("likely downstream finance breakage",),
                downstream_models=("stg_orders", "fct_orders"),
                confidence_reason="bounded mode evaluated a bounded window of 30d on created_at",
            ),
            Finding(
                source_name="stripe.transactions",
                severity="warning",
                code="row_count_history_band",
                message="row count outside historical band",
                history_metric="row_count",
                history_sample_size=5,
                history_center_value=100.0,
                history_lower_bound=80.0,
                history_upper_bound=120.0,
            ),
        ),
        sources_checked=1,
        used_baseline=True,
    )

    output = render_diff_result_github_markdown(result, "Check")

    assert "# Verixa Check" in output
    assert "## Errors" in output
    assert "## Warnings" in output
    assert "### `stripe.transactions`" in output
    assert "#### Contract Violation" in output
    assert "#### Historical Drift" in output
    assert "**Potential downstream impact:**" in output
    assert "- `stripe.transactions` -> `fct_orders`, `stg_orders`" in output
    assert "`contract.accepted_values_violation` `currency`" in output
    assert "Status: `new`" in output
    assert "Confidence note: bounded mode evaluated a bounded window of 30d on created_at" in output
    assert "Downstream models: `stg_orders`, `fct_orders`" in output
    assert "Historical band: `row_count` median=100 expected=80-120 over 5 run(s)" in output
    assert "Next step:" in output
    assert "Why it matters: likely downstream finance breakage" in output


def test_render_diff_result_github_markdown_includes_runtime_impact() -> None:
    result = DiffResult(findings=(), sources_checked=1, used_baseline=True)

    output = render_diff_result_github_markdown(
        result,
        "Diff",
        runtime_impact=RuntimeImpact(
            warehouse_kind="snowflake",
            mode="actual",
            execution_mode="bounded",
            query_tag="verixa:diff",
            actual_records=(
                RuntimeImpactRecord(
                    query_id="abc123",
                    query_tag="verixa:diff",
                    warehouse_name="VERIXA_WH",
                    start_time=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
                    total_elapsed_ms=250,
                    bytes_scanned=2048,
                    bytes_written=512,
                    rows_produced=1,
                ),
            ),
        ),
    )

    assert "**Warehouse impact:** actual 1 query(s), 2.0 KB scanned, 512 B written, 250ms elapsed" in output


def test_render_diff_result_github_markdown_includes_source_selection() -> None:
    result = DiffResult(findings=(), sources_checked=1, used_baseline=True)

    output = render_diff_result_github_markdown(
        result,
        "Diff",
        source_selection=SourceSelectionReport(
            mode="targeted_sources",
            confidence="medium",
            runner_source_names=("stripe.transactions",),
            selected_sources=("stripe.transactions",),
            changed_files=("models/staging/schema.yml",),
            reasons_by_source={
                "stripe.transactions": (
                    SourceSelectionReason(
                        code="matched_dbt_model_dependency",
                        confidence="high",
                        matched_files=("models/staging/schema.yml",),
                    ),
                )
            },
        ),
    )

    assert "**Target selection:** targeted 1 source(s) with `medium` confidence" in output
    assert "- `stripe.transactions`: `matched_dbt_model_dependency` (models/staging/schema.yml)" in output
