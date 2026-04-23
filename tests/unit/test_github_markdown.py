from __future__ import annotations

from verixa.diff.models import DiffResult, Finding
from verixa.output.github_markdown import render_diff_result_github_markdown


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
