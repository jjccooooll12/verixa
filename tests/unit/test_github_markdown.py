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
            ),
            Finding(
                source_name="stripe.transactions",
                severity="warning",
                code="row_count_changed",
                message="row count changed",
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
    assert "#### Baseline Drift" in output
    assert "`contract.accepted_values_violation` `currency`" in output
    assert "Status: `new`" in output
    assert "Next step:" in output
    assert "Why it matters: likely downstream finance breakage" in output
