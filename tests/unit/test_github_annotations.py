from __future__ import annotations

import json

from verixa.diff.models import DiffResult, Finding
from verixa.output.github_annotations import render_diff_result_github_annotations


def test_render_diff_result_github_annotations_emits_active_findings() -> None:
    result = DiffResult(
        findings=(
            Finding(
                source_name="stripe.transactions",
                severity="error",
                code="accepted_values_violation",
                message="accepted values violated on currency",
                column="currency",
            ),
        ),
        sources_checked=1,
        used_baseline=True,
    )

    payload = json.loads(render_diff_result_github_annotations(result, "Diff"))

    assert payload[0]["annotation_level"] == "failure"
    assert payload[0]["title"] == "Verixa Diff: contract.accepted_values_violation"
    assert payload[0]["source_name"] == "stripe.transactions"
    assert payload[0]["lifecycle_status"] == "new"
