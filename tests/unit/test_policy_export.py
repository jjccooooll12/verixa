from __future__ import annotations

import json

from verixa.diff.models import DiffResult, Finding
from verixa.policy.export import render_diff_result_policy_v1


def test_render_diff_result_policy_v1_emits_stable_document() -> None:
    result = DiffResult(
        findings=(
            Finding(
                source_name="stripe.transactions",
                severity="error",
                code="no_nulls_violation",
                message="no_nulls violated on amount",
                column="amount",
            ),
        ),
        sources_checked=1,
        used_baseline=False,
    )

    payload = json.loads(render_diff_result_policy_v1(result, "Validate"))

    assert payload["schema_version"] == "verixa.policy.v1"
    assert payload["run"]["command"] == "validate"
    assert payload["summary"]["errors"] == 1
    assert payload["summary"]["new_findings"] == 1
    assert payload["summary"]["resolved_findings"] == 0
    assert payload["summary"]["suppressed_findings"] == 0
    assert payload["findings"][0]["schema_version"] == "verixa.finding.v2"
    assert payload["findings"][0]["code"] == "no_nulls_violation"
    assert payload["findings"][0]["stable_code"] == "contract.no_nulls_violation"
    assert payload["findings"][0]["change_type"] == "contract_violation"
    assert payload["findings"][0]["remediation"]
