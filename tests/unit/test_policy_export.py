from __future__ import annotations

import json

from verixa.diff.models import DiffResult, Finding
from verixa.policy.export import render_diff_result_policy_v1
from verixa.runtime_impact import RuntimeImpact
from verixa.targeting import SourceSelectionReason, SourceSelectionReport


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
        execution_mode="cheap",
    )

    payload = json.loads(render_diff_result_policy_v1(result, "Validate"))

    assert payload["schema_version"] == "verixa.policy.v1"
    assert payload["run"]["command"] == "validate"
    assert payload["run"]["execution_mode"] == "cheap"
    assert payload["summary"]["errors"] == 1
    assert payload["summary"]["new_findings"] == 1
    assert payload["summary"]["resolved_findings"] == 0
    assert payload["summary"]["suppressed_findings"] == 0
    assert payload["findings"][0]["schema_version"] == "verixa.finding.v2"
    assert payload["findings"][0]["code"] == "no_nulls_violation"
    assert payload["findings"][0]["stable_code"] == "contract.no_nulls_violation"
    assert payload["findings"][0]["change_type"] == "contract_violation"
    assert payload["findings"][0]["remediation"]


def test_render_diff_result_policy_v1_includes_runtime_impact() -> None:
    result = DiffResult(findings=(), sources_checked=1, used_baseline=True, execution_mode="bounded")

    payload = json.loads(
        render_diff_result_policy_v1(
            result,
            "Diff",
            runtime_impact=RuntimeImpact(
                warehouse_kind="bigquery",
                mode="estimated",
                execution_mode="bounded",
                estimated_bytes_by_source={"stripe.transactions": 2048},
            ),
        )
    )

    assert payload["warehouse_impact"]["warehouse_kind"] == "bigquery"
    assert payload["warehouse_impact"]["mode"] == "estimated"
    assert payload["warehouse_impact"]["summary"]["estimated_bytes_processed"] == 2048


def test_render_diff_result_policy_v1_includes_source_selection() -> None:
    result = DiffResult(findings=(), sources_checked=1, used_baseline=True, execution_mode="bounded")

    payload = json.loads(
        render_diff_result_policy_v1(
            result,
            "Diff",
            source_selection=SourceSelectionReport(
                mode="targeted_sources",
                confidence="high",
                runner_source_names=("stripe.transactions",),
                selected_sources=("stripe.transactions",),
                changed_files=("models/staging/orders.sql",),
                reasons_by_source={
                    "stripe.transactions": (
                        SourceSelectionReason(
                            code="matched_path_rule",
                            confidence="high",
                            matched_files=("models/staging/orders.sql",),
                            detail="path rule models/staging/**",
                        ),
                    )
                },
            ),
        )
    )

    assert payload["source_selection"]["mode"] == "targeted_sources"
    assert payload["source_selection"]["changed_files"] == ["models/staging/orders.sql"]
    assert payload["source_selection"]["reasons_by_source"]["stripe.transactions"][0]["code"] == "matched_path_rule"
