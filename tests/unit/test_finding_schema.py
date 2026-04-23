from __future__ import annotations

from verixa.diff.models import DiffResult, Finding
from verixa.findings.schema import normalize_diff_result


def test_normalize_diff_result_maps_contract_and_baseline_fields() -> None:
    result = DiffResult(
        findings=(
            Finding(
                source_name="stripe.transactions",
                severity="error",
                code="no_nulls_violation",
                message="no_nulls violated on amount",
                column="amount",
                risks=("likely dashboard undercount",),
            ),
            Finding(
                source_name="stripe.transactions",
                severity="warning",
                code="row_count_changed",
                message="row count changed",
            ),
            Finding(
                source_name="stripe.transactions",
                severity="warning",
                code="baseline_missing_for_source",
                message="baseline missing",
            ),
        ),
        sources_checked=1,
        used_baseline=True,
    )

    normalized = normalize_diff_result(
        result,
        estimated_bytes_by_source={"stripe.transactions": 2048},
    )

    assert normalized[0].schema_version == "verixa.finding.v2"
    assert normalized[0].category == "contract"
    assert normalized[0].change_type == "contract_violation"
    assert normalized[0].baseline_status == "available"
    assert normalized[0].confidence == "high"
    assert normalized[0].estimated_bytes_processed == 2048
    assert normalized[0].risks == ("likely dashboard undercount",)

    assert normalized[1].category == "row_count"
    assert normalized[1].change_type == "baseline_drift"
    assert normalized[1].confidence == "medium"

    assert normalized[2].category == "baseline"
    assert normalized[2].change_type == "baseline_missing"
    assert normalized[2].baseline_status == "missing_for_source"


def test_normalize_diff_result_fingerprint_is_stable() -> None:
    finding = Finding(
        source_name="stripe.transactions",
        severity="error",
        code="accepted_values_violation",
        message="accepted values violated",
        column="currency",
    )
    result = DiffResult(findings=(finding,), sources_checked=1, used_baseline=False)

    first = normalize_diff_result(result)[0].fingerprint
    second = normalize_diff_result(result)[0].fingerprint

    assert first == second
    assert len(first) == 64


def test_normalize_diff_result_maps_environment_missing_baseline() -> None:
    result = DiffResult(
        findings=(
            Finding(
                source_name="baseline",
                severity="error",
                code="baseline_missing_for_environment",
                message="baseline missing for environment",
            ),
        ),
        sources_checked=1,
        used_baseline=False,
    )

    normalized = normalize_diff_result(result)

    assert normalized[0].baseline_status == "missing_for_environment"
    assert normalized[0].stable_code == "baseline.missing_for_environment"
