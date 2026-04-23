"""Stable normalized finding schema for automation-friendly outputs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
from typing import Literal

from verixa.diff.models import DiffResult, Finding

FindingCategory = Literal[
    "schema",
    "contract",
    "freshness",
    "null_rate",
    "row_count",
    "numeric_distribution",
    "baseline",
    "suppression",
    "runtime",
]

FindingChangeType = Literal[
    "contract_violation",
    "baseline_drift",
    "historical_drift",
    "baseline_missing",
    "baseline_stale",
    "runtime_error",
]

BaselineStatus = Literal[
    "available",
    "missing",
    "missing_for_environment",
    "missing_for_source",
    "stale",
    "not_used",
]
Confidence = Literal["high", "medium", "low"]
LifecycleStatus = Literal["new", "recurring", "resolved", "suppressed"]


@dataclass(frozen=True, slots=True)
class NormalizedFinding:
    """Stable finding record used by policy and PR-oriented outputs."""

    schema_version: str
    fingerprint: str
    source_name: str
    severity: str
    code: str
    stable_code: str
    message: str
    category: FindingCategory
    change_type: FindingChangeType
    baseline_status: BaselineStatus
    confidence: Confidence
    lifecycle_status: LifecycleStatus
    remediation: str
    confidence_reason: str | None = None
    column: str | None = None
    risks: tuple[str, ...] = ()
    owners: tuple[str, ...] = ()
    source_criticality: Literal["low", "medium", "high"] | None = None
    downstream_models: tuple[str, ...] = ()
    estimated_bytes_processed: int | None = None
    history_metric: str | None = None
    history_window: int | None = None
    history_sample_size: int | None = None
    history_center_value: float | None = None
    history_lower_bound: float | None = None
    history_upper_bound: float | None = None

    def as_dict(self) -> dict[str, object]:
        """Convert the normalized finding to a JSON-friendly mapping."""

        return asdict(self)


def normalize_diff_result(
    result: DiffResult,
    *,
    estimated_bytes_by_source: dict[str, int] | None = None,
) -> tuple[NormalizedFinding, ...]:
    """Normalize raw findings into a stable schema for external consumers."""

    return tuple(
        _normalize_finding(
            finding,
            used_baseline=result.used_baseline,
            estimated_bytes_by_source=estimated_bytes_by_source,
        )
        for finding in result.findings
    )


def _normalize_finding(
    finding: Finding,
    *,
    used_baseline: bool,
    estimated_bytes_by_source: dict[str, int] | None,
) -> NormalizedFinding:
    category = _category_for_code(finding.code)
    change_type = _change_type_for_code(finding.code)
    baseline_status = _baseline_status_for_code(finding.code, used_baseline=used_baseline)
    confidence = finding.confidence_override or _confidence_for_code(finding.code)
    remediation = _remediation_for_code(finding.code)
    estimated_bytes = None
    if estimated_bytes_by_source is not None:
        estimated_bytes = estimated_bytes_by_source.get(finding.source_name)

    return NormalizedFinding(
        schema_version="verixa.finding.v2",
        fingerprint=_fingerprint_for_finding(finding, change_type=change_type),
        source_name=finding.source_name,
        severity=finding.severity,
        code=finding.code,
        stable_code=_stable_code_for_internal(finding.code),
        message=finding.message,
        category=category,
        change_type=change_type,
        baseline_status=baseline_status,
        confidence=confidence,
        confidence_reason=finding.confidence_reason,
        lifecycle_status="new",
        remediation=remediation,
        column=finding.column,
        risks=finding.risks,
        owners=finding.owners,
        source_criticality=finding.source_criticality,
        downstream_models=finding.downstream_models,
        estimated_bytes_processed=estimated_bytes,
        history_metric=finding.history_metric,
        history_window=finding.history_window,
        history_sample_size=finding.history_sample_size,
        history_center_value=finding.history_center_value,
        history_lower_bound=finding.history_lower_bound,
        history_upper_bound=finding.history_upper_bound,
    )


def _fingerprint_for_finding(finding: Finding, *, change_type: str) -> str:
    raw = "|".join(
        (
            finding.source_name,
            finding.code,
            finding.column or "",
            change_type,
        )
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _category_for_code(code: str) -> FindingCategory:
    if code.startswith("schema_"):
        return "schema"
    if code in {"no_nulls_violation", "accepted_values_violation"}:
        return "contract"
    if code.startswith("freshness_"):
        return "freshness"
    if code == "null_rate_changed":
        return "null_rate"
    if code == "null_rate_history_band":
        return "null_rate"
    if code == "row_count_changed":
        return "row_count"
    if code == "row_count_history_band":
        return "row_count"
    if code in {"numeric_p50_history_band", "numeric_p95_history_band"}:
        return "numeric_distribution"
    if code.startswith("numeric_"):
        return "numeric_distribution"
    if code.startswith("baseline_"):
        return "baseline"
    if code.startswith("suppression_"):
        return "suppression"
    return "runtime"


def _change_type_for_code(code: str) -> FindingChangeType:
    if code in {
        "schema_column_missing",
        "schema_type_changed",
        "schema_column_added",
        "no_nulls_violation",
        "accepted_values_violation",
        "freshness_missing",
        "freshness_violated",
    }:
        return "contract_violation"
    if code in {"null_rate_changed", "row_count_changed", "numeric_p50_changed", "numeric_p95_changed"}:
        return "baseline_drift"
    if code in {
        "null_rate_history_band",
        "row_count_history_band",
        "numeric_p50_history_band",
        "numeric_p95_history_band",
    }:
        return "historical_drift"
    if code in {
        "baseline_missing",
        "baseline_missing_for_environment",
        "baseline_missing_for_source",
        "baseline_path_invalid",
        "baseline_unreadable",
    }:
        return "baseline_missing"
    if code == "baseline_stale":
        return "baseline_stale"
    return "runtime_error"


def _baseline_status_for_code(code: str, *, used_baseline: bool) -> BaselineStatus:
    if code == "baseline_missing_for_source":
        return "missing_for_source"
    if code == "baseline_missing_for_environment":
        return "missing_for_environment"
    if code in {"baseline_missing", "baseline_path_invalid", "baseline_unreadable"}:
        return "missing"
    if code == "baseline_stale":
        return "stale"
    if used_baseline:
        return "available"
    return "not_used"


def _confidence_for_code(code: str) -> Confidence:
    if code in {
        "schema_column_missing",
        "schema_type_changed",
        "schema_column_added",
        "no_nulls_violation",
        "accepted_values_violation",
        "freshness_missing",
        "freshness_violated",
        "null_rate_changed",
        "numeric_p50_changed",
        "numeric_p95_changed",
    }:
        return "high"
    if code in {
        "null_rate_history_band",
        "row_count_history_band",
        "numeric_p50_history_band",
        "numeric_p95_history_band",
    }:
        return "medium"
    if code in {
        "row_count_changed",
        "baseline_missing",
        "baseline_missing_for_environment",
        "baseline_missing_for_source",
        "baseline_stale",
    }:
        return "medium"
    return "medium"


def _remediation_for_code(code: str) -> str:
    if code.startswith("schema_"):
        return "Update the contract or fix the upstream schema change."
    if code in {"no_nulls_violation", "accepted_values_violation", "freshness_missing", "freshness_violated"}:
        return "Fix the upstream data or relax the contract if the change is expected."
    if code in {"null_rate_changed", "row_count_changed", "numeric_p50_changed", "numeric_p95_changed"}:
        return "Investigate the upstream change or refresh the baseline if the drift is expected."
    if code in {
        "null_rate_history_band",
        "row_count_history_band",
        "numeric_p50_history_band",
        "numeric_p95_history_band",
    }:
        return "Investigate the recent trend or enable backfill mode if this burst is expected."
    if code in {
        "baseline_missing",
        "baseline_missing_for_environment",
        "baseline_missing_for_source",
        "baseline_path_invalid",
        "baseline_unreadable",
    }:
        return "Capture or promote the correct baseline before relying on drift results."
    if code == "baseline_stale":
        return "Refresh and promote the baseline for this environment."
    return "Fix warehouse access or configuration before retrying."


def _stable_code_for_internal(code: str) -> str:
    mapping = {
        "accepted_values_violation": "contract.accepted_values_violation",
        "no_nulls_violation": "contract.no_nulls_violation",
        "freshness_missing": "freshness.missing",
        "freshness_violated": "freshness.sla_violated",
        "numeric_p50_changed": "drift.numeric_p50_changed",
        "numeric_p95_changed": "drift.numeric_p95_changed",
        "schema_column_missing": "schema.column_removed",
        "schema_type_changed": "schema.column_type_changed",
        "schema_column_added": "schema.column_added",
        "row_count_changed": "drift.row_count_changed",
        "null_rate_changed": "drift.null_rate_changed",
        "row_count_history_band": "drift.row_count_history_band",
        "null_rate_history_band": "drift.null_rate_history_band",
        "numeric_p50_history_band": "drift.numeric_p50_history_band",
        "numeric_p95_history_band": "drift.numeric_p95_history_band",
        "baseline_missing_for_source": "baseline.missing_for_source",
        "baseline_missing_for_environment": "baseline.missing_for_environment",
        "baseline_missing": "baseline.missing",
        "baseline_path_invalid": "baseline.path_invalid",
        "baseline_unreadable": "baseline.unreadable",
        "baseline_stale": "baseline.stale",
        "suppression_expired": "suppression.expired",
        "config_missing": "runtime.config_missing",
        "config_invalid": "runtime.config_invalid",
        "auth_check_failed": "runtime.auth_check_failed",
        "auth_unusable": "runtime.auth_unusable",
        "source_unreachable": "runtime.source_unreachable",
        "snowflake_runtime_unavailable": "runtime.snowflake_runtime_unavailable",
        "snowflake_warehouse_missing": "runtime.snowflake_warehouse_missing",
        "snowflake_warehouse_mismatch": "runtime.snowflake_warehouse_mismatch",
        "snowflake_warehouse_unusable": "runtime.snowflake_warehouse_unusable",
        "snowflake_role_mismatch": "runtime.snowflake_role_mismatch",
        "snowflake_database_mismatch": "runtime.snowflake_database_mismatch",
        "snowflake_schema_mismatch": "runtime.snowflake_schema_mismatch",
    }
    return mapping.get(code, f"runtime.{code}")


def stable_code_for_internal(code: str) -> str:
    """Expose stable dotted finding codes to other layers."""

    return _stable_code_for_internal(code)
