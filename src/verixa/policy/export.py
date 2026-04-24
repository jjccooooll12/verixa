"""Stable policy-oriented exports for diff-like results."""

from __future__ import annotations

import json
from typing import Any

from verixa.diff.models import DiffResult
from verixa.findings.schema import normalize_diff_result
from verixa.history.classifier import LifecycleReport, classify_finding_lifecycle
from verixa.runtime_impact import RuntimeImpact
from verixa.targeting import SourceSelectionReport


def render_diff_result_policy_v1(
    result: DiffResult,
    title: str,
    estimated_bytes_by_source: dict[str, int] | None = None,
    lifecycle_report: LifecycleReport | None = None,
    environment: str | None = None,
    source_selection: SourceSelectionReport | None = None,
    runtime_impact: RuntimeImpact | None = None,
) -> str:
    """Render a stable policy input document for external evaluators."""

    normalized_findings = normalize_diff_result(result, estimated_bytes_by_source=estimated_bytes_by_source)
    active_lifecycle = lifecycle_report or classify_finding_lifecycle(normalized_findings, ())
    payload: dict[str, Any] = {
        "schema_version": "verixa.policy.v1",
        "run": {
            "command": title.lower(),
            "title": title,
            "environment": environment,
            "sources_checked": result.sources_checked,
            "used_baseline": result.used_baseline,
            "execution_mode": result.execution_mode,
            "estimated_bytes_processed": None
            if estimated_bytes_by_source is None
            else sum(estimated_bytes_by_source.values()),
        },
        "summary": {
            "errors": result.error_count,
            "warnings": result.warning_count,
            "findings": len(result.findings),
            "new_findings": sum(1 for finding in active_lifecycle.active_findings if finding.lifecycle_status == "new"),
            "recurring_findings": sum(1 for finding in active_lifecycle.active_findings if finding.lifecycle_status == "recurring"),
            "resolved_findings": len(active_lifecycle.resolved_findings),
            "suppressed_findings": len(active_lifecycle.suppressed_findings),
            "warning_policy_failures": result.warning_policy_failure_count,
            "warning_policy_sources": list(result.warning_policy_sources),
            "advisory_mode": result.advisory_mode_enabled,
            "advisory_sources": list(result.advisory_sources),
        },
        "findings": [finding.as_dict() for finding in active_lifecycle.active_findings],
        "resolved_findings": [finding.as_dict() for finding in active_lifecycle.resolved_findings],
        "suppressed_findings": [finding.as_dict() for finding in active_lifecycle.suppressed_findings],
    }
    if source_selection is not None:
        payload["source_selection"] = _source_selection_payload(source_selection)
    if runtime_impact is not None:
        payload["warehouse_impact"] = _runtime_impact_payload(runtime_impact)
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _runtime_impact_payload(runtime_impact: RuntimeImpact) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "warehouse_kind": runtime_impact.warehouse_kind,
        "mode": runtime_impact.mode,
        "execution_mode": runtime_impact.execution_mode,
        "query_tag": runtime_impact.query_tag,
    }
    if runtime_impact.mode == "estimated":
        payload["summary"] = {
            "estimated_bytes_processed": runtime_impact.estimated_total_bytes,
            "sources_estimated": len(runtime_impact.estimated_bytes_by_source),
        }
        payload["sources"] = [
            {
                "source_name": source_name,
                "estimated_bytes_processed": bytes_processed,
            }
            for source_name, bytes_processed in sorted(runtime_impact.estimated_bytes_by_source.items())
        ]
        return payload

    payload["summary"] = {
        "queries_found": runtime_impact.actual_query_count,
        "total_bytes_scanned": runtime_impact.actual_total_bytes_scanned,
        "total_bytes_written": runtime_impact.actual_total_bytes_written,
        "total_elapsed_ms": runtime_impact.actual_total_elapsed_ms,
    }
    return payload


def _source_selection_payload(source_selection: SourceSelectionReport) -> dict[str, Any]:
    return {
        "mode": source_selection.mode,
        "confidence": source_selection.confidence,
        "changed_files": list(source_selection.changed_files),
        "targets_path": None if source_selection.targets_path is None else str(source_selection.targets_path),
        "selected_sources": list(source_selection.selected_sources),
        "runner_source_names": list(source_selection.runner_source_names),
        "reasons_by_source": {
            source_name: [
                {
                    "code": reason.code,
                    "confidence": reason.confidence,
                    "matched_files": list(reason.matched_files),
                    "detail": reason.detail,
                }
                for reason in reasons
            ]
            for source_name, reasons in sorted((source_selection.reasons_by_source or {}).items())
        },
    }
