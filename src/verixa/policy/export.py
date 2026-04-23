"""Stable policy-oriented exports for diff-like results."""

from __future__ import annotations

import json
from typing import Any

from verixa.diff.models import DiffResult
from verixa.findings.schema import normalize_diff_result
from verixa.history.classifier import LifecycleReport, classify_finding_lifecycle


def render_diff_result_policy_v1(
    result: DiffResult,
    title: str,
    estimated_bytes_by_source: dict[str, int] | None = None,
    lifecycle_report: LifecycleReport | None = None,
    environment: str | None = None,
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
        },
        "findings": [finding.as_dict() for finding in active_lifecycle.active_findings],
        "resolved_findings": [finding.as_dict() for finding in active_lifecycle.resolved_findings],
        "suppressed_findings": [finding.as_dict() for finding in active_lifecycle.suppressed_findings],
    }
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"
