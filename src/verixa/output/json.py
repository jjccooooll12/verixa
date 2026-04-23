"""Machine-readable JSON rendering for CLI output."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from verixa.diff.models import DiffResult
from verixa.findings.schema import normalize_diff_result
from verixa.history.classifier import LifecycleReport, classify_finding_lifecycle
from verixa.snapshot.models import ProjectSnapshot


def render_snapshot_summary_json(
    snapshot: ProjectSnapshot,
    path: Path,
    estimated_bytes_by_source: dict[str, int] | None = None,
) -> str:
    """Render a machine-readable snapshot capture summary."""

    payload = {
        "summary": {
            "sources_captured": len(snapshot.sources),
            "baseline_path": str(path),
            "generated_at": snapshot.generated_at.isoformat(),
            "estimated_bytes_processed": None
            if estimated_bytes_by_source is None
            else sum(estimated_bytes_by_source.values()),
        },
        "sources": [
            {
                "source_name": source_name,
                "table": source.table,
                "row_count": source.row_count,
                "column_count": len(source.schema),
                "numeric_summary_columns": len(source.numeric_summaries),
                "numeric_summaries": {
                    column_name: {
                        "min_value": summary.min_value,
                        "p50_value": summary.p50_value,
                        "p95_value": summary.p95_value,
                        "max_value": summary.max_value,
                        "mean_value": summary.mean_value,
                    }
                    for column_name, summary in sorted(source.numeric_summaries.items())
                },
                "captured_at": source.captured_at.isoformat(),
                "estimated_bytes_processed": None
                if estimated_bytes_by_source is None
                else estimated_bytes_by_source.get(source_name),
            }
            for source_name, source in sorted(snapshot.sources.items())
        ],
    }
    return _dumps(payload)


def render_diff_result_json(
    result: DiffResult,
    title: str,
    estimated_bytes_by_source: dict[str, int] | None = None,
    lifecycle_report: LifecycleReport | None = None,
    environment: str | None = None,
) -> str:
    """Render a machine-readable diff result."""

    normalized = normalize_diff_result(result, estimated_bytes_by_source=estimated_bytes_by_source)
    lifecycle = lifecycle_report or classify_finding_lifecycle(normalized, ())
    payload: dict[str, Any] = {
        "title": title,
        "summary": {
            "errors": result.error_count,
            "warnings": result.warning_count,
            "findings": len(result.findings),
            "has_errors": result.error_count > 0,
            "has_warnings": result.warning_count > 0,
            "new_findings": sum(
                1 for finding in lifecycle.active_findings if finding.lifecycle_status == "new"
            ),
            "recurring_findings": sum(
                1 for finding in lifecycle.active_findings if finding.lifecycle_status == "recurring"
            ),
            "resolved_findings": len(lifecycle.resolved_findings),
            "suppressed_findings": len(lifecycle.suppressed_findings),
            "warning_policy_failures": result.warning_policy_failure_count,
            "warning_policy_sources": list(result.warning_policy_sources),
            "advisory_mode": result.advisory_mode_enabled,
            "advisory_sources": list(result.advisory_sources),
            "execution_mode": result.execution_mode,
            "sources_checked": result.sources_checked,
            "used_baseline": result.used_baseline,
            "estimated_bytes_processed": None
            if estimated_bytes_by_source is None
            else sum(estimated_bytes_by_source.values()),
        },
        "environment": environment,
        "findings": [
            finding.as_dict() for finding in lifecycle.active_findings
        ],
        "resolved_findings": [finding.as_dict() for finding in lifecycle.resolved_findings],
        "suppressed_findings": [finding.as_dict() for finding in lifecycle.suppressed_findings],
    }
    return _dumps(payload)


def render_error_json(message: str, exit_code: int) -> str:
    """Render a machine-readable runtime error."""

    return _dumps({"error": {"message": message, "exit_code": exit_code}})


def _dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"
