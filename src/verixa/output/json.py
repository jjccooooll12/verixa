"""Machine-readable JSON rendering for CLI output."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from verixa.diff.models import DiffResult
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
) -> str:
    """Render a machine-readable diff result."""

    payload: dict[str, Any] = {
        "title": title,
        "summary": {
            "errors": result.error_count,
            "warnings": result.warning_count,
            "findings": len(result.findings),
            "has_errors": result.error_count > 0,
            "has_warnings": result.warning_count > 0,
            "warning_policy_failures": result.warning_policy_failure_count,
            "warning_policy_sources": list(result.warning_policy_sources),
            "sources_checked": result.sources_checked,
            "used_baseline": result.used_baseline,
            "estimated_bytes_processed": None
            if estimated_bytes_by_source is None
            else sum(estimated_bytes_by_source.values()),
        },
        "findings": [
            {
                "source_name": finding.source_name,
                "severity": finding.severity,
                "code": finding.code,
                "message": finding.message,
                "column": finding.column,
                "risks": list(finding.risks),
                "estimated_bytes_processed": None
                if estimated_bytes_by_source is None
                else estimated_bytes_by_source.get(finding.source_name),
            }
            for finding in result.findings
        ],
    }
    return _dumps(payload)


def render_error_json(message: str, exit_code: int) -> str:
    """Render a machine-readable runtime error."""

    return _dumps({"error": {"message": message, "exit_code": exit_code}})


def _dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"
