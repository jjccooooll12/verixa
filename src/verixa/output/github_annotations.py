"""GitHub annotation rendering for diff-like findings."""

from __future__ import annotations

import json
from typing import Any

from verixa.diff.models import DiffResult
from verixa.findings.schema import normalize_diff_result
from verixa.history.classifier import LifecycleReport, classify_finding_lifecycle


def render_diff_result_github_annotations(
    result: DiffResult,
    title: str,
    estimated_bytes_by_source: dict[str, int] | None = None,
    lifecycle_report: LifecycleReport | None = None,
) -> str:
    """Render GitHub-annotation-friendly JSON records for active findings."""

    normalized = normalize_diff_result(result, estimated_bytes_by_source=estimated_bytes_by_source)
    lifecycle = lifecycle_report or classify_finding_lifecycle(normalized, ())
    payload: list[dict[str, Any]] = []
    for finding in lifecycle.active_findings:
        payload.append(
            {
                "annotation_level": _annotation_level(finding.severity),
                "title": f"Verixa {title}: {finding.stable_code}",
                "message": _annotation_message(finding),
                "path": None,
                "start_line": None,
                "end_line": None,
                "source_name": finding.source_name,
                "fingerprint": finding.fingerprint,
                "lifecycle_status": finding.lifecycle_status,
                "change_type": finding.change_type,
            }
        )
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _annotation_level(severity: str) -> str:
    if severity == "error":
        return "failure"
    if severity == "warning":
        return "warning"
    return "notice"


def _annotation_message(finding) -> str:  # noqa: ANN001
    lines = [finding.message]
    lines.append(f"Code: {finding.stable_code}")
    lines.append(f"Status: {finding.lifecycle_status}")
    lines.append(f"Next step: {finding.remediation}")
    if finding.risks:
        lines.append(f"Why it matters: {'; '.join(finding.risks)}")
    return "\n".join(lines)
