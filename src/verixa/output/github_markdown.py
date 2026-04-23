"""GitHub-friendly markdown rendering for diff-like findings."""

from __future__ import annotations

from collections import defaultdict

from verixa.diff.models import DiffResult
from verixa.findings.schema import NormalizedFinding, normalize_diff_result
from verixa.history.classifier import LifecycleReport, classify_finding_lifecycle


def render_diff_result_github_markdown(
    result: DiffResult,
    title: str,
    estimated_bytes_by_source: dict[str, int] | None = None,
    lifecycle_report: LifecycleReport | None = None,
) -> str:
    """Render grouped reviewer-friendly markdown for pull request summaries."""

    findings = normalize_diff_result(result, estimated_bytes_by_source=estimated_bytes_by_source)
    lifecycle = lifecycle_report or classify_finding_lifecycle(findings, ())

    lines = [f"# Verixa {title}", ""]
    lines.append(
        f"**Summary:** {result.error_count} error(s), {result.warning_count} warning(s), "
        f"{result.sources_checked} source(s) checked"
    )
    if result.warning_policy_sources:
        lines.append(
            "**Warning policy:** warnings fail CI for "
            f"{', '.join(result.warning_policy_sources)}"
        )
    if estimated_bytes_by_source is not None:
        lines.append(
            f"**Estimated scan:** {_format_bytes(sum(estimated_bytes_by_source.values()))}"
        )
    lines.append("")

    if not lifecycle.active_findings and not lifecycle.resolved_findings:
        lines.append("No findings.")
        return "\n".join(lines).strip() + "\n"

    for severity in ("error", "warning", "info"):
        severity_findings = [finding for finding in lifecycle.active_findings if finding.severity == severity]
        if not severity_findings:
            continue
        lines.append(f"## {severity.title()}s")
        lines.append("")
        lines.extend(_render_grouped_findings(severity_findings))
        lines.append("")

    if lifecycle.resolved_findings:
        lines.append("## Resolved")
        lines.append("")
        lines.extend(_render_grouped_findings(list(lifecycle.resolved_findings)))
        lines.append("")

    if lifecycle.suppressed_findings:
        lines.append("## Suppressed")
        lines.append("")
        lines.extend(_render_grouped_findings(list(lifecycle.suppressed_findings)))
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def _render_grouped_findings(findings: list[NormalizedFinding]) -> list[str]:
    grouped: dict[str, dict[str, list[NormalizedFinding]]] = defaultdict(lambda: defaultdict(list))
    for finding in findings:
        grouped[finding.source_name][finding.change_type].append(finding)

    lines: list[str] = []
    for source_name in sorted(grouped):
        lines.append(f"### `{source_name}`")
        for change_type in sorted(grouped[source_name], key=_change_type_sort_key):
            lines.append(f"#### {_format_change_type(change_type)}")
            for finding in grouped[source_name][change_type]:
                prefix = f"`{finding.stable_code}`"
                if finding.column is not None:
                    prefix += f" `{finding.column}`"
                lines.append(f"- {prefix}: {finding.message}")
                lines.append(f"  - Status: `{finding.lifecycle_status}`")
                lines.append(f"  - Change type: `{finding.change_type}`")
                lines.append(f"  - Confidence: `{finding.confidence}`")
                if finding.source_criticality is not None:
                    lines.append(f"  - Source criticality: `{finding.source_criticality}`")
                if finding.owners:
                    lines.append(f"  - Owners: {', '.join(finding.owners)}")
                lines.append(f"  - Next step: {finding.remediation}")
                if finding.risks:
                    lines.append(f"  - Why it matters: {'; '.join(finding.risks)}")
        lines.append("")
    if lines and lines[-1] == "":
        lines.pop()
    return lines


def _format_change_type(change_type: str) -> str:
    return change_type.replace("_", " ").title()


def _change_type_sort_key(change_type: str) -> tuple[int, str]:
    order = {
        "contract_violation": 0,
        "baseline_drift": 1,
        "baseline_missing": 2,
        "baseline_stale": 3,
        "runtime_error": 4,
    }
    return order.get(change_type, 99), change_type


def _format_bytes(value: int) -> str:
    units = ("B", "KB", "MB", "GB", "TB")
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{int(value)} B"
