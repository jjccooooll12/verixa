"""Human-friendly terminal rendering."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from verixa.diff.models import DiffResult
from verixa.findings.schema import NormalizedFinding, normalize_diff_result
from verixa.history.classifier import LifecycleReport, classify_finding_lifecycle
from verixa.snapshot.models import ProjectSnapshot


def render_snapshot_summary(
    snapshot: ProjectSnapshot,
    path: Path,
    estimated_bytes_by_source: dict[str, int] | None = None,
) -> str:
    """Render a concise snapshot capture summary."""

    lines = [
        f"Captured baseline snapshot for {len(snapshot.sources)} source(s).",
        f"Baseline written to {path}",
    ]
    for source_name, source in sorted(snapshot.sources.items()):
        lines.append(
            f"- {source_name}: {source.row_count if source.row_count is not None else 'unknown'} rows, "
            f"{len(source.schema)} columns, "
            f"{len(source.numeric_summaries)} numeric summaries"
        )
    if estimated_bytes_by_source is not None:
        lines.append(f"Estimated scan: {_format_total_bytes(estimated_bytes_by_source)}")
    return "\n".join(lines)


def render_diff_result(
    result: DiffResult,
    title: str,
    estimated_bytes_by_source: dict[str, int] | None = None,
    lifecycle_report: LifecycleReport | None = None,
) -> str:
    """Render grouped findings with a summary."""

    lifecycle = lifecycle_report or classify_finding_lifecycle(
        normalize_diff_result(result, estimated_bytes_by_source=estimated_bytes_by_source),
        (),
    )

    if not lifecycle.active_findings and not lifecycle.resolved_findings:
        lines = [f"No {title.lower()} findings across {result.sources_checked} source(s)."]
        if estimated_bytes_by_source is not None:
            lines.append(f"Estimated scan: {_format_total_bytes(estimated_bytes_by_source)}")
        return "\n".join(lines)

    lines: list[str] = []
    lines.extend(_render_grouped_findings(lifecycle.active_findings))
    if lifecycle.resolved_findings:
        if lines:
            lines.append("")
        lines.append("Resolved")
        lines.extend(_render_grouped_findings(lifecycle.resolved_findings, indent="  "))
        lines.append("")

    if lifecycle.suppressed_findings:
        if lines:
            lines.append("")
        lines.append("Suppressed")
        lines.extend(_render_grouped_findings(lifecycle.suppressed_findings, indent="  "))
        lines.append("")

    lines.append(
        f"Summary: {result.error_count} error(s), {result.warning_count} warning(s), "
        f"{result.sources_checked} source(s) checked"
    )
    if result.warning_policy_sources:
        lines.append(
            "Warning policy: warnings fail CI for "
            f"{', '.join(result.warning_policy_sources)}"
        )
    if estimated_bytes_by_source is not None:
        lines.append(f"Estimated scan: {_format_total_bytes(estimated_bytes_by_source)}")
    return "\n".join(lines).strip()


def _render_grouped_findings(
    findings: tuple[NormalizedFinding, ...],
    *,
    indent: str = "",
) -> list[str]:
    grouped: dict[str, dict[str, dict[str, list[NormalizedFinding]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )
    for finding in findings:
        grouped[finding.source_name][finding.severity][finding.change_type].append(finding)

    lines: list[str] = []
    for source_name in sorted(grouped):
        lines.append(f"{indent}{source_name}")
        for severity in ("error", "warning", "info"):
            change_groups = grouped[source_name].get(severity)
            if not change_groups:
                continue
            lines.append(f"{indent}- {severity.upper()}")
            for change_type in sorted(change_groups, key=_change_type_sort_key):
                lines.append(f"{indent}  - {_format_change_type(change_type)}")
                for finding in change_groups[change_type]:
                    label = finding.stable_code
                    if finding.column is not None:
                        label += f" {finding.column}"
                    lines.append(f"{indent}    - {label}: {finding.message}")
                    lines.append(
                        f"{indent}      status={finding.lifecycle_status} confidence={finding.confidence}"
                    )
                    if finding.source_criticality is not None:
                        lines.append(
                            f"{indent}      criticality={finding.source_criticality}"
                        )
                    if finding.owners:
                        lines.append(f"{indent}      owners={','.join(finding.owners)}")
                    lines.append(f"{indent}      next={finding.remediation}")
                    for risk in finding.risks:
                        lines.append(f"{indent}      risk={risk}")
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


def _format_total_bytes(estimated_bytes_by_source: dict[str, int]) -> str:
    total = sum(estimated_bytes_by_source.values())
    return f"{_format_bytes(total)} total"


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
