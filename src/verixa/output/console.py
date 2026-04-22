"""Human-friendly terminal rendering."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from verixa.diff.models import DiffResult, Finding
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
            f"{len(source.schema)} columns"
        )
    if estimated_bytes_by_source is not None:
        lines.append(f"Estimated scan: {_format_total_bytes(estimated_bytes_by_source)}")
    return "\n".join(lines)


def render_diff_result(
    result: DiffResult,
    title: str,
    estimated_bytes_by_source: dict[str, int] | None = None,
) -> str:
    """Render grouped findings with a summary."""

    if not result.findings:
        lines = [f"No {title.lower()} findings across {result.sources_checked} source(s)."]
        if estimated_bytes_by_source is not None:
            lines.append(f"Estimated scan: {_format_total_bytes(estimated_bytes_by_source)}")
        return "\n".join(lines)

    grouped: dict[str, list[Finding]] = defaultdict(list)
    for finding in result.findings:
        grouped[finding.source_name].append(finding)

    lines: list[str] = []
    for source_name in sorted(grouped):
        lines.append(source_name)
        source_risks: list[str] = []
        for finding in grouped[source_name]:
            lines.append(f"- {finding.severity.upper()}: {finding.message}")
            source_risks.extend(finding.risks)
        unique_risks = list(dict.fromkeys(source_risks))
        if unique_risks:
            lines.append("Risk:")
            for risk in unique_risks:
                lines.append(f"- {risk}")
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
