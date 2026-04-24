"""GitHub-friendly markdown rendering for diff-like findings."""

from __future__ import annotations

from collections import defaultdict

from verixa.diff.models import DiffResult
from verixa.findings.schema import NormalizedFinding, normalize_diff_result
from verixa.history.classifier import LifecycleReport, classify_finding_lifecycle
from verixa.runtime_impact import RuntimeImpact
from verixa.targeting import SourceSelectionReport


def render_diff_result_github_markdown(
    result: DiffResult,
    title: str,
    estimated_bytes_by_source: dict[str, int] | None = None,
    lifecycle_report: LifecycleReport | None = None,
    source_selection: SourceSelectionReport | None = None,
    runtime_impact: RuntimeImpact | None = None,
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
    if result.advisory_mode_enabled:
        lines.append("**Advisory mode:** project-level advisory is enabled; findings are non-blocking.")
    elif result.advisory_sources:
        lines.append(
            "**Advisory sources:** "
            + ", ".join(f"`{name}`" for name in result.advisory_sources)
        )
    if estimated_bytes_by_source is not None:
        lines.append(
            f"**Estimated scan:** {_format_bytes(sum(estimated_bytes_by_source.values()))}"
        )
    if source_selection is not None:
        lines.extend(_render_source_selection(source_selection))
    if runtime_impact is not None:
        lines.append(_render_runtime_impact(runtime_impact))
    downstream_impact = _collect_downstream_impact(lifecycle.active_findings)
    if downstream_impact:
        lines.append("**Potential downstream impact:**")
        for source_name, model_names in downstream_impact.items():
            lines.append(
                f"- `{source_name}` -> " + ", ".join(f"`{name}`" for name in model_names)
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
                if finding.confidence_reason is not None:
                    lines.append(f"  - Confidence note: {finding.confidence_reason}")
                if finding.history_metric is not None:
                    lines.append(
                        "  - Historical band: "
                        f"`{finding.history_metric}` median={_format_optional_number(finding.history_center_value)} "
                        f"expected={_format_optional_number(finding.history_lower_bound)}-"
                        f"{_format_optional_number(finding.history_upper_bound)} "
                        f"over {finding.history_sample_size} run(s)"
                    )
                if finding.source_criticality is not None:
                    lines.append(f"  - Source criticality: `{finding.source_criticality}`")
                if finding.owners:
                    lines.append(f"  - Owners: {', '.join(finding.owners)}")
                if finding.downstream_models:
                    lines.append(
                        "  - Downstream models: "
                        + ", ".join(f"`{name}`" for name in finding.downstream_models)
                    )
                lines.append(f"  - Next step: {finding.remediation}")
                if finding.risks:
                    lines.append(f"  - Why it matters: {'; '.join(finding.risks)}")
        lines.append("")
    if lines and lines[-1] == "":
        lines.pop()
    return lines


def _format_change_type(change_type: str) -> str:
    return change_type.replace("_", " ").title()


def _collect_downstream_impact(
    findings: tuple[NormalizedFinding, ...],
) -> dict[str, tuple[str, ...]]:
    grouped: dict[str, set[str]] = defaultdict(set)
    for finding in findings:
        for model_name in finding.downstream_models:
            grouped[finding.source_name].add(model_name)
    return {
        source_name: tuple(sorted(model_names))
        for source_name, model_names in sorted(grouped.items())
        if model_names
    }


def _change_type_sort_key(change_type: str) -> tuple[int, str]:
    order = {
        "contract_violation": 0,
        "baseline_drift": 1,
        "historical_drift": 2,
        "baseline_missing": 3,
        "baseline_stale": 4,
        "runtime_error": 5,
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


def _format_optional_number(value: float | None) -> str:
    if value is None:
        return "n/a"
    formatted = f"{value:.4f}"
    return formatted.rstrip("0").rstrip(".")


def _render_runtime_impact(runtime_impact: RuntimeImpact) -> str:
    if runtime_impact.mode == "estimated":
        return (
            "**Warehouse impact:** estimated "
            f"{_format_bytes(runtime_impact.estimated_total_bytes)} "
            f"across {len(runtime_impact.estimated_bytes_by_source)} source(s)"
        )
    return (
        "**Warehouse impact:** actual "
        f"{runtime_impact.actual_query_count} query(s), "
        f"{_format_bytes(runtime_impact.actual_total_bytes_scanned)} scanned, "
        f"{_format_bytes(runtime_impact.actual_total_bytes_written)} written, "
        f"{runtime_impact.actual_total_elapsed_ms}ms elapsed"
    )


def _render_source_selection(source_selection: SourceSelectionReport) -> list[str]:
    if source_selection.mode == "all_sources":
        return ["**Target selection:** all configured sources"]
    if source_selection.mode == "explicit_sources":
        return [
            "**Target selection:** explicit sources "
            + ", ".join(f"`{name}`" for name in source_selection.selected_sources)
        ]
    lines = [
        (
            "**Target selection:** "
            + (
                "fallback to all configured sources"
                if source_selection.mode == "fallback_all_sources"
                else f"targeted {len(source_selection.selected_sources)} source(s)"
            )
            + f" with `{source_selection.confidence}` confidence"
        )
    ]
    reasons_by_source = source_selection.reasons_by_source or {}
    for source_name in source_selection.selected_sources:
        reasons = reasons_by_source.get(source_name)
        if not reasons:
            continue
        rendered = ", ".join(
            f"`{reason.code}`" + (f" ({', '.join(reason.matched_files)})" if reason.matched_files else "")
            for reason in reasons
        )
        lines.append(f"- `{source_name}`: {rendered}")
    return lines
