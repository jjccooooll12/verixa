"""Apply suppression rules to normalized findings and raw diff results."""

from __future__ import annotations

from dataclasses import dataclass, replace

from verixa.diff.models import DiffResult, Finding
from verixa.history.classifier import LifecycleReport, classify_finding_lifecycle
from verixa.findings.schema import NormalizedFinding, normalize_diff_result
from verixa.suppressions.loader import split_active_and_expired
from verixa.suppressions.models import SuppressionRule


@dataclass(frozen=True, slots=True)
class SuppressionOutcome:
    """Filtered diff result plus suppression metadata."""

    result: DiffResult
    lifecycle_report: LifecycleReport
    suppressed_findings: tuple[NormalizedFinding, ...]
    expired_rules: tuple[SuppressionRule, ...]


def apply_suppressions(
    result: DiffResult,
    *,
    environment: str | None,
    rules: tuple[SuppressionRule, ...],
    lifecycle_report: LifecycleReport | None = None,
    estimated_bytes_by_source: dict[str, int] | None = None,
) -> SuppressionOutcome:
    """Filter suppressed findings out of a diff result while keeping audit metadata."""

    lifecycle = lifecycle_report or classify_finding_lifecycle(
        normalize_diff_result(result, estimated_bytes_by_source=estimated_bytes_by_source),
        (),
    )
    active_rules, expired_rules = split_active_and_expired(rules)

    raw_findings: list[Finding] = []
    active_findings: list[NormalizedFinding] = []
    suppressed_findings: list[NormalizedFinding] = []

    for raw_finding, normalized in zip(result.findings, lifecycle.active_findings, strict=True):
        if _matching_rule(normalized, active_rules, environment=environment) is None:
            raw_findings.append(raw_finding)
            active_findings.append(normalized)
            continue
        suppressed_findings.append(replace(normalized, lifecycle_status="suppressed"))

    visible_sources = {finding.source_name for finding in raw_findings}
    warning_policy_sources = tuple(
        source_name
        for source_name in result.warning_policy_sources
        if source_name in visible_sources
    )
    filtered_result = DiffResult(
        findings=tuple(raw_findings),
        sources_checked=result.sources_checked,
        used_baseline=result.used_baseline,
        warning_policy_sources=warning_policy_sources,
    )
    filtered_lifecycle = LifecycleReport(
        active_findings=tuple(active_findings),
        resolved_findings=lifecycle.resolved_findings,
        suppressed_findings=tuple(suppressed_findings),
    )
    return SuppressionOutcome(
        result=filtered_result,
        lifecycle_report=filtered_lifecycle,
        suppressed_findings=tuple(suppressed_findings),
        expired_rules=expired_rules,
    )


def _matching_rule(
    finding: NormalizedFinding,
    rules: tuple[SuppressionRule, ...],
    *,
    environment: str | None,
) -> SuppressionRule | None:
    for rule in rules:
        if rule.fingerprint != finding.fingerprint:
            continue
        if rule.applies_globally:
            return rule
        if environment is not None and environment in rule.environments:
            return rule
    return None
