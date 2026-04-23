"""Lifecycle classification for normalized findings."""

from __future__ import annotations

from dataclasses import dataclass, replace

from verixa.findings.schema import NormalizedFinding


@dataclass(frozen=True, slots=True)
class LifecycleReport:
    """Current and resolved findings after comparing with the previous run."""

    active_findings: tuple[NormalizedFinding, ...]
    resolved_findings: tuple[NormalizedFinding, ...]
    suppressed_findings: tuple[NormalizedFinding, ...] = ()


def classify_finding_lifecycle(
    current_findings: tuple[NormalizedFinding, ...],
    previous_findings: tuple[NormalizedFinding, ...],
) -> LifecycleReport:
    """Classify current findings as new or recurring and infer resolved findings."""

    previous_by_fingerprint = {finding.fingerprint: finding for finding in previous_findings}
    current_fingerprints = {finding.fingerprint for finding in current_findings}

    active = tuple(
        replace(
            finding,
            lifecycle_status=(
                "recurring" if finding.fingerprint in previous_by_fingerprint else "new"
            ),
        )
        for finding in current_findings
    )

    resolved = tuple(
        replace(previous, lifecycle_status="resolved")
        for fingerprint, previous in sorted(previous_by_fingerprint.items())
        if fingerprint not in current_fingerprints
    )
    return LifecycleReport(
        active_findings=active,
        resolved_findings=resolved,
        suppressed_findings=(),
    )
