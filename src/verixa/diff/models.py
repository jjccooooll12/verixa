"""Diff and finding models used by plan/test/check commands."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Severity = Literal["error", "warning", "info"]


@dataclass(frozen=True, slots=True)
class Finding:
    """A single user-facing result from plan/test/check."""

    source_name: str
    severity: Severity
    code: str
    message: str
    column: str | None = None
    risks: tuple[str, ...] = ()
    owners: tuple[str, ...] = ()
    source_criticality: Literal["low", "medium", "high"] | None = None


@dataclass(frozen=True, slots=True)
class DiffResult:
    """Aggregated result for one command run."""

    findings: tuple[Finding, ...]
    sources_checked: int
    used_baseline: bool
    warning_policy_sources: tuple[str, ...] = ()

    @property
    def error_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "warning")

    @property
    def warning_policy_failure_count(self) -> int:
        return len(self.warning_policy_sources)
