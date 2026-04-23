"""Run-history models for finding lifecycle classification."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from verixa.findings.schema import NormalizedFinding


@dataclass(frozen=True, slots=True)
class FindingRunRecord:
    """Persisted record of the last successful run for one command/environment."""

    schema_version: str
    command: str
    environment: str | None
    generated_at: datetime
    findings: tuple[NormalizedFinding, ...]
