"""Run-history models for finding lifecycle classification."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from verixa.findings.schema import NormalizedFinding
from verixa.snapshot.models import ProjectSnapshot


@dataclass(frozen=True, slots=True)
class FindingRunRecord:
    """Persisted record of the last successful run for one command/environment."""

    schema_version: str
    command: str
    environment: str | None
    generated_at: datetime
    findings: tuple[NormalizedFinding, ...]


@dataclass(frozen=True, slots=True)
class SnapshotRunRecord:
    """Persisted record of a captured project snapshot for history-aware drift."""

    schema_version: str
    command: str
    environment: str | None
    execution_mode: str
    generated_at: datetime
    snapshot: ProjectSnapshot
