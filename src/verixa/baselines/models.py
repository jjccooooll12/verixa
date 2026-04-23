"""Baseline lifecycle models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from verixa.snapshot.models import ProjectSnapshot


@dataclass(frozen=True, slots=True)
class BaselineProposal:
    """A proposed baseline change waiting for promotion."""

    schema_version: str
    proposal_id: str
    environment: str
    reason: str
    created_at: datetime
    created_by: str | None
    git_sha: str | None
    source_names: tuple[str, ...]
    baseline_path: Path
    snapshot: ProjectSnapshot


@dataclass(frozen=True, slots=True)
class BaselineStatusReport:
    """Current baseline state plus pending proposal counts for one environment."""

    environment: str
    baseline_path: Path
    baseline_exists: bool
    baseline_age_seconds: int | None
    baseline_stale: bool
    warning_age_seconds: int | None
    current_proposal_id: str | None
    current_reason: str | None
    current_created_at: datetime | None
    current_created_by: str | None
    current_git_sha: str | None
    current_promoted_at: datetime | None
    current_promoted_by: str | None
    remediation: str | None
    pending_proposals: tuple[str, ...]
