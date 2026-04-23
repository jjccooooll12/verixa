"""Filesystem-backed baseline proposal and promotion workflow."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from verixa.baselines.models import BaselineProposal, BaselineStatusReport
from verixa.snapshot.models import ProjectSnapshot
from verixa.storage.filesystem import SnapshotStore, StorageError
from verixa.storage.json_codec import dumps_snapshot, loads_snapshot


class BaselineManagerError(RuntimeError):
    """Raised when baseline lifecycle operations fail."""


class BaselineManager:
    """Manage proposed and promoted baselines under `.verixa/baselines/`."""

    def __init__(self, root: Path | None = None) -> None:
        self._root = root or Path(".verixa") / "baselines"

    @property
    def proposals_dir(self) -> Path:
        return self._root / "proposals"

    def proposal_path(self, proposal_id: str) -> Path:
        return self.proposals_dir / f"{proposal_id}.json"

    def environment_dir(self, environment: str) -> Path:
        return self._root / environment

    def current_manifest_path(self, environment: str) -> Path:
        return self.environment_dir(environment) / "current.manifest.json"

    def history_dir(self, environment: str) -> Path:
        return self.environment_dir(environment) / "history"

    def create_proposal(
        self,
        snapshot: ProjectSnapshot,
        *,
        environment: str,
        reason: str,
        source_names: tuple[str, ...],
        baseline_path: Path,
    ) -> BaselineProposal:
        if not reason.strip():
            raise BaselineManagerError("Baseline proposals require a non-empty reason.")

        proposal = BaselineProposal(
            schema_version="verixa.baseline.proposal.v1",
            proposal_id=f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}",
            environment=environment,
            reason=reason.strip(),
            created_at=datetime.now(timezone.utc),
            created_by=_current_actor(),
            git_sha=_current_git_sha(),
            source_names=source_names,
            baseline_path=baseline_path,
            snapshot=snapshot,
        )
        self.proposals_dir.mkdir(parents=True, exist_ok=True)
        self.proposal_path(proposal.proposal_id).write_text(
            json.dumps(_proposal_to_data(proposal), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return proposal

    def read_proposal(self, proposal_id: str) -> BaselineProposal:
        path = self.proposal_path(proposal_id)
        if not path.exists():
            raise BaselineManagerError(f"Baseline proposal '{proposal_id}' was not found.")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise BaselineManagerError(f"Failed to read baseline proposal '{proposal_id}': {exc}") from exc
        return _proposal_from_data(payload)

    def list_proposals(self, *, environment: str | None = None) -> tuple[BaselineProposal, ...]:
        if not self.proposals_dir.exists():
            return ()
        proposals: list[BaselineProposal] = []
        for path in sorted(self.proposals_dir.glob("*.json")):
            proposal = _proposal_from_data(json.loads(path.read_text(encoding="utf-8")))
            if environment is None or proposal.environment == environment:
                proposals.append(proposal)
        return tuple(proposals)

    def promote_proposal(
        self,
        proposal_id: str,
        *,
        baseline_path: Path,
    ) -> Path:
        proposal = self.read_proposal(proposal_id)
        environment_dir = self.environment_dir(proposal.environment)
        history_dir = self.history_dir(proposal.environment)
        environment_dir.mkdir(parents=True, exist_ok=True)
        history_dir.mkdir(parents=True, exist_ok=True)

        current_manifest_path = self.current_manifest_path(proposal.environment)
        if baseline_path.exists():
            archived_baseline = history_dir / f"{proposal.proposal_id}.baseline.json"
            baseline_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(baseline_path, archived_baseline)
        if current_manifest_path.exists():
            archived_manifest = history_dir / f"{proposal.proposal_id}.manifest.json"
            shutil.copy2(current_manifest_path, archived_manifest)

        SnapshotStore(baseline_path=baseline_path).write_baseline(proposal.snapshot)
        promoted_at = datetime.now(timezone.utc)
        promoted_by = _current_actor()
        current_manifest_path.write_text(
            json.dumps(
                {
                    "schema_version": "verixa.baseline.current.v1",
                    "proposal_id": proposal.proposal_id,
                    "environment": proposal.environment,
                    "reason": proposal.reason,
                    "created_at": proposal.created_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "created_by": proposal.created_by,
                    "git_sha": proposal.git_sha,
                    "promoted_at": promoted_at.isoformat().replace("+00:00", "Z"),
                    "promoted_by": promoted_by,
                    "source_names": list(proposal.source_names),
                    "baseline_path": str(proposal.baseline_path),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        return baseline_path

    def status(
        self,
        *,
        environment: str,
        baseline_path: Path,
        warning_age_seconds: int | None = None,
    ) -> BaselineStatusReport:
        store = SnapshotStore(baseline_path=baseline_path)
        baseline_exists = store.baseline_exists()
        baseline_age_seconds: int | None = None
        baseline_stale = False
        if baseline_exists:
            try:
                snapshot = store.read_baseline()
            except StorageError as exc:
                raise BaselineManagerError(str(exc)) from exc
            baseline_age_seconds = int((datetime.now(timezone.utc) - snapshot.generated_at).total_seconds())
            if warning_age_seconds is not None and baseline_age_seconds >= warning_age_seconds:
                baseline_stale = True

        current_proposal_id: str | None = None
        current_reason: str | None = None
        current_created_at: datetime | None = None
        current_created_by: str | None = None
        current_git_sha: str | None = None
        current_promoted_at: datetime | None = None
        current_promoted_by: str | None = None
        current_manifest_path = self.current_manifest_path(environment)
        if current_manifest_path.exists():
            payload = json.loads(current_manifest_path.read_text(encoding="utf-8"))
            current_proposal_id = payload.get("proposal_id")
            current_reason = payload.get("reason")
            created_at = payload.get("created_at")
            promoted_at = payload.get("promoted_at")
            current_created_at = (
                datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                if isinstance(created_at, str)
                else None
            )
            current_created_by = payload.get("created_by")
            current_git_sha = payload.get("git_sha")
            current_promoted_at = (
                datetime.fromisoformat(promoted_at.replace("Z", "+00:00"))
                if isinstance(promoted_at, str)
                else None
            )
            current_promoted_by = payload.get("promoted_by")

        pending = tuple(proposal.proposal_id for proposal in self.list_proposals(environment=environment))
        remediation = _baseline_status_remediation(
            environment,
            baseline_exists=baseline_exists,
            baseline_stale=baseline_stale,
        )
        return BaselineStatusReport(
            environment=environment,
            baseline_path=baseline_path,
            baseline_exists=baseline_exists,
            baseline_age_seconds=baseline_age_seconds,
            baseline_stale=baseline_stale,
            warning_age_seconds=warning_age_seconds,
            current_proposal_id=current_proposal_id,
            current_reason=current_reason,
            current_created_at=current_created_at,
            current_created_by=current_created_by,
            current_git_sha=current_git_sha,
            current_promoted_at=current_promoted_at,
            current_promoted_by=current_promoted_by,
            remediation=remediation,
            pending_proposals=pending,
        )


def _proposal_to_data(proposal: BaselineProposal) -> dict[str, Any]:
    return {
        "schema_version": proposal.schema_version,
        "proposal_id": proposal.proposal_id,
        "environment": proposal.environment,
        "reason": proposal.reason,
        "created_at": proposal.created_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "created_by": proposal.created_by,
        "git_sha": proposal.git_sha,
        "source_names": list(proposal.source_names),
        "baseline_path": str(proposal.baseline_path),
        "snapshot": json.loads(dumps_snapshot(proposal.snapshot)),
    }


def _proposal_from_data(payload: dict[str, Any]) -> BaselineProposal:
    return BaselineProposal(
        schema_version=payload["schema_version"],
        proposal_id=payload["proposal_id"],
        environment=payload["environment"],
        reason=payload["reason"],
        created_at=datetime.fromisoformat(payload["created_at"].replace("Z", "+00:00")),
        created_by=payload.get("created_by"),
        git_sha=payload.get("git_sha"),
        source_names=tuple(payload.get("source_names", ())),
        baseline_path=Path(payload["baseline_path"]),
        snapshot=loads_snapshot(json.dumps(payload["snapshot"])),
    )


def _current_actor() -> str | None:
    for key in ("VERIXA_ACTOR", "GIT_AUTHOR_NAME", "USER", "USERNAME"):
        value = os.getenv(key)
        if value and value.strip():
            return value.strip()
    return None


def _current_git_sha() -> str | None:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        return None
    value = completed.stdout.strip()
    return value or None


def _baseline_status_remediation(
    environment: str,
    *,
    baseline_exists: bool,
    baseline_stale: bool,
) -> str | None:
    if not baseline_exists:
        return (
            f"Create or promote a baseline for '{environment}' with "
            f"'verixa baseline propose --environment {environment} --reason <reason>' "
            f"and 'verixa baseline promote --environment {environment} --proposal-id <id>'."
        )
    if baseline_stale:
        return (
            f"Refresh the '{environment}' baseline with "
            f"'verixa baseline propose --environment {environment} --reason <reason>' "
            f"and promote it after review."
        )
    return None
