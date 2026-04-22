"""Filesystem-backed local snapshot storage."""

from __future__ import annotations

import os
import re
from pathlib import Path

from verixa.snapshot.models import ProjectSnapshot
from verixa.storage.json_codec import dumps_snapshot, loads_snapshot

_ENVIRONMENT_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")


class StorageError(RuntimeError):
    """Raised when local Verixa state cannot be read or written."""


def resolve_environment_name(environment: str | None = None) -> str | None:
    """Resolve the active baseline environment from CLI input or VERIXA_ENV."""

    candidate = environment if environment is not None else os.getenv("VERIXA_ENV")
    if candidate is None:
        return None

    normalized = candidate.strip()
    if not normalized:
        return None
    if not _ENVIRONMENT_PATTERN.fullmatch(normalized):
        raise StorageError(
            "Environment names may contain only letters, numbers, '.', '-', and '_'."
        )
    return normalized


def resolve_baseline_path(
    path_template: str,
    *,
    environment: str | None = None,
) -> Path:
    """Resolve a baseline path, optionally expanding an environment placeholder."""

    active_environment = resolve_environment_name(environment)
    requires_environment = "{environment}" in path_template or "{env}" in path_template
    if requires_environment and active_environment is None:
        raise StorageError(
            f"Baseline path '{path_template}' requires an environment. "
            "Pass '--environment <name>' or set VERIXA_ENV."
        )

    try:
        rendered = path_template.format_map(
            {
                "environment": active_environment,
                "env": active_environment,
            }
        )
    except KeyError as exc:
        placeholder = exc.args[0]
        raise StorageError(
            f"Unsupported baseline.path placeholder '{{{placeholder}}}' in '{path_template}'. "
            "Supported placeholders: {environment}, {env}."
        ) from exc

    return Path(rendered)


class SnapshotStore:
    """Read and write deterministic baseline snapshots from disk."""

    def __init__(
        self,
        root: Path | None = None,
        *,
        baseline_path: Path | None = None,
    ) -> None:
        self._root = root or Path(".verixa")
        self._baseline_path = baseline_path

    @property
    def baseline_path(self) -> Path:
        if self._baseline_path is not None:
            return self._baseline_path
        return self._root / "baseline.json"

    def ensure_directory(self) -> None:
        self.baseline_path.parent.mkdir(parents=True, exist_ok=True)

    def write_baseline(self, snapshot: ProjectSnapshot) -> Path:
        self.ensure_directory()
        path = self.baseline_path
        path.write_text(dumps_snapshot(snapshot), encoding="utf-8")
        return path

    def merge_baseline(self, snapshot: ProjectSnapshot) -> Path:
        """Merge captured sources into an existing baseline when present."""

        if not self.baseline_path.exists():
            return self.write_baseline(snapshot)

        existing = self.read_baseline()
        merged_sources = dict(existing.sources)
        merged_sources.update(snapshot.sources)
        merged = ProjectSnapshot(
            warehouse_kind=snapshot.warehouse_kind,
            generated_at=snapshot.generated_at,
            sources=merged_sources,
        )
        return self.write_baseline(merged)

    def read_baseline(self) -> ProjectSnapshot:
        path = self.baseline_path
        if not path.exists():
            raise StorageError(
                f"Baseline snapshot '{self.baseline_path}' was not found. Run 'verixa snapshot' first."
            )
        try:
            return loads_snapshot(path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise StorageError(f"Failed to read baseline snapshot '{path}': {exc}") from exc

    def baseline_exists(self) -> bool:
        return self.baseline_path.exists()

    def existing_baseline_path(self) -> Path:
        return self.baseline_path


def create_snapshot_store(
    path_template: str,
    *,
    environment: str | None = None,
) -> SnapshotStore:
    """Create a snapshot store for a resolved baseline path."""

    return SnapshotStore(
        baseline_path=resolve_baseline_path(
            path_template,
            environment=environment,
        )
    )
