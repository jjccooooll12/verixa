"""Filesystem-backed local snapshot storage."""

from __future__ import annotations

from pathlib import Path

from verixa.snapshot.models import ProjectSnapshot
from verixa.storage.json_codec import dumps_snapshot, loads_snapshot


class StorageError(RuntimeError):
    """Raised when local Verixa state cannot be read or written."""


class SnapshotStore:
    """Read and write deterministic baseline snapshots from disk."""

    def __init__(self, root: Path | None = None) -> None:
        self._root = root or Path(".verixa")

    @property
    def baseline_path(self) -> Path:
        return self._root / "baseline.json"

    def ensure_directory(self) -> None:
        self._root.mkdir(parents=True, exist_ok=True)

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
