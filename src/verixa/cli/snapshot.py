"""Implementation of ``verixa snapshot``."""

from __future__ import annotations

from pathlib import Path

from verixa.config.loader import load_config
from verixa.connectors.bigquery.connector import BigQueryConnector
from verixa.snapshot.models import ProjectSnapshot
from verixa.snapshot.service import SnapshotService
from verixa.storage.filesystem import SnapshotStore


def run_snapshot(
    config_path: Path,
    *,
    source_names: tuple[str, ...] = (),
) -> tuple[ProjectSnapshot, Path]:
    """Capture the current baseline snapshot and persist it locally."""

    config = load_config(config_path, source_names=source_names)
    connector = BigQueryConnector(config.warehouse)
    service = SnapshotService(connector)
    snapshot = service.capture(config, mode="snapshot")
    store = SnapshotStore()
    if source_names:
        baseline_path = store.merge_baseline(snapshot)
    else:
        baseline_path = store.write_baseline(snapshot)
    return snapshot, baseline_path
