"""Implementation of ``verixa snapshot``."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from verixa.config.loader import load_config
from verixa.cli.workflow import query_tag_for_command
from verixa.connectors.base import WarehouseConnector
from verixa.connectors.factory import create_connector
from verixa.contracts.models import ProjectConfig, WarehouseConfig
from verixa.snapshot.models import ProjectSnapshot
from verixa.snapshot.service import SnapshotService
from verixa.storage.filesystem import SnapshotStore, create_snapshot_store

ConfigLoader = Callable[..., ProjectConfig]
ConnectorFactory = Callable[..., WarehouseConnector]
SnapshotServiceFactory = Callable[[WarehouseConnector], SnapshotService]
SnapshotStoreFactory = Callable[[], SnapshotStore]


def run_snapshot(
    config_path: Path,
    *,
    source_names: tuple[str, ...] = (),
    environment: str | None = None,
    max_bytes_billed: int | None = None,
    config_loader: ConfigLoader = load_config,
    connector_factory: ConnectorFactory = create_connector,
    snapshot_service_factory: SnapshotServiceFactory = SnapshotService,
    snapshot_store_factory: SnapshotStoreFactory = SnapshotStore,
) -> tuple[ProjectSnapshot, Path]:
    """Capture the current baseline snapshot and persist it locally."""

    config = config_loader(config_path, source_names=source_names)
    connector = connector_factory(
        config.warehouse,
        max_bytes_billed=max_bytes_billed,
        query_tag=query_tag_for_command("snapshot"),
    )
    service = snapshot_service_factory(connector)
    snapshot = service.capture(config, mode="snapshot")
    if snapshot_store_factory is SnapshotStore:
        store = create_snapshot_store(config.baseline.path, environment=environment)
    else:
        store = snapshot_store_factory()
    if source_names:
        baseline_path = store.merge_baseline(snapshot)
    else:
        baseline_path = store.write_baseline(snapshot)
    return snapshot, baseline_path
