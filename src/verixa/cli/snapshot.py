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
from verixa.history.store import SnapshotHistoryStore
from verixa.snapshot.models import ProjectSnapshot
from verixa.snapshot.service import SnapshotService
from verixa.storage.filesystem import SnapshotStore, create_snapshot_store

ConfigLoader = Callable[..., ProjectConfig]
ConnectorFactory = Callable[..., WarehouseConnector]
SnapshotServiceFactory = Callable[[WarehouseConnector], SnapshotService]
SnapshotStoreFactory = Callable[[], SnapshotStore]
SnapshotHistoryStoreFactory = Callable[[], SnapshotHistoryStore]


def run_snapshot(
    config_path: Path,
    *,
    source_names: tuple[str, ...] = (),
    environment: str | None = None,
    max_bytes_billed: int | None = None,
    execution_mode: str = "bounded",
    config_loader: ConfigLoader = load_config,
    connector_factory: ConnectorFactory = create_connector,
    snapshot_service_factory: SnapshotServiceFactory = SnapshotService,
    snapshot_store_factory: SnapshotStoreFactory = SnapshotStore,
    snapshot_history_store_factory: SnapshotHistoryStoreFactory = SnapshotHistoryStore,
) -> tuple[ProjectSnapshot, Path]:
    """Capture the current baseline snapshot and persist it locally."""

    config = config_loader(config_path, source_names=source_names)
    connector = connector_factory(
        config.warehouse,
        max_bytes_billed=max_bytes_billed,
        query_tag=query_tag_for_command("snapshot"),
    )
    service = snapshot_service_factory(connector)
    snapshot = _capture_project_snapshot(
        service,
        config,
        mode="snapshot",
        execution_mode=execution_mode,
    )
    if snapshot_store_factory is SnapshotStore:
        store = create_snapshot_store(config.baseline.path, environment=environment)
    else:
        store = snapshot_store_factory()
    if source_names:
        baseline_path = store.merge_baseline(snapshot)
    else:
        baseline_path = store.write_baseline(snapshot)
    snapshot_history_store_factory().write_run(
        "snapshot",
        snapshot,
        environment=environment,
        execution_mode=execution_mode,
    )
    return snapshot, baseline_path


def _capture_project_snapshot(
    service: SnapshotService,
    config: ProjectConfig,
    *,
    mode: str,
    execution_mode: str,
) -> ProjectSnapshot:
    capture_with_execution_mode = getattr(service, "capture_with_execution_mode", None)
    if callable(capture_with_execution_mode):
        return capture_with_execution_mode(
            config,
            mode=mode,
            execution_mode=execution_mode,
        )
    return service.capture(config, mode=mode)
