"""Implementation of ``verixa diff`` compatibility logic."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from verixa.cli.workflow import query_tag_for_command
from verixa.config.loader import load_config
from verixa.connectors.base import WarehouseConnector
from verixa.connectors.factory import create_connector
from verixa.contracts.models import ProjectConfig
from verixa.diff.engine import build_plan_result
from verixa.diff.models import DiffResult
from verixa.diff.risk import RiskConfig, load_risk_config
from verixa.snapshot.service import SnapshotService
from verixa.storage.filesystem import SnapshotStore, create_snapshot_store

ConfigLoader = Callable[..., ProjectConfig]
RiskLoader = Callable[[Path | None], RiskConfig | None]
ConnectorFactory = Callable[..., WarehouseConnector]
SnapshotServiceFactory = Callable[[WarehouseConnector], SnapshotService]
SnapshotStoreFactory = Callable[[], SnapshotStore]


def run_plan(
    config_path: Path,
    risk_path: Path | None = None,
    *,
    source_names: tuple[str, ...] = (),
    environment: str | None = None,
    max_bytes_billed: int | None = None,
    query_tag: str | None = None,
    config_loader: ConfigLoader = load_config,
    risk_loader: RiskLoader = load_risk_config,
    connector_factory: ConnectorFactory = create_connector,
    snapshot_service_factory: SnapshotServiceFactory = SnapshotService,
    snapshot_store_factory: SnapshotStoreFactory = SnapshotStore,
) -> DiffResult:
    """Compare current data state with the stored baseline and declared contracts."""

    config = config_loader(config_path, source_names=source_names)
    risk_config = risk_loader(risk_path)
    connector = connector_factory(
        config.warehouse,
        max_bytes_billed=max_bytes_billed,
        query_tag=query_tag or query_tag_for_command("diff"),
    )
    service = snapshot_service_factory(connector)
    if snapshot_store_factory is SnapshotStore:
        store = create_snapshot_store(config.baseline.path, environment=environment)
    else:
        store = snapshot_store_factory()
    baseline = store.read_baseline()
    current = service.capture(config, mode="plan")
    return build_plan_result(config, baseline, current, risk_config=risk_config)
