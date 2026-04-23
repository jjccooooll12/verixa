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
from verixa.diff.risk import RiskConfig, enrich_risk_config_with_dbt_impacts, load_risk_config
from verixa.history.store import SnapshotHistoryStore
from verixa.snapshot.service import SnapshotService
from verixa.storage.filesystem import SnapshotStore, create_snapshot_store

ConfigLoader = Callable[..., ProjectConfig]
RiskLoader = Callable[[Path | None], RiskConfig | None]
ConnectorFactory = Callable[..., WarehouseConnector]
SnapshotServiceFactory = Callable[[WarehouseConnector], SnapshotService]
SnapshotStoreFactory = Callable[[], SnapshotStore]
SnapshotHistoryStoreFactory = Callable[[], SnapshotHistoryStore]


def run_plan(
    config_path: Path,
    risk_path: Path | None = None,
    *,
    source_names: tuple[str, ...] = (),
    targets_path: Path | None = None,
    environment: str | None = None,
    max_bytes_billed: int | None = None,
    execution_mode: str = "bounded",
    query_tag: str | None = None,
    history_command: str = "diff",
    config_loader: ConfigLoader = load_config,
    risk_loader: RiskLoader = load_risk_config,
    connector_factory: ConnectorFactory = create_connector,
    snapshot_service_factory: SnapshotServiceFactory = SnapshotService,
    snapshot_store_factory: SnapshotStoreFactory = SnapshotStore,
    snapshot_history_store_factory: SnapshotHistoryStoreFactory = SnapshotHistoryStore,
) -> DiffResult:
    """Compare current data state with the stored baseline and declared contracts."""

    config = config_loader(config_path, source_names=source_names)
    risk_config = risk_loader(risk_path)
    risk_config = enrich_risk_config_with_dbt_impacts(
        risk_config,
        config=config,
        targets_path=targets_path if targets_path is not None else config_path.parent / "verixa.targets.yaml",
    )
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
    history_store = snapshot_history_store_factory()
    baseline = store.read_baseline()
    history_snapshots = _load_snapshot_history(
        history_store,
        config,
        environment=environment,
    )
    current = _capture_project_snapshot(
        service,
        config,
        mode="plan",
        execution_mode=execution_mode,
    )
    result = build_plan_result(
        config,
        baseline,
        current,
        risk_config=risk_config,
        historical_snapshots=history_snapshots,
        execution_mode=execution_mode,
    )
    history_store.write_run(
        history_command,
        current,
        environment=environment,
        execution_mode=execution_mode,
    )
    return result


def _capture_project_snapshot(
    service: SnapshotService,
    config: ProjectConfig,
    *,
    mode: str,
    execution_mode: str,
):
    capture_with_execution_mode = getattr(service, "capture_with_execution_mode", None)
    if callable(capture_with_execution_mode):
        return capture_with_execution_mode(
            config,
            mode=mode,
            execution_mode=execution_mode,
        )
    return service.capture(config, mode=mode)


def _load_snapshot_history(
    store: SnapshotHistoryStore,
    config: ProjectConfig,
    *,
    environment: str | None,
) -> tuple:
    history_window = max(
        (
            source.history.window
            for source in config.sources.values()
            if source.history is not None
        ),
        default=0,
    )
    if history_window == 0:
        return ()
    records = store.list_runs(environment=environment, limit=history_window)
    return tuple(record.snapshot for record in records)
