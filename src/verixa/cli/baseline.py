"""Implementation of ``verixa baseline ...`` commands."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from verixa.baselines.manager import BaselineManager, BaselineManagerError
from verixa.baselines.models import BaselineProposal, BaselineStatusReport
from verixa.cli.workflow import query_tag_for_command
from verixa.config.loader import load_config
from verixa.connectors.base import WarehouseConnector
from verixa.connectors.factory import create_connector
from verixa.contracts.models import ProjectConfig
from verixa.snapshot.models import ProjectSnapshot
from verixa.snapshot.service import SnapshotService
from verixa.storage.filesystem import create_snapshot_store, resolve_environment_name

ConfigLoader = Callable[..., ProjectConfig]
ConnectorFactory = Callable[..., WarehouseConnector]
SnapshotServiceFactory = Callable[[WarehouseConnector], SnapshotService]
BaselineManagerFactory = Callable[[], BaselineManager]


def run_baseline_status(
    config_path: Path,
    *,
    environment: str,
    config_loader: ConfigLoader = load_config,
    manager_factory: BaselineManagerFactory = BaselineManager,
) -> BaselineStatusReport:
    """Read the promoted baseline status for one environment."""

    config = config_loader(config_path)
    active_environment = _require_environment(environment)
    baseline_store = create_snapshot_store(config.baseline.path, environment=active_environment)
    manager = manager_factory()
    return manager.status(
        environment=active_environment,
        baseline_path=baseline_store.baseline_path,
        warning_age_seconds=config.baseline.warning_age_seconds,
    )


def run_baseline_propose(
    config_path: Path,
    *,
    environment: str,
    reason: str,
    source_names: tuple[str, ...] = (),
    max_bytes_billed: int | None = None,
    config_loader: ConfigLoader = load_config,
    connector_factory: ConnectorFactory = create_connector,
    snapshot_service_factory: SnapshotServiceFactory = SnapshotService,
    manager_factory: BaselineManagerFactory = BaselineManager,
) -> BaselineProposal:
    """Capture a current snapshot and save it as a proposal for promotion."""

    config = config_loader(config_path, source_names=source_names)
    active_environment = _require_environment(environment)
    baseline_store = create_snapshot_store(config.baseline.path, environment=active_environment)
    connector = connector_factory(
        config.warehouse,
        max_bytes_billed=max_bytes_billed,
        query_tag=query_tag_for_command("snapshot"),
    )
    service = snapshot_service_factory(connector)
    snapshot = service.capture(config, mode="snapshot")
    manager = manager_factory()
    return manager.create_proposal(
        snapshot,
        environment=active_environment,
        reason=reason,
        source_names=tuple(sorted(config.sources)),
        baseline_path=baseline_store.baseline_path,
    )


def run_baseline_promote(
    config_path: Path,
    *,
    environment: str,
    proposal_id: str,
    config_loader: ConfigLoader = load_config,
    manager_factory: BaselineManagerFactory = BaselineManager,
) -> Path:
    """Promote one proposal into the active baseline for an environment."""

    config = config_loader(config_path)
    active_environment = _require_environment(environment)
    baseline_store = create_snapshot_store(config.baseline.path, environment=active_environment)
    manager = manager_factory()
    return manager.promote_proposal(
        proposal_id,
        baseline_path=baseline_store.baseline_path,
    )


def run_baseline_accept(
    config_path: Path,
    *,
    environment: str,
    reason: str,
    source_names: tuple[str, ...] = (),
    max_bytes_billed: int | None = None,
    config_loader: ConfigLoader = load_config,
    connector_factory: ConnectorFactory = create_connector,
    snapshot_service_factory: SnapshotServiceFactory = SnapshotService,
    manager_factory: BaselineManagerFactory = BaselineManager,
) -> BaselineProposal:
    """Create a proposal that accepts an expected baseline change."""

    return run_baseline_propose(
        config_path,
        environment=environment,
        reason=reason,
        source_names=source_names,
        max_bytes_billed=max_bytes_billed,
        config_loader=config_loader,
        connector_factory=connector_factory,
        snapshot_service_factory=snapshot_service_factory,
        manager_factory=manager_factory,
    )


def _require_environment(environment: str | None) -> str:
    active_environment = resolve_environment_name(environment)
    if active_environment is None:
        raise BaselineManagerError("Baseline lifecycle commands require --environment <name>.")
    return active_environment
