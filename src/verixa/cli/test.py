"""Implementation of ``verixa validate``."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from verixa.cli.workflow import query_tag_for_command
from verixa.config.loader import load_config
from verixa.connectors.base import WarehouseConnector
from verixa.connectors.factory import create_connector
from verixa.contracts.models import ProjectConfig
from verixa.diff.engine import build_test_result
from verixa.diff.models import DiffResult
from verixa.diff.risk import RiskConfig, enrich_risk_config_with_dbt_impacts, load_risk_config
from verixa.snapshot.service import SnapshotService

ConfigLoader = Callable[..., ProjectConfig]
RiskLoader = Callable[[Path | None], RiskConfig | None]
ConnectorFactory = Callable[..., WarehouseConnector]
SnapshotServiceFactory = Callable[[WarehouseConnector], SnapshotService]


def run_test(
    config_path: Path,
    risk_path: Path | None = None,
    *,
    source_names: tuple[str, ...] = (),
    targets_path: Path | None = None,
    max_bytes_billed: int | None = None,
    execution_mode: str = "bounded",
    query_tag: str | None = None,
    config_loader: ConfigLoader = load_config,
    risk_loader: RiskLoader = load_risk_config,
    connector_factory: ConnectorFactory = create_connector,
    snapshot_service_factory: SnapshotServiceFactory = SnapshotService,
) -> DiffResult:
    """Run contract checks against current live data."""

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
        query_tag=query_tag or query_tag_for_command("validate"),
    )
    service = snapshot_service_factory(connector)
    current = _capture_project_snapshot(
        service,
        config,
        mode="test",
        execution_mode=execution_mode,
    )
    return build_test_result(
        config,
        current,
        risk_config=risk_config,
        execution_mode=execution_mode,
    )


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
