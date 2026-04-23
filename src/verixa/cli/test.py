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
from verixa.diff.risk import RiskConfig, load_risk_config
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
    max_bytes_billed: int | None = None,
    query_tag: str | None = None,
    config_loader: ConfigLoader = load_config,
    risk_loader: RiskLoader = load_risk_config,
    connector_factory: ConnectorFactory = create_connector,
    snapshot_service_factory: SnapshotServiceFactory = SnapshotService,
) -> DiffResult:
    """Run contract checks against current live data."""

    config = config_loader(config_path, source_names=source_names)
    risk_config = risk_loader(risk_path)
    connector = connector_factory(
        config.warehouse,
        max_bytes_billed=max_bytes_billed,
        query_tag=query_tag or query_tag_for_command("validate"),
    )
    service = snapshot_service_factory(connector)
    current = service.capture(config, mode="test")
    return build_test_result(config, current, risk_config=risk_config)
