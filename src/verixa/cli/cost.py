"""Implementation of ``verixa cost``."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Literal

from verixa.cli.workflow import resolve_workflow_command
from verixa.config.loader import load_config
from verixa.connectors.base import ConnectorError, WarehouseConnector
from verixa.connectors.factory import create_connector
from verixa.contracts.models import ProjectConfig
from verixa.snapshot.service import SnapshotService

CostMode = Literal["estimate", "history"]
ConfigLoader = Callable[..., ProjectConfig]
ConnectorFactory = Callable[..., WarehouseConnector]
SnapshotServiceFactory = Callable[[WarehouseConnector], SnapshotService]


@dataclass(frozen=True, slots=True)
class CostUsageRecord:
    """One historical warehouse-usage record returned by ``verixa cost``."""

    query_id: str
    query_tag: str | None
    warehouse_name: str | None
    start_time: datetime | None
    total_elapsed_ms: int | None
    bytes_scanned: int | None
    bytes_written: int | None
    rows_produced: int | None


@dataclass(frozen=True, slots=True)
class CostReport:
    """Cost output for either BigQuery estimates or Snowflake history reporting."""

    command: str
    mode: CostMode
    estimates: dict[str, int] = field(default_factory=dict)
    usage_records: tuple[CostUsageRecord, ...] = ()
    max_bytes_billed: int | None = None
    query_tag: str | None = None
    history_window_seconds: int | None = None

    @property
    def total_bytes(self) -> int:
        if self.mode == "estimate":
            return sum(self.estimates.values())
        return sum(record.bytes_scanned or 0 for record in self.usage_records)

    @property
    def total_bytes_written(self) -> int:
        return sum(record.bytes_written or 0 for record in self.usage_records)

    @property
    def total_elapsed_ms(self) -> int:
        return sum(record.total_elapsed_ms or 0 for record in self.usage_records)

    @property
    def over_limit_sources(self) -> tuple[str, ...]:
        if self.mode != "estimate" or self.max_bytes_billed is None:
            return ()
        return tuple(
            source_name
            for source_name, estimate in sorted(self.estimates.items())
            if estimate > self.max_bytes_billed
        )


def run_cost(
    config_path: Path,
    *,
    command: str,
    source_names: tuple[str, ...] = (),
    max_bytes_billed: int | None = None,
    history_window_seconds: int | None = None,
    execution_mode: str = "bounded",
    mode: Literal["auto", "estimate", "history"] = "auto",
    config_loader: ConfigLoader = load_config,
    connector_factory: ConnectorFactory = create_connector,
    snapshot_service_factory: SnapshotServiceFactory = SnapshotService,
) -> CostReport:
    """Estimate BigQuery query cost or report recent Snowflake warehouse usage."""

    command_spec = resolve_workflow_command(command)
    config = config_loader(config_path, source_names=source_names)
    connector = connector_factory(
        config.warehouse,
        max_bytes_billed=max_bytes_billed,
        query_tag=command_spec.query_tag,
    )

    if config.warehouse.kind == "bigquery":
        if mode == "history":
            raise ConnectorError("History-based cost reporting is currently supported only for Snowflake.")
        service = snapshot_service_factory(connector)
        estimates = _estimate_bytes(
            service,
            config,
            mode=command_spec.capture_mode,
            execution_mode=execution_mode,
        )
        effective_max_bytes_billed = (
            max_bytes_billed if max_bytes_billed is not None else config.warehouse.max_bytes_billed
        )
        return CostReport(
            command=command_spec.display_command,
            mode="estimate",
            estimates=estimates,
            max_bytes_billed=effective_max_bytes_billed,
            query_tag=command_spec.query_tag,
        )

    if config.warehouse.kind != "snowflake":
        raise ConnectorError(f"Unsupported warehouse kind '{config.warehouse.kind}'.")

    if mode == "estimate":
        raise ConnectorError("--estimate-bytes is currently supported only for BigQuery.")
    if not hasattr(connector, "report_query_usage"):
        raise ConnectorError("Snowflake cost reporting requires the Snowflake connector.")

    window_seconds = history_window_seconds if history_window_seconds is not None else 3600
    usage = connector.report_query_usage(
        query_tag=command_spec.query_tag,
        lookback_seconds=window_seconds,
    )
    records = tuple(
        CostUsageRecord(
            query_id=item.query_id,
            query_tag=item.query_tag,
            warehouse_name=item.warehouse_name,
            start_time=item.start_time,
            total_elapsed_ms=item.total_elapsed_ms,
            bytes_scanned=item.bytes_scanned,
            bytes_written=item.bytes_written,
            rows_produced=item.rows_produced,
        )
        for item in usage
    )
    return CostReport(
        command=command_spec.display_command,
        mode="history",
        usage_records=records,
        query_tag=command_spec.query_tag,
        history_window_seconds=window_seconds,
    )


def _estimate_bytes(
    service: SnapshotService,
    config: ProjectConfig,
    *,
    mode: str,
    execution_mode: str,
) -> dict[str, int]:
    estimate_with_execution_mode = getattr(service, "estimate_bytes_with_execution_mode", None)
    if callable(estimate_with_execution_mode):
        return estimate_with_execution_mode(
            config,
            mode=mode,
            execution_mode=execution_mode,
        )
    return service.estimate_bytes(config, mode=mode)
