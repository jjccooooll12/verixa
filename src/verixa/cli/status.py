"""Implementation of ``verixa status``."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from verixa.config.errors import ConfigError
from verixa.config.loader import load_config, resolve_config_path
from verixa.connectors.bigquery.connector import BigQueryConnector
from verixa.storage.filesystem import (
    SnapshotStore,
    StorageError,
    create_snapshot_store,
    resolve_environment_name,
)


@dataclass(frozen=True, slots=True)
class StatusReport:
    config_path: Path
    config_exists: bool
    config_error: str | None
    environment: str | None
    baseline_path: Path
    baseline_exists: bool
    baseline_age_seconds: int | None
    baseline_error: str | None
    auth_ok: bool | None
    auth_message: str | None
    warehouse_label: str | None
    warehouse_max_bytes_billed: int | None
    sources: tuple[str, ...]


def run_status(
    config_path: Path | None = None,
    *,
    source_names: tuple[str, ...] = (),
    environment: str | None = None,
) -> StatusReport:
    resolved_config_path = resolve_config_path(config_path)
    store: SnapshotStore | None = SnapshotStore()
    existing_baseline_path = store.existing_baseline_path()
    active_environment = resolve_environment_name(environment)

    config_exists = resolved_config_path.exists()
    config_error: str | None = None
    baseline_error: str | None = None
    auth_ok: bool | None = None
    auth_message: str | None = None
    warehouse_label: str | None = None
    warehouse_max_bytes_billed: int | None = None
    sources: tuple[str, ...] = ()

    if config_exists:
        try:
            config = load_config(resolved_config_path, source_names=source_names)
        except ConfigError as exc:
            config_error = str(exc)
        else:
            existing_baseline_path = Path(config.baseline.path)
            try:
                store = create_snapshot_store(
                    config.baseline.path,
                    environment=active_environment,
                )
            except StorageError as exc:
                baseline_error = str(exc)
                store = None
            else:
                existing_baseline_path = store.existing_baseline_path()
            warehouse_label = config.warehouse.kind
            if config.warehouse.project:
                warehouse_label += f" ({config.warehouse.project})"
            warehouse_max_bytes_billed = config.warehouse.max_bytes_billed
            sources = tuple(sorted(config.sources))
            connector = BigQueryConnector(config.warehouse)
            auth_ok, auth_message = connector.check_auth()
    else:
        warehouse_max_bytes_billed = None
        config_error = f"Config file '{resolved_config_path}' was not found."

    baseline_exists = store.baseline_exists() if store is not None else False
    baseline_age_seconds: int | None = None
    if baseline_exists and store is not None:
        try:
            snapshot = store.read_baseline()
        except StorageError as exc:
            baseline_error = str(exc)
        else:
            baseline_age_seconds = int(
                (datetime.now(timezone.utc) - snapshot.generated_at).total_seconds()
            )

    return StatusReport(
        config_path=resolved_config_path,
        config_exists=config_exists,
        config_error=config_error,
        environment=active_environment,
        baseline_path=existing_baseline_path,
        baseline_exists=baseline_exists,
        baseline_age_seconds=baseline_age_seconds,
        baseline_error=baseline_error,
        auth_ok=auth_ok,
        auth_message=auth_message,
        warehouse_label=warehouse_label,
        warehouse_max_bytes_billed=warehouse_max_bytes_billed,
        sources=sources,
    )
