"""Implementation of ``verixa cost``."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from verixa.config.loader import load_config
from verixa.connectors.bigquery.connector import BigQueryConnector
from verixa.snapshot.service import SnapshotService


@dataclass(frozen=True, slots=True)
class CostReport:
    command: str
    estimates: dict[str, int]
    max_bytes_billed: int | None = None

    @property
    def total_bytes(self) -> int:
        return sum(self.estimates.values())

    @property
    def over_limit_sources(self) -> tuple[str, ...]:
        if self.max_bytes_billed is None:
            return ()
        return tuple(
            source_name
            for source_name, estimate in sorted(self.estimates.items())
            if estimate > self.max_bytes_billed
        )


def normalize_cost_command(command: str) -> tuple[str, str]:
    normalized = command.lower()
    if normalized in {"diff", "plan", "check"}:
        display = "diff" if normalized == "plan" else normalized
        return display, "plan"
    if normalized in {"validate", "test"}:
        display = "validate" if normalized == "test" else normalized
        return display, "test"
    if normalized == "snapshot":
        return normalized, "snapshot"
    raise ValueError(f"Unsupported cost command '{command}'.")


def run_cost(
    config_path: Path,
    *,
    command: str,
    source_names: tuple[str, ...] = (),
    max_bytes_billed: int | None = None,
) -> CostReport:
    display_command, capture_mode = normalize_cost_command(command)
    config = load_config(config_path, source_names=source_names)
    connector = BigQueryConnector(config.warehouse)
    service = SnapshotService(connector)
    estimates = service.estimate_bytes(config, mode=capture_mode)  # type: ignore[arg-type]
    effective_max_bytes_billed = (
        max_bytes_billed if max_bytes_billed is not None else config.warehouse.max_bytes_billed
    )
    return CostReport(
        command=display_command,
        estimates=estimates,
        max_bytes_billed=effective_max_bytes_billed,
    )
