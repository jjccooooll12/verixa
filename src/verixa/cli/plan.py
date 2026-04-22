"""Implementation of ``verixa diff`` compatibility logic."""

from __future__ import annotations

from pathlib import Path

from verixa.config.loader import load_config
from verixa.connectors.bigquery.connector import BigQueryConnector
from verixa.diff.engine import build_plan_result
from verixa.diff.models import DiffResult
from verixa.diff.risk import load_risk_config
from verixa.snapshot.service import SnapshotService
from verixa.storage.filesystem import SnapshotStore


def run_plan(
    config_path: Path,
    risk_path: Path | None = None,
    *,
    source_names: tuple[str, ...] = (),
) -> DiffResult:
    """Compare current data state with the stored baseline and declared contracts."""

    config = load_config(config_path, source_names=source_names)
    risk_config = load_risk_config(risk_path)
    connector = BigQueryConnector(config.warehouse)
    service = SnapshotService(connector)
    store = SnapshotStore()
    baseline = store.read_baseline()
    current = service.capture(config, mode="plan")
    return build_plan_result(config, baseline, current, risk_config=risk_config)
