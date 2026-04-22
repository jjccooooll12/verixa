"""Implementation of ``dataguard test``."""

from __future__ import annotations

from pathlib import Path

from dataguard.config.loader import load_config
from dataguard.connectors.bigquery.connector import BigQueryConnector
from dataguard.diff.engine import build_test_result
from dataguard.diff.models import DiffResult
from dataguard.diff.risk import load_risk_config
from dataguard.snapshot.service import SnapshotService


def run_test(
    config_path: Path,
    risk_path: Path | None = None,
    *,
    source_names: tuple[str, ...] = (),
) -> DiffResult:
    """Run contract checks against current live data."""

    config = load_config(config_path, source_names=source_names)
    risk_config = load_risk_config(risk_path)
    connector = BigQueryConnector(config.warehouse)
    service = SnapshotService(connector)
    current = service.capture(config, mode="test")
    return build_test_result(config, current, risk_config=risk_config)
