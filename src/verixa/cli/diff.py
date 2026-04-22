"""Implementation of ``verixa diff``."""

from __future__ import annotations

from pathlib import Path

from verixa.cli.plan import run_plan
from verixa.diff.models import DiffResult


def run_diff(
    config_path: Path,
    risk_path: Path | None = None,
    *,
    source_names: tuple[str, ...] = (),
    environment: str | None = None,
    max_bytes_billed: int | None = None,
) -> DiffResult:
    """Compare current data state with the stored baseline and declared contracts."""

    return run_plan(
        config_path=config_path,
        risk_path=risk_path,
        source_names=source_names,
        environment=environment,
        max_bytes_billed=max_bytes_billed,
    )
