"""Implementation of ``verixa validate``."""

from __future__ import annotations

from pathlib import Path

from verixa.cli.test import run_test
from verixa.diff.models import DiffResult


def run_validate(
    config_path: Path,
    risk_path: Path | None = None,
    *,
    source_names: tuple[str, ...] = (),
) -> DiffResult:
    """Run contract checks against current live data."""

    return run_test(config_path=config_path, risk_path=risk_path, source_names=source_names)
