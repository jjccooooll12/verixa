"""Implementation of ``dataguard check``."""

from __future__ import annotations

from pathlib import Path

from dataguard.cli.plan import run_plan
from dataguard.diff.models import DiffResult


def run_check(
    config_path: Path,
    risk_path: Path | None = None,
    *,
    source_names: tuple[str, ...] = (),
) -> DiffResult:
    """CI-friendly wrapper around plan semantics."""

    return run_plan(
        config_path=config_path,
        risk_path=risk_path,
        source_names=source_names,
    )
