"""Implementation of ``verixa validate``."""

from __future__ import annotations

from pathlib import Path

from verixa.cli.test import run_test
from verixa.cli.workflow import query_tag_for_command
from verixa.diff.models import DiffResult


def run_validate(
    config_path: Path,
    risk_path: Path | None = None,
    *,
    source_names: tuple[str, ...] = (),
    targets_path: Path | None = None,
    max_bytes_billed: int | None = None,
    execution_mode: str = "bounded",
) -> DiffResult:
    """Run contract checks against current live data."""

    return run_test(
        config_path=config_path,
        risk_path=risk_path,
        source_names=source_names,
        targets_path=targets_path,
        max_bytes_billed=max_bytes_billed,
        execution_mode=execution_mode,
        query_tag=query_tag_for_command("validate"),
    )
