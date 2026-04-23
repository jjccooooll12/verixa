"""Implementation of ``verixa check``."""

from __future__ import annotations

from pathlib import Path

from verixa.cli.plan import run_plan
from verixa.cli.workflow import query_tag_for_command
from verixa.diff.models import DiffResult


def run_check(
    config_path: Path,
    risk_path: Path | None = None,
    *,
    source_names: tuple[str, ...] = (),
    environment: str | None = None,
    max_bytes_billed: int | None = None,
) -> DiffResult:
    """CI-friendly wrapper around plan semantics."""

    return run_plan(
        config_path=config_path,
        risk_path=risk_path,
        source_names=source_names,
        environment=environment,
        max_bytes_billed=max_bytes_billed,
        query_tag=query_tag_for_command("check"),
    )
