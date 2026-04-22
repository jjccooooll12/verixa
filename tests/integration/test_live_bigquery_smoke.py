from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dataguard.cli.app import app

pytestmark = pytest.mark.skipif(
    os.getenv("DATAGUARD_RUN_LIVE_BIGQUERY") != "1",
    reason="Set DATAGUARD_RUN_LIVE_BIGQUERY=1 to enable live BigQuery smoke tests.",
)


def test_live_bigquery_snapshot_plan_and_check(tmp_path: Path, monkeypatch) -> None:
    pytest.importorskip("google.cloud.bigquery")

    config_env = os.getenv("DATAGUARD_LIVE_CONFIG")
    if not config_env:
        pytest.skip("Set DATAGUARD_LIVE_CONFIG to a real DataGuard config file.")

    source_config_path = Path(config_env)
    if not source_config_path.exists():
        pytest.skip(f"Live config '{source_config_path}' does not exist.")

    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    local_config_path = tmp_path / "dataguard.yaml"
    local_config_path.write_text(
        source_config_path.read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    snapshot_result = runner.invoke(
        app,
        ["snapshot", "--config", str(local_config_path), "--format", "json"],
    )
    assert snapshot_result.exit_code == 0, snapshot_result.output
    snapshot_payload = json.loads(snapshot_result.stdout)
    assert snapshot_payload["summary"]["sources_captured"] >= 1

    plan_result = runner.invoke(
        app,
        ["plan", "--config", str(local_config_path), "--format", "json"],
    )
    assert plan_result.exit_code == 0, plan_result.output
    plan_payload = json.loads(plan_result.stdout)
    assert plan_payload["summary"]["sources_checked"] >= 1

    check_result = runner.invoke(
        app,
        ["check", "--config", str(local_config_path), "--format", "json"],
    )
    assert check_result.exit_code == 0, check_result.output
    check_payload = json.loads(check_result.stdout)
    assert check_payload["summary"]["sources_checked"] >= 1
