from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from typer.testing import CliRunner

from verixa.cli.app import app

pytestmark = pytest.mark.skipif(
    os.getenv("VERIXA_RUN_LIVE_SNOWFLAKE") != "1",
    reason="Set VERIXA_RUN_LIVE_SNOWFLAKE=1 to enable live Snowflake smoke tests.",
)


def test_live_snowflake_status_validate_snapshot_diff_and_check(tmp_path: Path) -> None:
    pytest.importorskip("snowflake.connector")

    config_env = os.getenv("VERIXA_LIVE_SNOWFLAKE_CONFIG")
    if not config_env:
        pytest.skip("Set VERIXA_LIVE_SNOWFLAKE_CONFIG to a real Verixa Snowflake config file.")

    source_config_path = Path(config_env)
    if not source_config_path.exists():
        pytest.skip(f"Live config '{source_config_path}' does not exist.")

    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        local_config_path = Path.cwd() / "verixa.yaml"
        local_config_path.write_text(
            source_config_path.read_text(encoding="utf-8"),
            encoding="utf-8",
        )

        status_result = runner.invoke(
            app,
            ["status", "--config", str(local_config_path), "--format", "json"],
        )
        assert status_result.exit_code == 0, status_result.output
        status_payload = json.loads(status_result.stdout)
        assert status_payload["auth"]["ok"] is True

        snapshot_result = runner.invoke(
            app,
            ["snapshot", "--config", str(local_config_path), "--format", "json"],
        )
        assert snapshot_result.exit_code == 0, snapshot_result.output
        snapshot_payload = json.loads(snapshot_result.stdout)
        assert snapshot_payload["summary"]["sources_captured"] >= 1

        validate_result = runner.invoke(
            app,
            ["validate", "--config", str(local_config_path), "--format", "json"],
        )
        assert validate_result.exit_code == 0, validate_result.output
        validate_payload = json.loads(validate_result.stdout)
        assert validate_payload["summary"]["sources_checked"] >= 1

        diff_result = runner.invoke(
            app,
            ["diff", "--config", str(local_config_path), "--format", "json"],
        )
        assert diff_result.exit_code == 0, diff_result.output
        diff_payload = json.loads(diff_result.stdout)
        assert diff_payload["summary"]["sources_checked"] >= 1

        check_result = runner.invoke(
            app,
            ["check", "--config", str(local_config_path), "--format", "json"],
        )
        assert check_result.exit_code == 0, check_result.output
        check_payload = json.loads(check_result.stdout)
        assert check_payload["summary"]["sources_checked"] >= 1
