from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from verixa.cli.app import app


def test_init_command_creates_starter_files(tmp_path: Path) -> None:
    runner = CliRunner()

    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(app, ["init"])
        cwd = Path.cwd()

        assert result.exit_code == 0
        assert (cwd / "verixa.yaml").exists()
        assert (cwd / "verixa.risk.yaml.example").exists()
        assert (cwd / "verixa.targets.yaml.example").exists()
        assert (cwd / "verixa.suppressions.yaml.example").exists()
        assert (cwd / ".verixa").is_dir()
        assert (cwd / ".verixa" / "baselines" / "proposals").is_dir()


def test_init_command_can_write_snowflake_starter_config(tmp_path: Path) -> None:
    runner = CliRunner()

    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(app, ["init", "--warehouse", "snowflake"])
        cwd = Path.cwd()

        assert result.exit_code == 0
        config_text = (cwd / "verixa.yaml").read_text(encoding="utf-8")
        assert "kind: snowflake" in config_text
        assert "connection_name: verixa" in config_text
        assert "VERIXA_DB.RAW.STRIPE_TRANSACTIONS" in config_text
