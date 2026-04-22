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
        assert (cwd / ".verixa").is_dir()
