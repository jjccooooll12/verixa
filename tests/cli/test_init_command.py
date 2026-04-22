from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from verixa.cli.app import app


def test_init_command_creates_starter_files(tmp_path: Path, monkeypatch) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["init"])

    assert result.exit_code == 0
    assert (tmp_path / "verixa.yaml").exists()
    assert (tmp_path / "verixa.risk.yaml.example").exists()
    assert (tmp_path / ".verixa").is_dir()
