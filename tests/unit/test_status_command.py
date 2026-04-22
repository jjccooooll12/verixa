from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from verixa.cli.status import run_status


class _FakeConnector:
    def __init__(self, warehouse) -> None:  # noqa: ANN001
        self.warehouse = warehouse

    def check_auth(self) -> tuple[bool, str]:
        return True, "authenticated"


def test_run_status_reports_baseline_path_error_for_missing_environment(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
  project: demo
baseline:
  path: .verixa/{environment}/baseline.json
sources:
  stripe.transactions:
    table: raw.stripe_transactions
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with patch("verixa.cli.status.BigQueryConnector", _FakeConnector):
        report = run_status(config_path)

    assert report.environment is None
    assert report.baseline_exists is False
    assert report.baseline_path == Path(".verixa/{environment}/baseline.json")
    assert report.baseline_error is not None
    assert "requires an environment" in report.baseline_error
    assert report.auth_ok is True
