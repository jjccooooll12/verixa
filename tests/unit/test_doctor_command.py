from __future__ import annotations

from pathlib import Path

from verixa.cli.doctor import run_doctor


def test_run_doctor_reports_invalid_environment_scoped_baseline_path(
    tmp_path: Path,
) -> None:
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

    result = run_doctor(config_path)

    assert result.error_count == 1
    assert result.findings[0].code == "baseline_path_invalid"
    assert "requires an environment" in result.findings[0].message
