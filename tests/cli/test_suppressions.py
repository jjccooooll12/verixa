from __future__ import annotations

import json

from typer.testing import CliRunner

from tests.cli.support import build_app
from verixa.diff.models import DiffResult, Finding
from verixa.findings.schema import normalize_diff_result


def test_check_command_suppresses_matching_finding(tmp_path) -> None:  # noqa: ANN001
    runner = CliRunner()
    finding = Finding(
        source_name="stripe.transactions",
        severity="error",
        code="no_nulls_violation",
        message="no_nulls violated on amount",
        column="amount",
    )
    fingerprint = normalize_diff_result(
        DiffResult(findings=(finding,), sources_checked=1, used_baseline=False)
    )[0].fingerprint

    app = build_app(
        run_check=lambda config, risk_path=None, source_names=(), environment=None, max_bytes_billed=None: DiffResult(
            findings=(finding,),
            sources_checked=1,
            used_baseline=False,
        )
    )

    with runner.isolated_filesystem(temp_dir=tmp_path):
        with open("verixa.suppressions.yaml", "w", encoding="utf-8") as handle:
            handle.write(
                (
                    "suppressions:\n"
                    f"  - fingerprint: {fingerprint}\n"
                    "    owner: data-platform\n"
                    "    reason: expected rollout\n"
                    "    expires_at: 2026-05-15T00:00:00Z\n"
                )
            )

        result = runner.invoke(app, ["check", "--fail-on-error", "--format", "json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["summary"]["errors"] == 0
    assert payload["summary"]["suppressed_findings"] == 1
    assert payload["findings"] == []


def test_doctor_reports_expired_suppressions(tmp_path) -> None:  # noqa: ANN001
    runner = CliRunner()
    app = build_app(
        run_doctor=lambda config, source_names=(), environment=None: DiffResult(
            findings=(),
            sources_checked=0,
            used_baseline=False,
        )
    )

    with runner.isolated_filesystem(temp_dir=tmp_path):
        with open("verixa.suppressions.yaml", "w", encoding="utf-8") as handle:
            handle.write(
                (
                    "suppressions:\n"
                    "  - fingerprint: deadbeef\n"
                    "    owner: data-platform\n"
                    "    reason: stale waiver\n"
                    "    expires_at: 2026-04-01T00:00:00Z\n"
                )
            )

        result = runner.invoke(app, ["doctor", "--format", "json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["summary"]["warnings"] == 1
    assert payload["findings"][0]["stable_code"] == "suppression.expired"
