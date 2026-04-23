from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from verixa.diff.models import DiffResult, Finding
from verixa.findings.schema import normalize_diff_result
from verixa.history.classifier import classify_finding_lifecycle
from verixa.suppressions.apply import apply_suppressions
from verixa.suppressions.loader import load_suppressions, split_active_and_expired


def test_load_suppressions_and_split_expired(tmp_path: Path) -> None:
    path = tmp_path / "verixa.suppressions.yaml"
    path.write_text(
        """
suppressions:
  - fingerprint: abc
    owner: data-platform
    reason: rollout
    expires_at: 2026-05-15T00:00:00Z
    environments: [staging]
  - fingerprint: def
    owner: data-platform
    reason: old rollout
    expires_at: 2026-04-01T00:00:00Z
""".strip()
        + "\n",
        encoding="utf-8",
    )

    rules = load_suppressions(path)
    active, expired = split_active_and_expired(
        rules,
        now=datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc),
    )

    assert len(active) == 1
    assert active[0].environments == ("staging",)
    assert len(expired) == 1
    assert expired[0].fingerprint == "def"


def test_apply_suppressions_filters_matching_findings(tmp_path: Path) -> None:
    result = DiffResult(
        findings=(
            Finding(
                source_name="stripe.transactions",
                severity="error",
                code="no_nulls_violation",
                message="no_nulls violated on amount",
                column="amount",
            ),
            Finding(
                source_name="stripe.transactions",
                severity="warning",
                code="row_count_changed",
                message="row count changed",
            ),
        ),
        sources_checked=1,
        used_baseline=True,
    )
    normalized = normalize_diff_result(result)
    lifecycle = classify_finding_lifecycle(normalized, ())

    rules = load_suppressions(
        _write_suppressions(
            tmp_path,
            f"""
suppressions:
  - fingerprint: {normalized[0].fingerprint}
    owner: data-platform
    reason: expected rollout
    expires_at: 2026-05-15T00:00:00Z
    environments: [prod]
"""
        )
    )

    outcome = apply_suppressions(
        result,
        environment="prod",
        rules=rules,
        lifecycle_report=lifecycle,
    )

    assert len(outcome.result.findings) == 1
    assert outcome.result.findings[0].code == "row_count_changed"
    assert len(outcome.lifecycle_report.suppressed_findings) == 1
    assert outcome.lifecycle_report.suppressed_findings[0].lifecycle_status == "suppressed"


def _write_suppressions(tmp_path: Path, contents: str) -> Path:
    path = tmp_path / "verixa.suppressions.yaml"
    path.write_text(contents.strip() + "\n", encoding="utf-8")
    return path
