from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from verixa.snapshot.models import (
    AcceptedValuesSnapshot,
    FreshnessSnapshot,
    NumericSummarySnapshot,
    ProjectSnapshot,
    SourceSnapshot,
)
from verixa.storage.filesystem import SnapshotStore, StorageError, resolve_baseline_path


def test_snapshot_store_round_trip(tmp_path: Path) -> None:
    store = SnapshotStore(tmp_path / ".verixa")
    snapshot = ProjectSnapshot(
        warehouse_kind="bigquery",
        generated_at=datetime(2026, 4, 22, 11, 0, tzinfo=timezone.utc),
        sources={
            "stripe.transactions": SourceSnapshot(
                source_name="stripe.transactions",
                table="demo.raw.stripe_transactions",
                schema={"amount": "FLOAT64", "currency": "STRING"},
                row_count=42,
                null_rates={"amount": 0.0, "currency": 0.1},
                freshness=FreshnessSnapshot(
                    column="created_at",
                    max_age_seconds=3600,
                    latest_value=datetime(2026, 4, 22, 10, 30, tzinfo=timezone.utc),
                    age_seconds=1800,
                ),
                accepted_values={
                    "currency": AcceptedValuesSnapshot(
                        column="currency",
                        invalid_count=1,
                        invalid_examples=("AUD",),
                    )
                },
                captured_at=datetime(2026, 4, 22, 11, 0, tzinfo=timezone.utc),
                numeric_summaries={
                    "amount": NumericSummarySnapshot(
                        column="amount",
                        min_value=1.0,
                        p50_value=5.0,
                        p95_value=9.0,
                        max_value=10.0,
                        mean_value=5.5,
                    )
                },
            )
        },
    )

    path = store.write_baseline(snapshot)
    loaded = store.read_baseline()

    assert path.name == "baseline.json"
    assert loaded == snapshot


def test_snapshot_store_merges_selected_sources_into_existing_baseline(tmp_path: Path) -> None:
    store = SnapshotStore(tmp_path / ".verixa")
    baseline = ProjectSnapshot(
        warehouse_kind="bigquery",
        generated_at=datetime(2026, 4, 22, 11, 0, tzinfo=timezone.utc),
        sources={
            "payments.transactions": SourceSnapshot(
                source_name="payments.transactions",
                table="demo.raw.payments_transactions",
                schema={"amount": "FLOAT64"},
                row_count=10,
                null_rates={"amount": 0.0},
                freshness=None,
                accepted_values={},
                captured_at=datetime(2026, 4, 22, 11, 0, tzinfo=timezone.utc),
            )
        },
    )
    update = ProjectSnapshot(
        warehouse_kind="bigquery",
        generated_at=datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc),
        sources={
            "stripe.transactions": SourceSnapshot(
                source_name="stripe.transactions",
                table="demo.raw.stripe_transactions",
                schema={"amount": "FLOAT64"},
                row_count=42,
                null_rates={"amount": 0.0},
                freshness=None,
                accepted_values={},
                captured_at=datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc),
            )
        },
    )

    store.write_baseline(baseline)
    store.merge_baseline(update)
    loaded = store.read_baseline()

    assert tuple(sorted(loaded.sources)) == (
        "payments.transactions",
        "stripe.transactions",
    )
    assert loaded.generated_at == update.generated_at


def test_resolve_baseline_path_expands_environment_placeholder() -> None:
    path = resolve_baseline_path(
        ".verixa/{environment}/baseline.json",
        environment="prod",
    )

    assert path == Path(".verixa/prod/baseline.json")


def test_resolve_baseline_path_uses_verixa_env() -> None:
    with patch.dict("os.environ", {"VERIXA_ENV": "staging"}):
        path = resolve_baseline_path(".verixa/{env}/baseline.json")

    assert path == Path(".verixa/staging/baseline.json")


def test_resolve_baseline_path_requires_environment_for_placeholder() -> None:
    with pytest.raises(StorageError, match="requires an environment"):
        resolve_baseline_path(".verixa/{environment}/baseline.json")


def test_resolve_baseline_path_rejects_invalid_environment_name() -> None:
    with pytest.raises(StorageError, match="Environment names may contain only"):
        resolve_baseline_path(".verixa/baseline.json", environment="../prod")
