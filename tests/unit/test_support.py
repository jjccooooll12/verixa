from __future__ import annotations

from datetime import datetime, timezone

from dataguard.snapshot.models import (
    AcceptedValuesSnapshot,
    FreshnessSnapshot,
    SourceSnapshot,
)


def make_source_snapshot(
    *,
    source_name: str = "stripe.transactions",
    table: str = "demo.raw.stripe_transactions",
    schema: dict[str, str] | None = None,
    row_count: int | None = 100,
    null_rates: dict[str, float | None] | None = None,
    freshness: FreshnessSnapshot | None = None,
    accepted_values: dict[str, AcceptedValuesSnapshot] | None = None,
) -> SourceSnapshot:
    return SourceSnapshot(
        source_name=source_name,
        table=table,
        schema=schema or {"amount": "FLOAT64", "currency": "STRING"},
        row_count=row_count,
        null_rates=null_rates or {"amount": 0.0, "currency": 0.0},
        freshness=freshness,
        accepted_values=accepted_values or {},
        captured_at=datetime(2026, 4, 22, 11, 0, tzinfo=timezone.utc),
    )


def make_freshness_snapshot(age_seconds: int) -> FreshnessSnapshot:
    return FreshnessSnapshot(
        column="created_at",
        max_age_seconds=3600,
        latest_value=datetime(2026, 4, 22, 10, 0, tzinfo=timezone.utc),
        age_seconds=age_seconds,
    )


def make_accepted_values_snapshot(
    column: str,
    *,
    invalid_count: int,
    examples: tuple[str, ...] = (),
) -> AcceptedValuesSnapshot:
    return AcceptedValuesSnapshot(
        column=column,
        invalid_count=invalid_count,
        invalid_examples=examples,
    )
