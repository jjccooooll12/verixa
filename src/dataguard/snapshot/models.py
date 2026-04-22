"""Snapshot models persisted to local state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class AcceptedValuesSnapshot:
    """Current accepted-values results for one column."""

    column: str
    invalid_count: int
    invalid_examples: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class FreshnessSnapshot:
    """Current freshness data for one source."""

    column: str
    max_age_seconds: int
    latest_value: datetime | None
    age_seconds: int | None


@dataclass(frozen=True, slots=True)
class SourceSnapshot:
    """Observed state for a single source at one point in time."""

    source_name: str
    table: str
    schema: dict[str, str]
    row_count: int | None
    null_rates: dict[str, float | None]
    freshness: FreshnessSnapshot | None
    accepted_values: dict[str, AcceptedValuesSnapshot]
    captured_at: datetime


@dataclass(frozen=True, slots=True)
class ProjectSnapshot:
    """Observed state for all configured sources."""

    warehouse_kind: str
    generated_at: datetime
    sources: dict[str, SourceSnapshot]
