"""Runtime warehouse impact summaries for CLI output."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

ImpactMode = Literal["estimated", "actual"]


@dataclass(frozen=True, slots=True)
class RuntimeImpactRecord:
    """One warehouse query-usage record."""

    query_id: str
    query_tag: str | None
    warehouse_name: str | None
    start_time: datetime | None
    total_elapsed_ms: int | None
    bytes_scanned: int | None
    bytes_written: int | None
    rows_produced: int | None


@dataclass(frozen=True, slots=True)
class RuntimeImpact:
    """Execution-cost summary attached to one Verixa CLI run."""

    warehouse_kind: str
    mode: ImpactMode
    execution_mode: str | None = None
    query_tag: str | None = None
    estimated_bytes_by_source: dict[str, int] = field(default_factory=dict)
    actual_records: tuple[RuntimeImpactRecord, ...] = ()

    @property
    def estimated_total_bytes(self) -> int:
        return sum(self.estimated_bytes_by_source.values())

    @property
    def actual_query_count(self) -> int:
        return len(self.actual_records)

    @property
    def actual_total_bytes_scanned(self) -> int:
        return sum(record.bytes_scanned or 0 for record in self.actual_records)

    @property
    def actual_total_bytes_written(self) -> int:
        return sum(record.bytes_written or 0 for record in self.actual_records)

    @property
    def actual_total_elapsed_ms(self) -> int:
        return sum(record.total_elapsed_ms or 0 for record in self.actual_records)

