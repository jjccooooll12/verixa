"""Canonical contract models used across Verixa."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TypeAlias


@dataclass(frozen=True, slots=True)
class WarehouseConfig:
    """Warehouse settings for a Verixa project."""

    kind: str
    project: str | None = None
    location: str | None = None


@dataclass(frozen=True, slots=True)
class FreshnessConfig:
    """Freshness SLA for a source table."""

    column: str
    max_age: str
    max_age_seconds: int


@dataclass(frozen=True, slots=True)
class ScanConfig:
    """Optional bounded scan settings for expensive warehouse stats queries."""

    timestamp_column: str
    column_type: str
    lookback: str
    lookback_seconds: int


@dataclass(frozen=True, slots=True)
class NoNullsTest:
    """Contract rule requiring a column to contain no NULL values."""

    column: str


@dataclass(frozen=True, slots=True)
class AcceptedValuesTest:
    """Contract rule limiting a column to a fixed set of values."""

    column: str
    values: tuple[str, ...]


TestDefinition: TypeAlias = NoNullsTest | AcceptedValuesTest


@dataclass(frozen=True, slots=True)
class NullRateChangeThresholds:
    """Thresholds for null-rate drift findings."""

    warning_delta: float = 0.01
    error_delta: float = 0.05


@dataclass(frozen=True, slots=True)
class RowCountChangeThresholds:
    """Thresholds for row-count drift findings."""

    warning_drop_ratio: float = 0.20
    error_drop_ratio: float = 0.50
    warning_growth_ratio: float = 0.20
    error_growth_ratio: float = 1.00


@dataclass(frozen=True, slots=True)
class RulesConfig:
    """Project-level heuristic thresholds."""

    null_rate_change: NullRateChangeThresholds = field(
        default_factory=NullRateChangeThresholds
    )
    row_count_change: RowCountChangeThresholds = field(
        default_factory=RowCountChangeThresholds
    )


@dataclass(frozen=True, slots=True)
class BaselineConfig:
    """Baseline freshness settings for plan/check runs."""

    warning_age: str | None = "168h"
    warning_age_seconds: int | None = 7 * 24 * 3600


@dataclass(frozen=True, slots=True)
class CheckConfig:
    """Check-specific CI policy settings."""

    fail_on_warning: bool = False


@dataclass(frozen=True, slots=True)
class SourceContract:
    """A single logical source declaration."""

    name: str
    table: str
    schema: dict[str, str]
    freshness: FreshnessConfig | None
    tests: tuple[TestDefinition, ...]
    scan: ScanConfig | None = None
    check: CheckConfig = field(default_factory=CheckConfig)
    rules: RulesConfig = field(default_factory=RulesConfig)

    @property
    def declared_columns(self) -> tuple[str, ...]:
        return tuple(sorted(self.schema))

    @property
    def no_null_columns(self) -> tuple[str, ...]:
        columns = sorted(
            test.column for test in self.tests if isinstance(test, NoNullsTest)
        )
        return tuple(columns)

    @property
    def accepted_values_tests(self) -> tuple[AcceptedValuesTest, ...]:
        tests = sorted(
            (test for test in self.tests if isinstance(test, AcceptedValuesTest)),
            key=lambda item: item.column,
        )
        return tuple(tests)

    @property
    def relevant_columns(self) -> tuple[str, ...]:
        columns = set(self.schema)
        if self.freshness is not None:
            columns.add(self.freshness.column)
        columns.update(self.no_null_columns)
        columns.update(test.column for test in self.accepted_values_tests)
        return tuple(sorted(columns))


@dataclass(frozen=True, slots=True)
class ProjectConfig:
    """Top-level project configuration."""

    warehouse: WarehouseConfig
    sources: dict[str, SourceContract]
    rules: RulesConfig = field(default_factory=RulesConfig)
    baseline: BaselineConfig = field(default_factory=BaselineConfig)
    check: CheckConfig = field(default_factory=CheckConfig)
