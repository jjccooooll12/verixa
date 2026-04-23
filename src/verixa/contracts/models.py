"""Canonical contract models used across Verixa."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, TypeAlias

from verixa.contracts.normalize import is_numeric_type


@dataclass(frozen=True, slots=True)
class WarehouseConfig:
    """Warehouse settings for a Verixa project."""

    kind: str
    project: str | None = None
    location: str | None = None
    max_bytes_billed: int | None = None
    account: str | None = None
    user: str | None = None
    password_env: str | None = None
    warehouse_name: str | None = None
    database: str | None = None
    schema: str | None = None
    role: str | None = None
    authenticator: str | None = None
    connection_name: str | None = None


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
SeverityLevel: TypeAlias = Literal["error", "warning", "info"]


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
class NumericDistributionChangeThresholds:
    """Thresholds for numeric summary drift findings."""

    warning_relative_delta: float = 0.25
    error_relative_delta: float = 0.50
    minimum_baseline_value: float = 1.0


@dataclass(frozen=True, slots=True)
class HistoryDriftConfig:
    """Optional history-aware drift settings for noisy sources."""

    window: int = 5
    minimum_snapshots: int = 3
    row_count: bool = True
    null_rate: bool = True
    numeric_distribution: bool = True
    backfill_mode: bool = False


@dataclass(frozen=True, slots=True)
class RulesConfig:
    """Project-level heuristic thresholds."""

    null_rate_change: NullRateChangeThresholds = field(
        default_factory=NullRateChangeThresholds
    )
    row_count_change: RowCountChangeThresholds = field(
        default_factory=RowCountChangeThresholds
    )
    numeric_distribution_change: NumericDistributionChangeThresholds = field(
        default_factory=NumericDistributionChangeThresholds
    )


@dataclass(frozen=True, slots=True)
class BaselineConfig:
    """Baseline freshness settings for plan/check runs."""

    warning_age: str | None = "168h"
    warning_age_seconds: int | None = 7 * 24 * 3600
    path: str = ".verixa/baseline.json"


@dataclass(frozen=True, slots=True)
class CheckConfig:
    """Check-specific CI policy settings."""

    fail_on_warning: bool = False
    advisory: bool = False


@dataclass(frozen=True, slots=True)
class SourceContract:
    """A single logical source declaration."""

    name: str
    table: str
    schema: dict[str, str]
    freshness: FreshnessConfig | None
    tests: tuple[TestDefinition, ...]
    scan: ScanConfig | None = None
    history: HistoryDriftConfig | None = None
    check: CheckConfig = field(default_factory=CheckConfig)
    rules: RulesConfig = field(default_factory=RulesConfig)
    severity_overrides: dict[str, SeverityLevel] = field(default_factory=dict)

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

    @property
    def numeric_summary_columns(self) -> tuple[str, ...]:
        columns = sorted(
            column_name
            for column_name, column_type in self.schema.items()
            if is_numeric_type(column_type)
        )
        return tuple(columns)


@dataclass(frozen=True, slots=True)
class ProjectConfig:
    """Top-level project configuration."""

    warehouse: WarehouseConfig
    sources: dict[str, SourceContract]
    rules: RulesConfig = field(default_factory=RulesConfig)
    baseline: BaselineConfig = field(default_factory=BaselineConfig)
    check: CheckConfig = field(default_factory=CheckConfig)
