"""Connector interfaces and shared errors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Literal

from verixa.contracts.models import AcceptedValuesTest, SourceContract
from verixa.snapshot.models import SourceSnapshot


class ConnectorError(RuntimeError):
    """Raised when a warehouse connector cannot complete a request."""


ExecutionMode = Literal["cheap", "bounded", "full"]


@dataclass(frozen=True, slots=True)
class SourceCaptureRequest:
    """Requested warehouse stats for one source capture."""

    null_rate_columns: tuple[str, ...] = ()
    freshness_column: str | None = None
    accepted_values_tests: tuple[AcceptedValuesTest, ...] = ()
    numeric_summary_columns: tuple[str, ...] = ()
    include_exact_row_count: bool = False
    scan_timestamp_column: str | None = None
    scan_timestamp_type: str | None = None
    scan_lookback_seconds: int | None = None

    @property
    def needs_stats_query(self) -> bool:
        """Whether the connector must issue a stats query beyond metadata."""

        return bool(
            self.include_exact_row_count
            or self.null_rate_columns
            or self.freshness_column
            or self.accepted_values_tests
            or self.numeric_summary_columns
        )

    @classmethod
    def for_snapshot(
        cls,
        source: SourceContract,
        *,
        execution_mode: ExecutionMode = "bounded",
    ) -> "SourceCaptureRequest":
        """Capture the full baseline shape needed for snapshot and plan."""

        if execution_mode == "cheap":
            scan_timestamp_column, scan_timestamp_type, scan_lookback_seconds = _scan_settings(
                source,
                execution_mode="bounded",
            )
            return cls(
                null_rate_columns=source.declared_columns,
                freshness_column=source.freshness.column if source.freshness else None,
                accepted_values_tests=source.accepted_values_tests,
                numeric_summary_columns=(),
                include_exact_row_count=False,
                scan_timestamp_column=scan_timestamp_column,
                scan_timestamp_type=scan_timestamp_type,
                scan_lookback_seconds=scan_lookback_seconds,
            )

        scan_timestamp_column, scan_timestamp_type, scan_lookback_seconds = _scan_settings(
            source,
            execution_mode=execution_mode,
        )
        return cls(
            null_rate_columns=source.declared_columns,
            freshness_column=source.freshness.column if source.freshness else None,
            accepted_values_tests=source.accepted_values_tests,
            numeric_summary_columns=source.numeric_summary_columns,
            include_exact_row_count=True,
            scan_timestamp_column=scan_timestamp_column,
            scan_timestamp_type=scan_timestamp_type,
            scan_lookback_seconds=scan_lookback_seconds,
        )

    @classmethod
    def for_plan(
        cls,
        source: SourceContract,
        *,
        execution_mode: ExecutionMode = "bounded",
    ) -> "SourceCaptureRequest":
        """Capture the full current shape needed for baseline drift checks."""

        if execution_mode == "cheap":
            scan_timestamp_column, scan_timestamp_type, scan_lookback_seconds = _scan_settings(
                source,
                execution_mode="bounded",
            )
            return cls(
                null_rate_columns=source.declared_columns,
                freshness_column=source.freshness.column if source.freshness else None,
                accepted_values_tests=source.accepted_values_tests,
                numeric_summary_columns=(),
                include_exact_row_count=False,
                scan_timestamp_column=scan_timestamp_column,
                scan_timestamp_type=scan_timestamp_type,
                scan_lookback_seconds=scan_lookback_seconds,
            )
        return cls.for_snapshot(source, execution_mode=execution_mode)

    @classmethod
    def for_test(
        cls,
        source: SourceContract,
        *,
        execution_mode: ExecutionMode = "bounded",
    ) -> "SourceCaptureRequest":
        """Capture only the contract checks needed for ``verixa validate``."""

        scan_timestamp_column, scan_timestamp_type, scan_lookback_seconds = _scan_settings(
            source,
            execution_mode=execution_mode,
        )
        return cls(
            null_rate_columns=source.no_null_columns,
            freshness_column=source.freshness.column if source.freshness else None,
            accepted_values_tests=source.accepted_values_tests,
            numeric_summary_columns=(),
            include_exact_row_count=False,
            scan_timestamp_column=scan_timestamp_column,
            scan_timestamp_type=scan_timestamp_type,
            scan_lookback_seconds=scan_lookback_seconds,
        )


class WarehouseConnector(ABC):
    """Abstract interface for warehouse-specific data collection."""

    @abstractmethod
    def capture_source(
        self,
        source: SourceContract,
        capture_request: SourceCaptureRequest,
    ) -> SourceSnapshot:
        """Collect current state for one source contract."""

    @abstractmethod
    def estimate_source_bytes(
        self,
        source: SourceContract,
        capture_request: SourceCaptureRequest,
    ) -> int:
        """Estimate bytes processed for one source capture when supported."""

    @abstractmethod
    def check_auth(self) -> tuple[bool, str]:
        """Check whether warehouse authentication is usable."""

    @abstractmethod
    def check_source_access(self, source: SourceContract) -> tuple[bool, str]:
        """Check whether metadata for a source can be accessed."""


def _scan_settings(
    source: SourceContract,
    *,
    execution_mode: ExecutionMode,
) -> tuple[str | None, str | None, int | None]:
    if source.scan is None:
        return None, None, None
    if execution_mode == "full":
        return None, None, None
    return (
        source.scan.timestamp_column,
        source.scan.column_type,
        source.scan.lookback_seconds,
    )
