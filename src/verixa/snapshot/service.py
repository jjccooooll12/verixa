"""Snapshot capture orchestration."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Literal

from verixa.connectors.base import ExecutionMode, SourceCaptureRequest, WarehouseConnector
from verixa.contracts.models import ProjectConfig, SourceContract
from verixa.snapshot.models import ProjectSnapshot, SourceSnapshot

CaptureMode = Literal["snapshot", "plan", "test"]


class SnapshotService:
    """Capture the current state for all configured sources."""

    def __init__(self, connector: WarehouseConnector, *, max_workers: int = 4) -> None:
        self._connector = connector
        self._max_workers = max(1, max_workers)

    def capture(self, config: ProjectConfig, *, mode: CaptureMode = "snapshot") -> ProjectSnapshot:
        return self.capture_with_execution_mode(config, mode=mode, execution_mode="bounded")

    def capture_with_execution_mode(
        self,
        config: ProjectConfig,
        *,
        mode: CaptureMode = "snapshot",
        execution_mode: ExecutionMode = "bounded",
    ) -> ProjectSnapshot:
        source_names = tuple(sorted(config.sources))
        if len(source_names) <= 1 or self._max_workers == 1:
            sources = {
                source_name: self._capture_source(
                    config.sources[source_name],
                    mode,
                    execution_mode,
                )
                for source_name in source_names
            }
        else:
            sources = self._capture_sources_parallel(config, source_names, mode, execution_mode)

        return ProjectSnapshot(
            warehouse_kind=config.warehouse.kind,
            generated_at=datetime.now(timezone.utc),
            sources=sources,
        )

    def estimate_bytes(
        self,
        config: ProjectConfig,
        *,
        mode: CaptureMode = "snapshot",
    ) -> dict[str, int]:
        return self.estimate_bytes_with_execution_mode(config, mode=mode, execution_mode="bounded")

    def estimate_bytes_with_execution_mode(
        self,
        config: ProjectConfig,
        *,
        mode: CaptureMode = "snapshot",
        execution_mode: ExecutionMode = "bounded",
    ) -> dict[str, int]:
        source_names = tuple(sorted(config.sources))
        if len(source_names) <= 1 or self._max_workers == 1:
            return {
                source_name: self._estimate_source(
                    config.sources[source_name],
                    mode,
                    execution_mode,
                )
                for source_name in source_names
            }

        estimated: dict[str, int] = {}
        max_workers = min(self._max_workers, len(source_names))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_name = {
                executor.submit(
                    self._estimate_source,
                    config.sources[source_name],
                    mode,
                    execution_mode,
                ): source_name
                for source_name in source_names
            }
            for future in as_completed(future_to_name):
                source_name = future_to_name[future]
                estimated[source_name] = future.result()

        return {source_name: estimated[source_name] for source_name in source_names}

    def _capture_source(
        self,
        source: SourceContract,
        mode: CaptureMode,
        execution_mode: ExecutionMode,
    ) -> SourceSnapshot:
        request = _build_capture_request(source, mode, execution_mode)
        return self._connector.capture_source(source, request)

    def _estimate_source(
        self,
        source: SourceContract,
        mode: CaptureMode,
        execution_mode: ExecutionMode,
    ) -> int:
        request = _build_capture_request(source, mode, execution_mode)
        return self._connector.estimate_source_bytes(source, request)

    def _capture_sources_parallel(
        self,
        config: ProjectConfig,
        source_names: tuple[str, ...],
        mode: CaptureMode,
        execution_mode: ExecutionMode,
    ) -> dict[str, SourceSnapshot]:
        captured: dict[str, SourceSnapshot] = {}
        max_workers = min(self._max_workers, len(source_names))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_name = {
                executor.submit(
                    self._capture_source,
                    config.sources[source_name],
                    mode,
                    execution_mode,
                ): source_name
                for source_name in source_names
            }
            for future in as_completed(future_to_name):
                source_name = future_to_name[future]
                captured[source_name] = future.result()

        return {source_name: captured[source_name] for source_name in source_names}


def _build_capture_request(
    source: SourceContract,
    mode: CaptureMode,
    execution_mode: ExecutionMode,
) -> SourceCaptureRequest:
    if mode == "test":
        return SourceCaptureRequest.for_test(source, execution_mode=execution_mode)
    if mode == "plan":
        return SourceCaptureRequest.for_plan(source, execution_mode=execution_mode)
    return SourceCaptureRequest.for_snapshot(source, execution_mode=execution_mode)
