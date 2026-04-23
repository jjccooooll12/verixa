"""Filesystem-backed run history for lifecycle-aware outputs."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from verixa.findings.schema import NormalizedFinding
from verixa.history.models import FindingRunRecord, SnapshotRunRecord
from verixa.snapshot.models import ProjectSnapshot
from verixa.storage.json_codec import dumps_snapshot, loads_snapshot


class HistoryStoreError(RuntimeError):
    """Raised when run history cannot be read or written."""


class RunHistoryStore:
    """Persist the latest normalized findings per command and environment."""

    def __init__(self, root: Path | None = None) -> None:
        self._root = root or Path(".verixa") / "history"

    def history_path(self, command: str, *, environment: str | None = None) -> Path:
        env_name = environment or "default"
        return self._root / env_name / f"{command}.json"

    def read_last_run(
        self,
        command: str,
        *,
        environment: str | None = None,
    ) -> FindingRunRecord | None:
        path = self.history_path(command, environment=environment)
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:  # pragma: no cover - covered through error path indirectly
            raise HistoryStoreError(f"Failed to read run history '{path}': {exc}") from exc
        return _record_from_data(payload)

    def write_run(
        self,
        command: str,
        findings: tuple[NormalizedFinding, ...],
        *,
        environment: str | None = None,
        generated_at: datetime | None = None,
    ) -> Path:
        path = self.history_path(command, environment=environment)
        path.parent.mkdir(parents=True, exist_ok=True)
        record = FindingRunRecord(
            schema_version="verixa.run_history.v1",
            command=command,
            environment=environment,
            generated_at=generated_at or datetime.now(timezone.utc),
            findings=tuple(
                replace(finding, lifecycle_status="new") for finding in findings
            ),
        )
        path.write_text(json.dumps(_record_to_data(record), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return path


class SnapshotHistoryStore:
    """Persist captured project snapshots for history-aware drift checks."""

    def __init__(self, root: Path | None = None) -> None:
        self._root = root or Path(".verixa") / "history" / "snapshots"

    def environment_dir(self, *, environment: str | None = None) -> Path:
        return self._root / (environment or "default")

    def write_run(
        self,
        command: str,
        snapshot: ProjectSnapshot,
        *,
        environment: str | None = None,
        execution_mode: str = "bounded",
    ) -> Path:
        path = self.environment_dir(environment=environment) / _snapshot_filename(
            snapshot.generated_at,
            command,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        record = SnapshotRunRecord(
            schema_version="verixa.snapshot_history.v1",
            command=command,
            environment=environment,
            execution_mode=execution_mode,
            generated_at=snapshot.generated_at,
            snapshot=snapshot,
        )
        path.write_text(
            json.dumps(_snapshot_record_to_data(record), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return path

    def list_runs(
        self,
        *,
        environment: str | None = None,
        limit: int | None = None,
    ) -> tuple[SnapshotRunRecord, ...]:
        directory = self.environment_dir(environment=environment)
        if not directory.exists():
            return ()
        records: list[SnapshotRunRecord] = []
        for path in sorted(directory.glob("*.json"), reverse=True):
            payload = json.loads(path.read_text(encoding="utf-8"))
            records.append(_snapshot_record_from_data(payload))
            if limit is not None and len(records) >= limit:
                break
        records.reverse()
        return tuple(records)


def _record_to_data(record: FindingRunRecord) -> dict[str, Any]:
    return {
        "schema_version": record.schema_version,
        "command": record.command,
        "environment": record.environment,
        "generated_at": record.generated_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "findings": [finding.as_dict() for finding in record.findings],
    }


def _record_from_data(payload: dict[str, Any]) -> FindingRunRecord:
    findings = tuple(
        NormalizedFinding(
            schema_version=item["schema_version"],
            fingerprint=item["fingerprint"],
            source_name=item["source_name"],
            severity=item["severity"],
            code=item["code"],
            stable_code=item.get("stable_code", item["code"]),
            message=item["message"],
            category=item["category"],
            change_type=item["change_type"],
            baseline_status=item["baseline_status"],
            confidence=item["confidence"],
            confidence_reason=item.get("confidence_reason"),
            lifecycle_status=item.get("lifecycle_status", "new"),
            remediation=item["remediation"],
            column=item.get("column"),
            risks=tuple(item.get("risks", ())),
            owners=tuple(item.get("owners", ())),
            source_criticality=item.get("source_criticality"),
            downstream_models=tuple(item.get("downstream_models", ())),
            estimated_bytes_processed=item.get("estimated_bytes_processed"),
            history_metric=item.get("history_metric"),
            history_window=item.get("history_window"),
            history_sample_size=item.get("history_sample_size"),
            history_center_value=item.get("history_center_value"),
            history_lower_bound=item.get("history_lower_bound"),
            history_upper_bound=item.get("history_upper_bound"),
        )
        for item in payload.get("findings", [])
    )
    return FindingRunRecord(
        schema_version=payload["schema_version"],
        command=payload["command"],
        environment=payload.get("environment"),
        generated_at=datetime.fromisoformat(payload["generated_at"].replace("Z", "+00:00")),
        findings=findings,
    )


def _snapshot_filename(generated_at: datetime, command: str) -> str:
    return generated_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + f"-{command}.json"


def _snapshot_record_to_data(record: SnapshotRunRecord) -> dict[str, Any]:
    return {
        "schema_version": record.schema_version,
        "command": record.command,
        "environment": record.environment,
        "execution_mode": record.execution_mode,
        "generated_at": record.generated_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "snapshot": json.loads(dumps_snapshot(record.snapshot)),
    }


def _snapshot_record_from_data(payload: dict[str, Any]) -> SnapshotRunRecord:
    return SnapshotRunRecord(
        schema_version=payload["schema_version"],
        command=payload["command"],
        environment=payload.get("environment"),
        execution_mode=payload.get("execution_mode", "bounded"),
        generated_at=datetime.fromisoformat(payload["generated_at"].replace("Z", "+00:00")),
        snapshot=loads_snapshot(json.dumps(payload["snapshot"])),
    )
