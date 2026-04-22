"""Deterministic JSON encoding/decoding for snapshots."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from verixa.snapshot.models import (
    AcceptedValuesSnapshot,
    FreshnessSnapshot,
    NumericSummarySnapshot,
    ProjectSnapshot,
    SourceSnapshot,
)


def dumps_snapshot(snapshot: ProjectSnapshot) -> str:
    """Serialize a snapshot into stable, sorted JSON."""

    return json.dumps(_snapshot_to_data(snapshot), indent=2, sort_keys=True) + "\n"


def loads_snapshot(raw_json: str) -> ProjectSnapshot:
    """Deserialize a stored project snapshot."""

    data = json.loads(raw_json)
    return _snapshot_from_data(data)


def _snapshot_to_data(snapshot: ProjectSnapshot) -> dict[str, Any]:
    return {
        "warehouse_kind": snapshot.warehouse_kind,
        "generated_at": _format_datetime(snapshot.generated_at),
        "sources": {
            name: {
                "source_name": source.source_name,
                "table": source.table,
                "schema": dict(sorted(source.schema.items())),
                "row_count": source.row_count,
                "null_rates": dict(sorted(source.null_rates.items())),
                "freshness": None
                if source.freshness is None
                else {
                    "column": source.freshness.column,
                    "max_age_seconds": source.freshness.max_age_seconds,
                    "latest_value": _format_datetime(source.freshness.latest_value),
                    "age_seconds": source.freshness.age_seconds,
                },
                "accepted_values": {
                    column: {
                        "column": item.column,
                        "invalid_count": item.invalid_count,
                        "invalid_examples": list(item.invalid_examples),
                    }
                    for column, item in sorted(source.accepted_values.items())
                },
                "numeric_summaries": {
                    column: {
                        "column": item.column,
                        "min_value": item.min_value,
                        "p50_value": item.p50_value,
                        "p95_value": item.p95_value,
                        "max_value": item.max_value,
                        "mean_value": item.mean_value,
                    }
                    for column, item in sorted(source.numeric_summaries.items())
                },
                "captured_at": _format_datetime(source.captured_at),
            }
            for name, source in sorted(snapshot.sources.items())
        },
    }


def _snapshot_from_data(data: dict[str, Any]) -> ProjectSnapshot:
    sources = {
        source_name: SourceSnapshot(
            source_name=payload["source_name"],
            table=payload["table"],
            schema=dict(payload["schema"]),
            row_count=payload.get("row_count"),
            null_rates=dict(payload.get("null_rates", {})),
            freshness=None
            if payload.get("freshness") is None
            else FreshnessSnapshot(
                column=payload["freshness"]["column"],
                max_age_seconds=payload["freshness"]["max_age_seconds"],
                latest_value=_parse_datetime(payload["freshness"].get("latest_value")),
                age_seconds=payload["freshness"].get("age_seconds"),
            ),
            accepted_values={
                column: AcceptedValuesSnapshot(
                    column=item["column"],
                    invalid_count=item["invalid_count"],
                    invalid_examples=tuple(item.get("invalid_examples", [])),
                )
                for column, item in payload.get("accepted_values", {}).items()
            },
            numeric_summaries={
                column: NumericSummarySnapshot(
                    column=item["column"],
                    min_value=item.get("min_value"),
                    p50_value=item.get("p50_value"),
                    p95_value=item.get("p95_value"),
                    max_value=item.get("max_value"),
                    mean_value=item.get("mean_value"),
                )
                for column, item in payload.get("numeric_summaries", {}).items()
            },
            captured_at=_parse_datetime(payload["captured_at"]),
        )
        for source_name, payload in data.get("sources", {}).items()
    }
    return ProjectSnapshot(
        warehouse_kind=data["warehouse_kind"],
        generated_at=_parse_datetime(data["generated_at"]),
        sources=sources,
    )


def _format_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    normalized = value.astimezone(timezone.utc)
    return normalized.isoformat().replace("+00:00", "Z")


def _parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
