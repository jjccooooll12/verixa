"""Implementation of ``verixa explain``."""

from __future__ import annotations

from typing import Any
from pathlib import Path

from verixa.config.loader import load_config
from verixa.contracts.models import AcceptedValuesTest, NoNullsTest


def run_explain(config_path: Path, source_name: str) -> dict[str, Any]:
    config = load_config(config_path, source_names=(source_name,))
    source = config.sources[source_name]

    tests: list[dict[str, Any]] = []
    for test in source.tests:
        if isinstance(test, NoNullsTest):
            tests.append({"kind": "no_nulls", "column": test.column})
        elif isinstance(test, AcceptedValuesTest):
            tests.append(
                {
                    "kind": "accepted_values",
                    "column": test.column,
                    "values": list(test.values),
                }
            )

    return {
        "source_name": source.name,
        "table": source.table,
        "warehouse": {
            "kind": config.warehouse.kind,
            "project": config.warehouse.project,
            "location": config.warehouse.location,
            "max_bytes_billed": config.warehouse.max_bytes_billed,
        },
        "schema": [{"name": name, "type": column_type} for name, column_type in sorted(source.schema.items())],
        "freshness": None
        if source.freshness is None
        else {
            "column": source.freshness.column,
            "max_age": source.freshness.max_age,
        },
        "scan": None
        if source.scan is None
        else {
            "column": source.scan.timestamp_column,
            "column_type": source.scan.column_type,
            "lookback": source.scan.lookback,
        },
        "check": {"fail_on_warning": source.check.fail_on_warning},
        "rules": {
            "null_rate_change": {
                "warning_delta": source.rules.null_rate_change.warning_delta,
                "error_delta": source.rules.null_rate_change.error_delta,
            },
            "row_count_change": {
                "warning_drop_ratio": source.rules.row_count_change.warning_drop_ratio,
                "error_drop_ratio": source.rules.row_count_change.error_drop_ratio,
                "warning_growth_ratio": source.rules.row_count_change.warning_growth_ratio,
                "error_growth_ratio": source.rules.row_count_change.error_growth_ratio,
            },
        },
        "tests": tests,
    }
