"""Implementation of ``dataguard init``."""

from __future__ import annotations

from pathlib import Path

from dataguard.storage.filesystem import SnapshotStore

DEFAULT_CONFIG = """warehouse:
  kind: bigquery
  project: your-gcp-project
  location: US

rules:
  null_rate_change:
    warning_delta: 0.02
    error_delta: 0.05
  row_count_change:
    warning_drop_ratio: 0.15
    error_drop_ratio: 0.40
    warning_growth_ratio: 0.25
    error_growth_ratio: 1.50

baseline:
  warning_age: 168h

sources:
  stripe.transactions:
    table: raw.stripe_transactions
    scan:
      timestamp_column: created_at
      lookback: 30d
    check:
      fail_on_warning: false
    rules:
      row_count_change:
        warning_drop_ratio: 0.10
        error_drop_ratio: 0.25
    freshness:
      column: created_at
      max_age: 1h
    schema:
      amount: float
      currency: string
      created_at: timestamp
    tests:
      - no_nulls: amount
      - accepted_values:
          column: currency
          values: [USD, EUR, GBP]
"""

DEFAULT_RISK_CONFIG = """sources:
  stripe.transactions:
    general:
      - likely undercount in recent dashboards
    columns:
      currency:
        - likely to break finance models depending on currency
"""


def init_project(force: bool = False) -> list[Path]:
    """Create starter config files and the local state directory."""

    created_paths: list[Path] = []
    config_path = Path("dataguard.yaml")
    risk_path = Path("dataguard.risk.yaml.example")
    store = SnapshotStore()

    for path in (config_path, risk_path):
        if path.exists() and not force:
            raise FileExistsError(
                f"'{path}' already exists. Use --force to overwrite starter files."
            )

    store.ensure_directory()

    config_path.write_text(DEFAULT_CONFIG, encoding="utf-8")
    risk_path.write_text(DEFAULT_RISK_CONFIG, encoding="utf-8")
    created_paths.extend([config_path, risk_path, store.baseline_path.parent])
    return created_paths
