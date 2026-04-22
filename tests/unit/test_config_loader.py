from __future__ import annotations

from pathlib import Path

import pytest

from verixa.config.errors import ConfigError
from verixa.config.loader import load_config


def test_load_config_supports_schema_list_shorthand(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
  project: demo-project
  location: US
sources:
  stripe.transactions:
    table: raw.stripe_transactions
    freshness:
      column: created_at
      max_age: 1h
    schema:
      - amount: float
      - currency: string
      - created_at: timestamp
    tests:
      - no_nulls: amount
      - accepted_values:
          column: currency
          values: [USD, EUR]
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path)

    source = config.sources["stripe.transactions"]
    assert config.warehouse.kind == "bigquery"
    assert source.schema == {
        "amount": "FLOAT64",
        "created_at": "TIMESTAMP",
        "currency": "STRING",
    }
    assert source.freshness is not None
    assert source.freshness.max_age_seconds == 3600
    assert source.no_null_columns == ("amount",)
    assert source.accepted_values_tests[0].values == ("USD", "EUR")


def test_load_config_rejects_freshness_string_shorthand(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
sources:
  stripe.transactions:
    table: raw.stripe_transactions
    freshness: <1h
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ConfigError,
        match=r"requires 'freshness: \{column: .* max_age: .*\}'",
    ):
        load_config(config_path)


def test_load_config_parses_rule_thresholds(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
  project: demo-project
rules:
  null_rate_change:
    warning_delta: 0.02
    error_delta: 0.07
  row_count_change:
    warning_drop_ratio: 0.15
    error_drop_ratio: 0.4
    warning_growth_ratio: 0.25
    error_growth_ratio: 1.5
sources:
  stripe.transactions:
    table: raw.stripe_transactions
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.rules.null_rate_change.warning_delta == 0.02
    assert config.rules.null_rate_change.error_delta == 0.07
    assert config.rules.row_count_change.warning_drop_ratio == 0.15
    assert config.rules.row_count_change.error_drop_ratio == 0.4
    assert config.rules.row_count_change.warning_growth_ratio == 0.25
    assert config.rules.row_count_change.error_growth_ratio == 1.5


def test_load_config_allows_source_level_rule_overrides(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
  project: demo-project
rules:
  null_rate_change:
    warning_delta: 0.02
    error_delta: 0.07
sources:
  stripe.transactions:
    table: raw.stripe_transactions
    rules:
      row_count_change:
        warning_drop_ratio: 0.10
        error_drop_ratio: 0.25
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path)
    source = config.sources["stripe.transactions"]

    assert source.rules.null_rate_change.warning_delta == 0.02
    assert source.rules.null_rate_change.error_delta == 0.07
    assert source.rules.row_count_change.warning_drop_ratio == 0.10
    assert source.rules.row_count_change.error_drop_ratio == 0.25
    assert source.rules.row_count_change.warning_growth_ratio == 0.20
    assert source.rules.row_count_change.error_growth_ratio == 1.0


def test_load_config_parses_scan_and_baseline_settings(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
  project: demo-project
baseline:
  warning_age: 72h
sources:
  stripe.transactions:
    table: raw.stripe_transactions
    scan:
      timestamp_column: created_at
      lookback: 14d
    schema:
      amount: float
      created_at: timestamp
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path)
    source = config.sources["stripe.transactions"]

    assert config.baseline.warning_age == "72h"
    assert config.baseline.warning_age_seconds == 72 * 3600
    assert source.scan is not None
    assert source.scan.timestamp_column == "created_at"
    assert source.scan.column_type == "TIMESTAMP"
    assert source.scan.lookback_seconds == 14 * 24 * 3600


def test_load_config_parses_source_check_policy(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
  project: demo-project
check:
  fail_on_warning: false
sources:
  stripe.transactions:
    table: raw.stripe_transactions
    check:
      fail_on_warning: true
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.check.fail_on_warning is False
    assert config.sources["stripe.transactions"].check.fail_on_warning is True


def test_load_config_accepts_date_scan_columns(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
  project: demo-project
sources:
  stripe.transactions:
    table: raw.stripe_transactions
    scan:
      timestamp_column: created_date
      lookback: 2d
    schema:
      amount: float
      created_date: date
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.sources["stripe.transactions"].scan is not None
    assert config.sources["stripe.transactions"].scan.column_type == "DATE"


def test_load_config_can_select_sources(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
  project: demo-project
sources:
  payments.transactions:
    table: raw.payments_transactions
    schema:
      amount: float
  stripe.transactions:
    table: raw.stripe_transactions
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path, source_names=("stripe.transactions",))

    assert tuple(config.sources) == ("stripe.transactions",)


def test_load_config_rejects_unknown_source_selection(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
  project: demo-project
sources:
  stripe.transactions:
    table: raw.stripe_transactions
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="Unknown source selection"):
        load_config(config_path, source_names=("missing.source",))
