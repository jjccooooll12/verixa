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
  numeric_distribution_change:
    warning_relative_delta: 0.3
    error_relative_delta: 0.8
    minimum_baseline_value: 2.5
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
    assert config.rules.numeric_distribution_change.warning_relative_delta == 0.3
    assert config.rules.numeric_distribution_change.error_relative_delta == 0.8
    assert config.rules.numeric_distribution_change.minimum_baseline_value == 2.5


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
      numeric_distribution_change:
        warning_relative_delta: 0.4
        minimum_baseline_value: 5.0
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
    assert source.rules.numeric_distribution_change.warning_relative_delta == 0.4
    assert source.rules.numeric_distribution_change.error_relative_delta == 0.5
    assert source.rules.numeric_distribution_change.minimum_baseline_value == 5.0


def test_load_config_parses_scan_and_baseline_settings(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
  project: demo-project
baseline:
  warning_age: 72h
  path: .verixa/{environment}/baseline.json
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
    assert config.baseline.path == ".verixa/{environment}/baseline.json"
    assert source.scan is not None
    assert source.scan.timestamp_column == "created_at"
    assert source.scan.column_type == "TIMESTAMP"
    assert source.scan.lookback_seconds == 14 * 24 * 3600


def test_load_config_parses_history_settings(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
  project: demo-project
sources:
  stripe.transactions:
    table: raw.stripe_transactions
    history:
      window: 7
      minimum_snapshots: 4
      row_count: true
      null_rate: false
      numeric_distribution: true
      backfill_mode: true
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path)
    history = config.sources["stripe.transactions"].history

    assert history is not None
    assert history.window == 7
    assert history.minimum_snapshots == 4
    assert history.row_count is True
    assert history.null_rate is False
    assert history.numeric_distribution is True
    assert history.backfill_mode is True


def test_load_config_parses_warehouse_max_bytes_billed(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
  project: demo-project
  max_bytes_billed: 500MB
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

    assert config.warehouse.max_bytes_billed == 500 * 1024 * 1024


def test_load_config_parses_source_severity_overrides(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
  project: demo-project
sources:
  stripe.transactions:
    table: raw.stripe_transactions
    severity_overrides:
      drift.row_count_changed: error
      baseline.missing_for_source: info
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.sources["stripe.transactions"].severity_overrides == {
        "baseline.missing_for_source": "info",
        "drift.row_count_changed": "error",
    }


def test_load_config_rejects_invalid_warehouse_max_bytes_billed(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
  project: demo-project
  max_bytes_billed: nope
sources:
  stripe.transactions:
    table: raw.stripe_transactions
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="Unsupported byte size"):
        load_config(config_path)


def test_load_config_parses_snowflake_connection_settings(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: snowflake
  account: xy12345.us-east-1
  user: analyst
  password_env: VERIXA_SNOWFLAKE_PASSWORD
  warehouse_name: ANALYTICS
  database: RAW
  schema: INGEST
  role: TRANSFORMER
  authenticator: externalbrowser
sources:
  stripe.transactions:
    table: ingest.orders
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.warehouse.kind == "snowflake"
    assert config.warehouse.account == "xy12345.us-east-1"
    assert config.warehouse.user == "analyst"
    assert config.warehouse.password_env == "VERIXA_SNOWFLAKE_PASSWORD"
    assert config.warehouse.warehouse_name == "ANALYTICS"
    assert config.warehouse.database == "RAW"
    assert config.warehouse.schema == "INGEST"
    assert config.warehouse.role == "TRANSFORMER"
    assert config.warehouse.authenticator == "externalbrowser"


def test_load_config_parses_snowflake_connection_name(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: snowflake
  connection_name: team-dev
sources:
  stripe.transactions:
    table: RAW.INGEST.ORDERS
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.warehouse.kind == "snowflake"
    assert config.warehouse.connection_name == "team-dev"
    assert config.warehouse.account is None
    assert config.warehouse.user is None


def test_load_config_rejects_invalid_snowflake_config_without_connection_info(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: snowflake
  account: xy12345.us-east-1
sources:
  stripe.transactions:
    table: RAW.INGEST.ORDERS
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="warehouse.connection_name or both"):
        load_config(config_path)


def test_load_config_rejects_max_bytes_billed_for_snowflake(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: snowflake
  connection_name: team-dev
  max_bytes_billed: 1GB
sources:
  stripe.transactions:
    table: RAW.INGEST.ORDERS
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="supported only for BigQuery"):
        load_config(config_path)


def test_load_config_rejects_empty_baseline_path(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
baseline:
  path: ""
sources:
  stripe.transactions:
    table: raw.stripe_transactions
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="baseline.path must be a non-empty string"):
        load_config(config_path)


def test_load_config_parses_source_check_policy(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
  project: demo-project
check:
  fail_on_warning: false
  advisory: false
sources:
  stripe.transactions:
    table: raw.stripe_transactions
    check:
      fail_on_warning: true
      advisory: true
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.check.fail_on_warning is False
    assert config.check.advisory is False
    assert config.sources["stripe.transactions"].check.fail_on_warning is True
    assert config.sources["stripe.transactions"].check.advisory is True


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


def test_load_config_parses_extension_hooks(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
  project: demo-project
extensions:
  checks:
    - tests.unit.extensions_demo:custom_check
  finding_enrichers:
    - tests.unit.extensions_demo:finding_enricher
  source_metadata_enrichers:
    - tests.unit.extensions_demo:source_metadata_enricher
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

    assert len(config.extensions.checks) == 1
    assert len(config.extensions.finding_enrichers) == 1
    assert len(config.extensions.source_metadata_enrichers) == 1


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
