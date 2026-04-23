from __future__ import annotations

from pathlib import Path

from verixa.cli.doctor import run_doctor
from verixa.connectors.snowflake.connector import SnowflakeConnector, SnowflakeSessionContext
from verixa.contracts.models import SourceContract, WarehouseConfig


class _FakeSnowflakeConnector(SnowflakeConnector):
    def __init__(self, warehouse, *, auth_ok=True, auth_message="authenticated", context=None, source_error=None):  # noqa: ANN001
        super().__init__(warehouse)
        self._auth_ok = auth_ok
        self._auth_message = auth_message
        self._context = context
        self._source_error = source_error

    def check_auth(self) -> tuple[bool, str]:
        return self._auth_ok, self._auth_message

    def describe_runtime_environment(self) -> SnowflakeSessionContext:
        if isinstance(self._context, Exception):
            raise self._context
        assert self._context is not None
        return self._context

    def check_source_access(self, source: SourceContract) -> tuple[bool, str]:
        if self._source_error is not None:
            raise self._source_error
        return True, source.table

    def capture_source(self, source, capture_request):  # noqa: ANN001
        raise NotImplementedError

    def estimate_source_bytes(self, source, capture_request):  # noqa: ANN001
        raise NotImplementedError


class _FakeBigQueryConnector:
    def check_auth(self) -> tuple[bool, str]:
        return True, "authenticated"

    def check_source_access(self, source: SourceContract) -> tuple[bool, str]:
        return True, source.table


def test_run_doctor_reports_invalid_environment_scoped_baseline_path(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
  project: demo
baseline:
  path: .verixa/{environment}/baseline.json
sources:
  stripe.transactions:
    table: raw.stripe_transactions
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    result = run_doctor(config_path)

    assert result.error_count == 1
    assert result.findings[0].code == "baseline_path_invalid"
    assert "requires an environment" in result.findings[0].message


def test_run_doctor_reports_missing_environment_baseline(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: bigquery
  project: demo
baseline:
  path: .verixa/{environment}/baseline.json
sources:
  stripe.transactions:
    table: raw.stripe_transactions
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    result = run_doctor(config_path, environment="prod", connector_factory=lambda *args, **kwargs: _FakeBigQueryConnector())

    assert result.error_count == 1
    assert result.findings[0].code == "baseline_missing_for_environment"
    assert "environment 'prod'" in result.findings[0].message


def test_run_doctor_reports_snowflake_runtime_mismatches(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: snowflake
  connection_name: verixa
  warehouse: VERIXA_WH
  database: VERIXA_DB
  schema: RAW
  role: VERIXA_ROLE
sources:
  stripe.transactions:
    table: RAW.INGEST.ORDERS
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    context = SnowflakeSessionContext(
        current_role="ACCOUNTADMIN",
        current_warehouse="OTHER_WH",
        current_database="OTHER_DB",
        current_schema="STAGING",
        compute_ok=False,
        compute_message="warehouse suspended",
    )

    def _connector_factory(warehouse, *, max_bytes_billed=None, query_tag=None):  # noqa: ANN001
        return _FakeSnowflakeConnector(warehouse, context=context)

    result = run_doctor(config_path, connector_factory=_connector_factory)

    codes = {finding.code for finding in result.findings}
    assert "snowflake_warehouse_mismatch" in codes
    assert "snowflake_role_mismatch" in codes
    assert "snowflake_database_mismatch" in codes
    assert "snowflake_schema_mismatch" in codes
    assert "snowflake_warehouse_unusable" in codes


def test_run_doctor_wraps_source_access_exceptions(tmp_path: Path) -> None:
    config_path = tmp_path / "verixa.yaml"
    config_path.write_text(
        """
warehouse:
  kind: snowflake
  connection_name: verixa
sources:
  stripe.transactions:
    table: RAW.INGEST.ORDERS
    schema:
      amount: float
""".strip()
        + "\n",
        encoding="utf-8",
    )

    context = SnowflakeSessionContext(
        current_role="ACCOUNTADMIN",
        current_warehouse="VERIXA_WH",
        current_database=None,
        current_schema=None,
        compute_ok=True,
        compute_message=None,
    )

    def _connector_factory(warehouse, *, max_bytes_billed=None, query_tag=None):  # noqa: ANN001
        return _FakeSnowflakeConnector(
            warehouse,
            context=context,
            source_error=RuntimeError("permission denied"),
        )

    result = run_doctor(config_path, connector_factory=_connector_factory)

    assert any(finding.code == "source_unreachable" for finding in result.findings)
    assert any("permission denied" in finding.message for finding in result.findings)
