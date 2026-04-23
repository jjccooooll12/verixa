"""Warehouse connector selection and display helpers."""

from __future__ import annotations

from verixa.connectors.base import ConnectorError, WarehouseConnector
from verixa.connectors.bigquery.connector import BigQueryConnector
from verixa.connectors.snowflake.connector import SnowflakeConnector
from verixa.contracts.models import WarehouseConfig


def create_connector(
    warehouse: WarehouseConfig,
    *,
    max_bytes_billed: int | None = None,
    query_tag: str | None = None,
) -> WarehouseConnector:
    """Instantiate the connector for one configured warehouse."""

    if warehouse.kind == "bigquery":
        return BigQueryConnector(warehouse, max_bytes_billed=max_bytes_billed)
    if warehouse.kind == "snowflake":
        if max_bytes_billed is not None:
            raise ConnectorError(
                "--max-bytes-billed is currently supported only for BigQuery."
            )
        return SnowflakeConnector(warehouse, query_tag=query_tag)
    raise ConnectorError(f"Unsupported warehouse kind '{warehouse.kind}'.")


def warehouse_label(warehouse: WarehouseConfig) -> str:
    """Render a concise warehouse label for status and diagnostics."""

    if warehouse.kind == "bigquery":
        if warehouse.project:
            return f"bigquery ({warehouse.project})"
        return "bigquery"

    if warehouse.kind == "snowflake":
        if warehouse.connection_name:
            return f"snowflake ({warehouse.connection_name})"
        if warehouse.account:
            return f"snowflake ({warehouse.account})"
        return "snowflake"

    return warehouse.kind
