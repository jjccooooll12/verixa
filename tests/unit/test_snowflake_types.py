from __future__ import annotations

import pytest

from verixa.connectors.base import ConnectorError
from verixa.connectors.snowflake.types import parse_table_ref


def test_parse_table_ref_accepts_fully_qualified_snowflake_table() -> None:
    table_ref = parse_table_ref("raw.ingest.orders")

    assert table_ref.database == "RAW"
    assert table_ref.schema == "INGEST"
    assert table_ref.table == "ORDERS"
    assert table_ref.full_name == "RAW.INGEST.ORDERS"
    assert table_ref.quoted_name == '"RAW"."INGEST"."ORDERS"'


def test_parse_table_ref_accepts_schema_table_with_default_database() -> None:
    table_ref = parse_table_ref("ingest.orders", default_database="raw")

    assert table_ref.database == "RAW"
    assert table_ref.schema == "INGEST"
    assert table_ref.table == "ORDERS"


def test_parse_table_ref_accepts_table_with_default_database_and_schema() -> None:
    table_ref = parse_table_ref(
        "orders",
        default_database="raw",
        default_schema="ingest",
    )

    assert table_ref.database == "RAW"
    assert table_ref.schema == "INGEST"
    assert table_ref.table == "ORDERS"


def test_parse_table_ref_requires_defaults_for_shorter_names() -> None:
    with pytest.raises(ConnectorError, match="Snowflake tables must be"):
        parse_table_ref("ingest.orders")
