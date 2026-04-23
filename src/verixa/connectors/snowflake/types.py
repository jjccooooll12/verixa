"""Snowflake-specific helper types."""

from __future__ import annotations

from dataclasses import dataclass

from verixa.connectors.base import ConnectorError
from verixa.contracts.normalize import validate_identifier


@dataclass(frozen=True, slots=True)
class SnowflakeTableRef:
    """Parsed Snowflake table identifier."""

    database: str
    schema: str
    table: str

    @property
    def full_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"

    @property
    def quoted_name(self) -> str:
        return ".".join(
            _quote_identifier(part)
            for part in (self.database, self.schema, self.table)
        )

    @property
    def information_schema_name(self) -> str:
        return f'{_quote_identifier(self.database)}.INFORMATION_SCHEMA'


def parse_table_ref(
    raw_table: str,
    *,
    default_database: str | None = None,
    default_schema: str | None = None,
) -> SnowflakeTableRef:
    """Parse Snowflake table references with optional database/schema defaults."""

    normalized = raw_table.strip().strip('"')
    parts = [part.strip() for part in normalized.split(".")]
    if len(parts) == 3:
        database, schema, table = parts
    elif len(parts) == 2 and default_database:
        schema, table = parts
        database = default_database
    elif len(parts) == 1 and default_database and default_schema:
        (table,) = parts
        database = default_database
        schema = default_schema
    else:
        raise ConnectorError(
            "Snowflake tables must be 'database.schema.table', 'schema.table' when "
            "warehouse.database is configured, or 'table' when both warehouse.database "
            "and warehouse.schema are configured."
        )

    resolved_parts = [database, schema, table]
    if not all(resolved_parts):
        raise ConnectorError(f"Invalid Snowflake table reference '{raw_table}'.")
    for part in resolved_parts:
        validate_identifier(part, "Snowflake identifier")
    return SnowflakeTableRef(
        database=database.upper(),
        schema=schema.upper(),
        table=table.upper(),
    )


def quote_column(identifier: str) -> str:
    """Quote a Snowflake column identifier using the MVP's simple-id rules."""

    validate_identifier(identifier, "column name")
    return _quote_identifier(identifier.upper())


def alias_name(prefix: str, identifier: str) -> str:
    """Build a deterministic uppercase alias for one stats field."""

    validate_identifier(identifier, "column name")
    return f"{prefix}__{identifier}".upper()


def _quote_identifier(identifier: str) -> str:
    validate_identifier(identifier, "Snowflake identifier")
    return f'"{identifier}"'
