#!/usr/bin/env python3
"""Provision a small Snowflake fixture set for live Verixa validation."""

from __future__ import annotations

import argparse
import os
import sys

import snowflake.connector


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--connection-name",
        default=os.getenv("VERIXA_SNOWFLAKE_CONNECTION_NAME", "verixa"),
        help="Named Snowflake connection from ~/.snowflake/connections.toml.",
    )
    parser.add_argument("--warehouse", default="VERIXA_WH")
    parser.add_argument("--database", default="VERIXA_DB")
    parser.add_argument("--schema", default="RAW")
    parser.add_argument(
        "--mode",
        choices=("clean", "drift"),
        default="clean",
        help="Whether to load only the clean baseline rows or append drift rows too.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    table_name = f"{args.database}.{args.schema}.STRIPE_TRANSACTIONS"
    date_table_name = f"{args.database}.{args.schema}.ORDERS_BY_DATE"
    datetime_table_name = f"{args.database}.{args.schema}.ORDERS_BY_DATETIME"

    statements = [
        (
            f"CREATE WAREHOUSE IF NOT EXISTS {args.warehouse} "
            "WAREHOUSE_SIZE='XSMALL' AUTO_SUSPEND=60 AUTO_RESUME=TRUE INITIALLY_SUSPENDED=TRUE"
        ),
        f"CREATE DATABASE IF NOT EXISTS {args.database}",
        f"CREATE SCHEMA IF NOT EXISTS {args.database}.{args.schema}",
        (
            f"CREATE OR REPLACE TABLE {table_name} ("
            "AMOUNT FLOAT, CURRENCY VARCHAR, CREATED_AT TIMESTAMP_NTZ)"
        ),
        (
            f"INSERT INTO {table_name} (AMOUNT, CURRENCY, CREATED_AT) VALUES "
            "(10.5, 'USD', DATEADD('minute', -10, CURRENT_TIMESTAMP())), "
            "(20.0, 'EUR', DATEADD('minute', -20, CURRENT_TIMESTAMP())), "
            "(15.25, 'GBP', DATEADD('minute', -30, CURRENT_TIMESTAMP())), "
            "(9.75, 'USD', DATEADD('minute', -40, CURRENT_TIMESTAMP())), "
            "(33.0, 'EUR', DATEADD('minute', -50, CURRENT_TIMESTAMP()))"
        ),
        (
            f"CREATE OR REPLACE TABLE {date_table_name} ("
            "ORDER_ID NUMBER(18,0), ORDER_DATE DATE, COUNTRY VARCHAR)"
        ),
        (
            f"INSERT INTO {date_table_name} (ORDER_ID, ORDER_DATE, COUNTRY) VALUES "
            "(1, CURRENT_DATE(), 'US'), "
            "(2, DATEADD('day', -1, CURRENT_DATE()), 'DE'), "
            "(3, DATEADD('day', -2, CURRENT_DATE()), 'GB')"
        ),
        (
            f"CREATE OR REPLACE TABLE {datetime_table_name} ("
            "ORDER_ID NUMBER(18,0), CREATED_AT DATETIME, STATUS VARCHAR)"
        ),
        (
            f"INSERT INTO {datetime_table_name} (ORDER_ID, CREATED_AT, STATUS) VALUES "
            "(1, DATEADD('minute', -15, CURRENT_TIMESTAMP()::DATETIME), 'ok'), "
            "(2, DATEADD('minute', -30, CURRENT_TIMESTAMP()::DATETIME), 'ok'), "
            "(3, DATEADD('minute', -45, CURRENT_TIMESTAMP()::DATETIME), 'ok')"
        ),
    ]

    if args.mode == "drift":
        statements.append(
            f"INSERT INTO {table_name} (AMOUNT, CURRENCY, CREATED_AT) VALUES "
            "(NULL, 'AUD', CURRENT_TIMESTAMP()), "
            "(45.0, 'USD', CURRENT_TIMESTAMP()), "
            "(18.0, 'CAD', DATEADD('minute', -5, CURRENT_TIMESTAMP()))"
        )

    with snowflake.connector.connect(
        connection_name=args.connection_name,
        application="verixa",
        session_parameters={"TIMEZONE": "UTC", "QUERY_TAG": "verixa-setup"},
    ) as conn:
        cur = conn.cursor()
        try:
            for statement in statements:
                cur.execute(statement)
            row = cur.execute(
                f"SELECT COUNT(*) AS ROW_COUNT FROM {table_name}"
            ).fetchone()
        finally:
            cur.close()

    print(
        f"Loaded Snowflake fixtures into {args.database}.{args.schema} "
        f"(transactions rows={row[0]}, mode={args.mode})."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
