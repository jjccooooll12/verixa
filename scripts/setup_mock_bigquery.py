"""Create a small mock BigQuery dataset and table for DataGuard demos."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from google.cloud import bigquery


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="dataguard-494111", help="GCP project ID")
    parser.add_argument("--dataset", default="dataguard_demo", help="BigQuery dataset ID")
    parser.add_argument(
        "--table",
        default="stripe_transactions",
        help="BigQuery table ID inside the dataset",
    )
    parser.add_argument(
        "--location",
        default="US",
        help="BigQuery location for the dataset",
    )
    parser.add_argument(
        "--data-file",
        default="examples/mock_data/stripe_transactions.jsonl",
        help="Path to newline-delimited JSON rows",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client = bigquery.Client(project=args.project)

    dataset_ref = f"{args.project}.{args.dataset}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = args.location
    client.create_dataset(dataset, exists_ok=True)

    table_ref = f"{dataset_ref}.{args.table}"
    schema = [
        bigquery.SchemaField("transaction_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("amount", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
    ]
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table, exists_ok=True)

    data_path = Path(args.data_file)
    rows = [json.loads(line) for line in data_path.read_text(encoding="utf-8").splitlines() if line]
    client.delete_table(table_ref, not_found_ok=True)
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
    )
    load_job = client.load_table_from_json(rows, table_ref, job_config=job_config)
    load_job.result()

    created_table = client.get_table(table_ref)
    print(f"Created dataset: {dataset_ref}")
    print(f"Loaded table: {table_ref}")
    print(f"Rows: {created_table.num_rows}")


if __name__ == "__main__":
    main()
