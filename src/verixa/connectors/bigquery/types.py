"""BigQuery-specific helper types."""

from __future__ import annotations

from dataclasses import dataclass

from verixa.connectors.base import ConnectorError


@dataclass(frozen=True, slots=True)
class BigQueryTableRef:
    """Parsed BigQuery table identifier."""

    project: str
    dataset: str
    table: str

    @property
    def full_name(self) -> str:
        return f"{self.project}.{self.dataset}.{self.table}"


def parse_table_ref(raw_table: str, default_project: str | None = None) -> BigQueryTableRef:
    """Parse either ``project.dataset.table`` or ``dataset.table`` references."""

    normalized = raw_table.strip().strip("`")
    parts = normalized.split(".")
    if len(parts) == 3:
        project, dataset, table = parts
    elif len(parts) == 2 and default_project:
        dataset, table = parts
        project = default_project
    else:
        raise ConnectorError(
            "BigQuery tables must be 'project.dataset.table' or 'dataset.table' when "
            "warehouse.project is configured."
        )

    if not all(parts):
        raise ConnectorError(f"Invalid BigQuery table reference '{raw_table}'.")
    return BigQueryTableRef(project=project, dataset=dataset, table=table)
