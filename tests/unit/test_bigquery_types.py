from __future__ import annotations

import pytest

from verixa.connectors.base import ConnectorError
from verixa.connectors.bigquery.types import parse_table_ref


def test_parse_table_ref_supports_default_project() -> None:
    table_ref = parse_table_ref("raw.stripe_transactions", default_project="demo-project")

    assert table_ref.project == "demo-project"
    assert table_ref.dataset == "raw"
    assert table_ref.table == "stripe_transactions"
    assert table_ref.full_name == "demo-project.raw.stripe_transactions"


def test_parse_table_ref_rejects_missing_project_without_default() -> None:
    with pytest.raises(ConnectorError, match="project.dataset.table"):
        parse_table_ref("raw.stripe_transactions")
