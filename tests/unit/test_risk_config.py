from __future__ import annotations

from pathlib import Path

import pytest

from verixa.contracts.models import ExtensionsConfig, ProjectConfig, SourceContract, WarehouseConfig
from verixa.diff.risk import RiskConfig, SourceRiskHints, enrich_risk_config_with_dbt_impacts
from verixa.extensions.api import ExtensionError


def _project_config() -> ProjectConfig:
    return ProjectConfig(
        warehouse=WarehouseConfig(kind="bigquery", project="demo"),
        sources={
            "stripe.transactions": SourceContract(
                name="stripe.transactions",
                table="raw.stripe_transactions",
                schema={"amount": "FLOAT64"},
                freshness=None,
                tests=(),
            ),
        },
    )


def test_enrich_risk_config_with_dbt_impacts_merges_metadata_without_overriding_explicit_values(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "target" / "manifest.json"
    manifest_path.parent.mkdir()
    manifest_path.write_text(
        """
{
  "nodes": {
    "model.demo.stg_orders": {
      "resource_type": "model",
      "name": "stg_orders",
      "depends_on": {
        "nodes": ["source.demo.raw.stripe_transactions"],
        "macros": []
      }
    }
  },
  "sources": {
    "source.demo.raw.stripe_transactions": {
      "database": "demo",
      "schema": "raw",
      "identifier": "stripe_transactions",
      "name": "stripe_transactions",
      "meta": {
        "verixa": {
          "owners": ["finance", "analytics"],
          "criticality": "high"
        }
      }
    }
  },
  "macros": {}
}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    targets_path = tmp_path / "verixa.targets.yaml"
    targets_path.write_text(
        """
dbt:
  manifest_path: target/manifest.json
""".strip()
        + "\n",
        encoding="utf-8",
    )
    risk_config = RiskConfig(
        sources={
            "stripe.transactions": SourceRiskHints(
                general=("manual risk",),
                columns={},
                owners=("data-platform",),
                criticality="medium",
                downstream_models=("manual_model",),
            )
        }
    )

    enriched = enrich_risk_config_with_dbt_impacts(
        risk_config,
        config=_project_config(),
        targets_path=targets_path,
    )

    assert enriched is not None
    hints = enriched.sources["stripe.transactions"]
    assert hints.general == ("manual risk",)
    assert hints.owners == ("data-platform", "finance", "analytics")
    assert hints.criticality == "medium"
    assert hints.downstream_models == ("manual_model", "stg_orders")


def test_enrich_risk_config_with_dbt_impacts_creates_hints_when_only_dbt_metadata_exists(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "target" / "manifest.json"
    manifest_path.parent.mkdir()
    manifest_path.write_text(
        """
{
  "nodes": {},
  "sources": {
    "source.demo.raw.stripe_transactions": {
      "database": "demo",
      "schema": "raw",
      "identifier": "stripe_transactions",
      "name": "stripe_transactions",
      "meta": {
        "owners": ["finance"],
        "criticality": "high"
      }
    }
  },
  "macros": {}
}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    targets_path = tmp_path / "verixa.targets.yaml"
    targets_path.write_text(
        """
dbt:
  manifest_path: target/manifest.json
""".strip()
        + "\n",
        encoding="utf-8",
    )

    enriched = enrich_risk_config_with_dbt_impacts(
        None,
        config=_project_config(),
        targets_path=targets_path,
    )

    assert enriched is not None
    hints = enriched.sources["stripe.transactions"]
    assert hints.general == ()
    assert hints.columns == {}
    assert hints.owners == ("finance",)
    assert hints.criticality == "high"
    assert hints.downstream_models == ()


def test_enrich_risk_config_with_extension_metadata_backfills_missing_values() -> None:
    from tests.unit.extensions_demo import source_metadata_enricher

    config = ProjectConfig(
        warehouse=WarehouseConfig(kind="bigquery", project="demo"),
        sources=_project_config().sources,
        extensions=ExtensionsConfig(source_metadata_enrichers=(source_metadata_enricher,)),
    )

    enriched = enrich_risk_config_with_dbt_impacts(
        None,
        config=config,
        targets_path=None,
    )

    assert enriched is not None
    hints = enriched.sources["stripe.transactions"]
    assert hints.general == ("extension-general-risk",)
    assert hints.columns == {"amount": ("extension-column-risk",)}
    assert hints.owners == ("extension-owner",)
    assert hints.criticality == "medium"
    assert hints.downstream_models == ("ext_model",)


def test_enrich_risk_config_with_extension_metadata_does_not_override_explicit_criticality() -> None:
    from tests.unit.extensions_demo import source_metadata_enricher

    risk_config = RiskConfig(
        sources={
            "stripe.transactions": SourceRiskHints(
                general=(),
                columns={},
                owners=("data-platform",),
                criticality="high",
                downstream_models=(),
            )
        }
    )
    config = ProjectConfig(
        warehouse=WarehouseConfig(kind="bigquery", project="demo"),
        sources=_project_config().sources,
        extensions=ExtensionsConfig(source_metadata_enrichers=(source_metadata_enricher,)),
    )

    enriched = enrich_risk_config_with_dbt_impacts(
        risk_config,
        config=config,
        targets_path=None,
    )

    assert enriched is not None
    hints = enriched.sources["stripe.transactions"]
    assert hints.owners == ("data-platform", "extension-owner")
    assert hints.criticality == "high"


def test_enrich_risk_config_raises_extension_error_for_invalid_source_metadata_hook() -> None:
    from tests.unit.extensions_demo import invalid_source_metadata_enricher

    config = ProjectConfig(
        warehouse=WarehouseConfig(kind="bigquery", project="demo"),
        sources=_project_config().sources,
        extensions=ExtensionsConfig(
            source_metadata_enrichers=(invalid_source_metadata_enricher,)
        ),
    )

    with pytest.raises(ExtensionError, match="returned a non-SourceRiskHints value"):
        enrich_risk_config_with_dbt_impacts(
            None,
            config=config,
            targets_path=None,
        )
