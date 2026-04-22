from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from verixa.config.errors import ConfigError
from verixa.contracts.models import ProjectConfig, SourceContract, WarehouseConfig
from verixa.targeting import load_targets_config, list_changed_files_against, resolve_source_names


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
            "stripe.customers": SourceContract(
                name="stripe.customers",
                table="raw.stripe_customers",
                schema={"id": "STRING"},
                freshness=None,
                tests=(),
            ),
        },
    )


def test_load_targets_config_parses_paths_mapping(tmp_path: Path) -> None:
    targets_path = tmp_path / "verixa.targets.yaml"
    targets_path.write_text(
        """
paths:
  models/staging/stripe/**/*.sql: stripe.transactions
  macros/shared/:
    - stripe.transactions
    - stripe.customers
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_targets_config(targets_path)

    assert config is not None
    assert config.paths["models/staging/stripe/**/*.sql"] == ("stripe.transactions",)
    assert config.paths["macros/shared/"] == (
        "stripe.transactions",
        "stripe.customers",
    )


def test_load_targets_config_parses_optional_dbt_manifest_path(tmp_path: Path) -> None:
    targets_path = tmp_path / "verixa.targets.yaml"
    targets_path.write_text(
        """
dbt:
  manifest_path: target/manifest.json
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_targets_config(targets_path)

    assert config is not None
    assert config.paths == {}
    assert config.dbt_manifest_path == (tmp_path / "target" / "manifest.json").resolve()


def test_resolve_source_names_prefers_explicit_sources() -> None:
    resolved = resolve_source_names(
        Path("verixa.yaml"),
        explicit_source_names=("stripe.transactions", "stripe.transactions"),
        config_loader=lambda path: (_ for _ in ()).throw(AssertionError("should not load config")),
        targets_loader=lambda path: (_ for _ in ()).throw(AssertionError("should not load targets")),
    )

    assert resolved == ("stripe.transactions",)


def test_resolve_source_names_maps_changed_files_to_matching_sources(tmp_path: Path) -> None:
    targets_path = tmp_path / "verixa.targets.yaml"
    targets_path.write_text(
        """
paths:
  models/staging/stripe/**/*.sql:
    - stripe.transactions
  macros/shared/:
    - stripe.transactions
    - stripe.customers
""".strip()
        + "\n",
        encoding="utf-8",
    )

    resolved = resolve_source_names(
        tmp_path / "verixa.yaml",
        changed_files=("models/staging/stripe/orders.sql", "macros/shared/currency.sql"),
        targets_path=targets_path,
        config_loader=lambda path: _project_config(),
    )

    assert resolved == ("stripe.transactions", "stripe.customers")


def test_resolve_source_names_returns_empty_selection_when_no_patterns_match(tmp_path: Path) -> None:
    targets_path = tmp_path / "verixa.targets.yaml"
    targets_path.write_text(
        """
paths:
  models/staging/stripe/**/*.sql:
    - stripe.transactions
""".strip()
        + "\n",
        encoding="utf-8",
    )

    resolved = resolve_source_names(
        tmp_path / "verixa.yaml",
        changed_files=("docs/readme.md",),
        targets_path=targets_path,
        config_loader=lambda path: _project_config(),
    )

    assert resolved == ()


def test_resolve_source_names_requires_targets_file_when_requested(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="Changed-file targeting requested"):
        resolve_source_names(
            tmp_path / "verixa.yaml",
            changed_files=("models/staging/stripe/orders.sql",),
            targets_path=tmp_path / "missing.targets.yaml",
            config_loader=lambda path: _project_config(),
        )


def test_resolve_source_names_rejects_unknown_target_sources(tmp_path: Path) -> None:
    targets_path = tmp_path / "verixa.targets.yaml"
    targets_path.write_text(
        """
paths:
  models/staging/stripe/**/*.sql:
    - missing.source
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="unknown sources"):
        resolve_source_names(
            tmp_path / "verixa.yaml",
            changed_files=("models/staging/stripe/orders.sql",),
            targets_path=targets_path,
            config_loader=lambda path: _project_config(),
        )


def test_resolve_source_names_can_use_changed_against_provider(tmp_path: Path) -> None:
    targets_path = tmp_path / "verixa.targets.yaml"
    targets_path.write_text(
        """
paths:
  models/staging/stripe/**/*.sql:
    - stripe.transactions
""".strip()
        + "\n",
        encoding="utf-8",
    )
    seen: dict[str, object] = {}

    def _provider(base_ref: str, *, cwd: Path) -> tuple[str, ...]:
        seen["base_ref"] = base_ref
        seen["cwd"] = cwd
        return ("models/staging/stripe/orders.sql",)

    resolved = resolve_source_names(
        tmp_path / "nested" / "verixa.yaml",
        changed_against="origin/main",
        targets_path=targets_path,
        config_loader=lambda path: _project_config(),
        changed_file_provider=_provider,
    )

    assert resolved == ("stripe.transactions",)
    assert seen["base_ref"] == "origin/main"
    assert seen["cwd"] == tmp_path / "nested"


def test_resolve_source_names_can_map_dbt_model_changes_to_sources(tmp_path: Path) -> None:
    manifest_path = tmp_path / "target" / "manifest.json"
    manifest_path.parent.mkdir()
    manifest_path.write_text(
        """
{
  "nodes": {
    "model.demo.stg_orders": {
      "resource_type": "model",
      "original_file_path": "models/staging/orders.sql",
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
      "original_file_path": "models/sources/stripe.yml",
      "depends_on": {
        "nodes": []
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

    resolved = resolve_source_names(
        tmp_path / "verixa.yaml",
        changed_files=("models/staging/orders.sql",),
        targets_path=targets_path,
        config_loader=lambda path: _project_config(),
    )

    assert resolved == ("stripe.transactions",)


def test_resolve_source_names_can_map_changed_dbt_macros_to_sources(tmp_path: Path) -> None:
    manifest_path = tmp_path / "target" / "manifest.json"
    manifest_path.parent.mkdir()
    manifest_path.write_text(
        """
{
  "nodes": {
    "model.demo.stg_orders": {
      "resource_type": "model",
      "original_file_path": "models/staging/orders.sql",
      "depends_on": {
        "nodes": ["source.demo.raw.stripe_transactions"],
        "macros": ["macro.demo.currency_cleanup"]
      }
    }
  },
  "sources": {
    "source.demo.raw.stripe_transactions": {
      "database": "demo",
      "schema": "raw",
      "identifier": "stripe_transactions",
      "name": "stripe_transactions",
      "original_file_path": "models/sources/stripe.yml",
      "depends_on": {
        "nodes": []
      }
    }
  },
  "macros": {
    "macro.demo.currency_cleanup": {
      "resource_type": "macro",
      "original_file_path": "macros/currency_cleanup.sql"
    }
  }
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

    resolved = resolve_source_names(
        tmp_path / "verixa.yaml",
        changed_files=("macros/currency_cleanup.sql",),
        targets_path=targets_path,
        config_loader=lambda path: _project_config(),
    )

    assert resolved == ("stripe.transactions",)


def test_resolve_source_names_raises_for_missing_dbt_manifest(tmp_path: Path) -> None:
    targets_path = tmp_path / "verixa.targets.yaml"
    targets_path.write_text(
        """
dbt:
  manifest_path: target/missing.json
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="dbt manifest"):
        resolve_source_names(
            tmp_path / "verixa.yaml",
            changed_files=("models/staging/orders.sql",),
            targets_path=targets_path,
            config_loader=lambda path: _project_config(),
        )


def test_list_changed_files_against_reads_git_diff(tmp_path: Path) -> None:
    if shutil.which("git") is None:
        pytest.skip("git is not installed")

    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "verixa@example.com"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "Verixa"], cwd=tmp_path, check=True)
    subprocess.run(["git", "branch", "-m", "main"], cwd=tmp_path, check=True)

    baseline = tmp_path / "models"
    baseline.mkdir()
    tracked = baseline / "orders.sql"
    tracked.write_text("select 1\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-m", "baseline"], cwd=tmp_path, check=True, capture_output=True)

    subprocess.run(["git", "checkout", "-b", "feature"], cwd=tmp_path, check=True, capture_output=True)
    tracked.write_text("select 2\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-m", "change"], cwd=tmp_path, check=True, capture_output=True)

    changed = list_changed_files_against("main", cwd=tmp_path)

    assert changed == ("models/orders.sql",)
