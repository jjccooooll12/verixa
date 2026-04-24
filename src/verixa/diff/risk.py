"""Optional downstream risk mapping support."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from verixa.config.errors import ConfigError
from verixa.contracts.models import ExtensionsConfig, ProjectConfig
from verixa.extensions.api import ExtensionError, SourceMetadataEnrichmentContext
from verixa.targeting import load_dbt_downstream_models, load_dbt_source_metadata


@dataclass(frozen=True, slots=True)
class SourceRiskHints:
    """User-maintained downstream risk metadata for one source."""

    general: tuple[str, ...]
    columns: dict[str, tuple[str, ...]]
    owners: tuple[str, ...] = ()
    criticality: str | None = None
    downstream_models: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class RiskConfig:
    """Optional risk mapping file."""

    sources: dict[str, SourceRiskHints]


DEFAULT_RISK_PATH = Path("verixa.risk.yaml")


def load_risk_config(path: Path | None = None) -> RiskConfig | None:
    """Load optional downstream risk mappings if the file exists."""

    risk_path = resolve_risk_path(path)
    if not risk_path.exists():
        return None

    try:
        payload = yaml.safe_load(risk_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"Failed to parse YAML from '{risk_path}': {exc}") from exc

    if not isinstance(payload, dict):
        raise ConfigError("Risk config must be a mapping.")

    raw_sources = payload.get("sources", {})
    if not isinstance(raw_sources, dict):
        raise ConfigError("Risk config 'sources' must be a mapping.")

    sources: dict[str, SourceRiskHints] = {}
    for source_name, raw_source in sorted(raw_sources.items()):
        if not isinstance(raw_source, dict):
            raise ConfigError(f"Risk config for source '{source_name}' must be a mapping.")
        general = _parse_risk_list(raw_source.get("general", []), source_name)
        owners = _parse_optional_string_list(raw_source.get("owners", []), source_name, "owners")
        criticality = _parse_optional_criticality(raw_source.get("criticality"), source_name)
        downstream_models = _parse_optional_string_list(
            raw_source.get("downstream_models", []),
            source_name,
            "downstream_models",
        )
        raw_columns = raw_source.get("columns", {})
        if not isinstance(raw_columns, dict):
            raise ConfigError(
                f"Risk config for source '{source_name}' columns must be a mapping."
            )
        columns = {
            column_name: _parse_risk_list(values, source_name, column_name)
            for column_name, values in sorted(raw_columns.items())
        }
        sources[source_name] = SourceRiskHints(
            general=general,
            columns=columns,
            owners=owners,
            criticality=criticality,
            downstream_models=downstream_models,
        )

    return RiskConfig(sources=sources)


def resolve_risk_path(path: Path | None = None) -> Path:
    """Resolve the preferred risk-config path."""

    if path is not None:
        return path
    return DEFAULT_RISK_PATH


def enrich_risk_config_with_dbt_impacts(
    risk_config: RiskConfig | None,
    *,
    config: ProjectConfig,
    targets_path: Path | None,
) -> RiskConfig | None:
    """Merge dbt-derived metadata into risk hints without overriding explicit config."""

    downstream_models = load_dbt_downstream_models(config, targets_path=targets_path)
    source_metadata = load_dbt_source_metadata(config, targets_path=targets_path)
    extension_metadata = _load_extension_source_metadata(config.extensions, config)
    if not downstream_models and not source_metadata and not extension_metadata:
        return risk_config

    merged_sources: dict[str, SourceRiskHints] = {}
    existing_sources = {} if risk_config is None else risk_config.sources
    for source_name in sorted(
        set(existing_sources) | set(downstream_models) | set(source_metadata) | set(extension_metadata)
    ):
        existing = existing_sources.get(source_name)
        derived_models = downstream_models.get(source_name, ())
        derived_metadata = source_metadata.get(source_name)
        extension_hints = extension_metadata.get(source_name)
        merged_general = () if extension_hints is None else extension_hints.general
        merged_columns = {} if extension_hints is None else extension_hints.columns
        merged_owners = () if extension_hints is None else extension_hints.owners
        merged_criticality = None if extension_hints is None else extension_hints.criticality
        merged_downstream_models = () if extension_hints is None else extension_hints.downstream_models
        if existing is None:
            merged_sources[source_name] = SourceRiskHints(
                general=merged_general,
                columns=merged_columns,
                owners=tuple(dict.fromkeys((*merged_owners, *( () if derived_metadata is None else derived_metadata.owners )))),
                criticality=merged_criticality
                if merged_criticality is not None
                else None if derived_metadata is None else derived_metadata.criticality,
                downstream_models=tuple(dict.fromkeys((*merged_downstream_models, *derived_models))),
            )
            continue
        merged_sources[source_name] = SourceRiskHints(
            general=tuple(dict.fromkeys((*existing.general, *merged_general))),
            columns=_merge_column_hints(existing.columns, merged_columns),
            owners=tuple(
                dict.fromkeys(
                    (
                        *existing.owners,
                        *merged_owners,
                        *( () if derived_metadata is None else derived_metadata.owners ),
                    )
                )
            ),
            criticality=existing.criticality
            if existing.criticality is not None
            else merged_criticality
            if merged_criticality is not None
            else None if derived_metadata is None else derived_metadata.criticality,
            downstream_models=tuple(
                dict.fromkeys((*existing.downstream_models, *merged_downstream_models, *derived_models))
            ),
        )

    return RiskConfig(sources=merged_sources)


def _load_extension_source_metadata(
    extensions: ExtensionsConfig,
    config: ProjectConfig,
) -> dict[str, SourceRiskHints]:
    if not extensions.source_metadata_enrichers:
        return {}

    merged: dict[str, SourceRiskHints] = {}
    context = SourceMetadataEnrichmentContext(config=config)
    for hook in extensions.source_metadata_enrichers:
        try:
            hints = hook(context)
        except Exception as exc:  # pragma: no cover - hook-specific failures are environment specific
            raise ExtensionError(
                f"Source metadata enricher hook '{_hook_name(hook)}' failed: {exc}"
            ) from exc
        if not isinstance(hints, dict):
            raise ExtensionError(
                f"Source metadata enricher hook '{_hook_name(hook)}' must return a dict[str, SourceRiskHints]."
            )
        for source_name, hint in hints.items():
            if source_name not in config.sources:
                continue
            if not isinstance(hint, SourceRiskHints):
                raise ExtensionError(
                    f"Source metadata enricher hook '{_hook_name(hook)}' returned a non-SourceRiskHints value for '{source_name}'."
                )
            existing = merged.get(source_name)
            if existing is None:
                merged[source_name] = hint
                continue
            merged[source_name] = SourceRiskHints(
                general=tuple(dict.fromkeys((*existing.general, *hint.general))),
                columns=_merge_column_hints(existing.columns, hint.columns),
                owners=tuple(dict.fromkeys((*existing.owners, *hint.owners))),
                criticality=existing.criticality or hint.criticality,
                downstream_models=tuple(
                    dict.fromkeys((*existing.downstream_models, *hint.downstream_models))
                ),
            )
    return merged


def _merge_column_hints(
    existing: dict[str, tuple[str, ...]],
    additional: dict[str, tuple[str, ...]],
) -> dict[str, tuple[str, ...]]:
    merged = {column_name: values for column_name, values in existing.items()}
    for column_name, values in additional.items():
        merged[column_name] = tuple(dict.fromkeys((*merged.get(column_name, ()), *values)))
    return merged


def _parse_risk_list(
    raw_value: Any,
    source_name: str,
    column_name: str | None = None,
) -> tuple[str, ...]:
    if not isinstance(raw_value, list):
        suffix = f" column '{column_name}'" if column_name else ""
        raise ConfigError(
            f"Risk config for source '{source_name}'{suffix} must be a list of strings."
        )

    values: list[str] = []
    for item in raw_value:
        if not isinstance(item, str) or not item:
            suffix = f" column '{column_name}'" if column_name else ""
            raise ConfigError(
                f"Risk config for source '{source_name}'{suffix} must contain only strings."
            )
        values.append(item)
    return tuple(values)


def _parse_optional_string_list(
    raw_value: Any,
    source_name: str,
    field_name: str,
) -> tuple[str, ...]:
    if raw_value in (None, []):
        return ()
    if not isinstance(raw_value, list):
        raise ConfigError(
            f"Risk config for source '{source_name}' {field_name} must be a list of strings."
        )
    values: list[str] = []
    for item in raw_value:
        if not isinstance(item, str) or not item.strip():
            raise ConfigError(
                f"Risk config for source '{source_name}' {field_name} must contain only strings."
            )
        values.append(item.strip())
    return tuple(values)


def _parse_optional_criticality(raw_value: Any, source_name: str) -> str | None:
    if raw_value is None:
        return None
    if raw_value not in {"low", "medium", "high"}:
        raise ConfigError(
            f"Risk config for source '{source_name}' criticality must be one of: low, medium, high."
        )
    return raw_value


def _hook_name(hook: object) -> str:
    hook_type = type(hook)
    module = getattr(hook, "__module__", hook_type.__module__)
    qualname = getattr(hook, "__qualname__", hook_type.__qualname__)
    return f"{module}.{qualname}"
