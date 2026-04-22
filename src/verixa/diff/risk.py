"""Optional downstream risk mapping support."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from verixa.config.errors import ConfigError


@dataclass(frozen=True, slots=True)
class SourceRiskHints:
    """User-maintained downstream risk metadata for one source."""

    general: tuple[str, ...]
    columns: dict[str, tuple[str, ...]]


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
        raw_columns = raw_source.get("columns", {})
        if not isinstance(raw_columns, dict):
            raise ConfigError(
                f"Risk config for source '{source_name}' columns must be a mapping."
            )
        columns = {
            column_name: _parse_risk_list(values, source_name, column_name)
            for column_name, values in sorted(raw_columns.items())
        }
        sources[source_name] = SourceRiskHints(general=general, columns=columns)

    return RiskConfig(sources=sources)


def resolve_risk_path(path: Path | None = None) -> Path:
    """Resolve the preferred risk-config path."""

    if path is not None:
        return path
    return DEFAULT_RISK_PATH


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
