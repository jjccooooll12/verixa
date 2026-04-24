"""Import configured Verixa extension hooks."""

from __future__ import annotations

from importlib import import_module
from typing import Any

from verixa.config.errors import ConfigError
from verixa.contracts.models import ExtensionsConfig
from verixa.extensions.api import (
    CustomCheckHook,
    ExtensionError,
    FindingEnricherHook,
    SourceMetadataEnricherHook,
)


def load_extensions(raw_extensions: Any) -> ExtensionsConfig:
    """Parse and import configured extension hooks."""

    if raw_extensions is None:
        return ExtensionsConfig()
    if not isinstance(raw_extensions, dict):
        raise ConfigError("extensions must be a mapping when provided.")

    checks = _load_hook_list(
        raw_extensions.get("checks", ()),
        field_name="extensions.checks",
    )
    finding_enrichers = _load_hook_list(
        raw_extensions.get("finding_enrichers", ()),
        field_name="extensions.finding_enrichers",
    )
    source_metadata_enrichers = _load_hook_list(
        raw_extensions.get("source_metadata_enrichers", ()),
        field_name="extensions.source_metadata_enrichers",
    )
    return ExtensionsConfig(
        checks=tuple(checks),
        finding_enrichers=tuple(finding_enrichers),
        source_metadata_enrichers=tuple(source_metadata_enrichers),
    )


def _load_hook_list(raw_value: Any, *, field_name: str) -> list[object]:
    if raw_value in (None, (), []):
        return []
    if not isinstance(raw_value, list):
        raise ConfigError(f"{field_name} must be a list of import strings.")

    hooks: list[object] = []
    for index, item in enumerate(raw_value):
        if not isinstance(item, str) or not item.strip():
            raise ConfigError(f"{field_name}[{index}] must be a non-empty import string.")
        hooks.append(_load_hook(item.strip(), field_name=field_name, index=index))
    return hooks


def _load_hook(import_path: str, *, field_name: str, index: int) -> object:
    if ":" not in import_path:
        raise ConfigError(
            f"{field_name}[{index}] must use 'module:attribute' import syntax."
        )
    module_name, attribute_name = import_path.split(":", 1)
    if not module_name or not attribute_name:
        raise ConfigError(
            f"{field_name}[{index}] must use 'module:attribute' import syntax."
        )
    try:
        module = import_module(module_name)
    except Exception as exc:  # pragma: no cover - import machinery errors are environment specific
        raise ConfigError(
            f"Failed to import module '{module_name}' for {field_name}[{index}]: {exc}"
        ) from exc

    try:
        hook = getattr(module, attribute_name)
    except AttributeError as exc:
        raise ConfigError(
            f"Failed to resolve '{attribute_name}' from module '{module_name}' for {field_name}[{index}]."
        ) from exc

    if not callable(hook):
        raise ConfigError(f"{field_name}[{index}] must resolve to a callable hook.")
    return hook


__all__ = [
    "CustomCheckHook",
    "ExtensionError",
    "FindingEnricherHook",
    "SourceMetadataEnricherHook",
    "load_extensions",
]

