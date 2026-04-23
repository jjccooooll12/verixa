"""Changed-file to source targeting for CI-oriented Verixa runs."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path, PurePosixPath
import subprocess
from typing import Any, Callable

import yaml

from verixa.config.errors import ConfigError
from verixa.config.loader import load_config
from verixa.contracts.models import ProjectConfig

DEFAULT_TARGETS_PATH = Path("verixa.targets.yaml")


@dataclass(frozen=True, slots=True)
class TargetsConfig:
    """Path-pattern to source-name mappings used for CI targeting."""

    paths: dict[str, tuple[str, ...]]
    dbt_manifest_path: Path | None = None


@dataclass(frozen=True, slots=True)
class DbtSourceMetadata:
    """Optional metadata imported from dbt source nodes."""

    owners: tuple[str, ...] = ()
    criticality: str | None = None


ConfigLoader = Callable[[Path | None], ProjectConfig]
TargetsLoader = Callable[[Path | None], TargetsConfig | None]
ChangedFileProvider = Callable[..., tuple[str, ...]]


def resolve_targets_path(path: Path | None = None) -> Path:
    """Resolve the preferred changed-file targeting config path."""

    if path is not None:
        return path
    return DEFAULT_TARGETS_PATH


def load_targets_config(path: Path | None = None) -> TargetsConfig | None:
    """Load optional changed-file targeting mappings from YAML."""

    targets_path = resolve_targets_path(path)
    if not targets_path.exists():
        return None

    try:
        payload = yaml.safe_load(targets_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"Failed to parse YAML from '{targets_path}': {exc}") from exc

    if not isinstance(payload, dict):
        raise ConfigError("Targets config must be a mapping.")

    raw_paths = payload.get("paths", {})
    if not isinstance(raw_paths, dict):
        raise ConfigError("Targets config 'paths' must be a mapping.")

    paths: dict[str, tuple[str, ...]] = {}
    for pattern, raw_sources in raw_paths.items():
        if not isinstance(pattern, str) or not pattern.strip():
            raise ConfigError("Targets config path patterns must be non-empty strings.")
        sources = _parse_target_sources(pattern, raw_sources)
        paths[_normalize_pattern(pattern)] = sources

    dbt_manifest_path = _parse_dbt_manifest_path(payload.get("dbt"), targets_path)

    return TargetsConfig(paths=paths, dbt_manifest_path=dbt_manifest_path)


def resolve_source_names(
    config_path: Path,
    *,
    explicit_source_names: tuple[str, ...] = (),
    changed_files: tuple[str, ...] = (),
    changed_against: str | None = None,
    targets_path: Path | None = None,
    config_loader: ConfigLoader = load_config,
    targets_loader: TargetsLoader = load_targets_config,
    changed_file_provider: ChangedFileProvider | None = None,
) -> tuple[str, ...]:
    """Resolve the source selection for one CLI run.

    Explicit ``--source`` selection wins. When changed-file targeting is requested,
    this returns the mapped sources. If no changed files match the mapping, an empty
    tuple is returned so downstream commands naturally fall back to all configured
    sources.
    """

    explicit = tuple(dict.fromkeys(explicit_source_names))
    if explicit:
        return explicit

    requested_changed_files = tuple(dict.fromkeys(changed_files))
    if not requested_changed_files and changed_against is None:
        return ()

    active_changed_file_provider = changed_file_provider or list_changed_files_against

    config = config_loader(config_path)
    targets_config = targets_loader(targets_path)
    resolved_targets_path = resolve_targets_path(targets_path)
    if targets_config is None:
        raise ConfigError(
            f"Changed-file targeting requested but '{resolved_targets_path}' was not found. "
            "Add the mapping file or use --source."
        )

    _validate_target_sources(targets_config, tuple(config.sources))

    discovered_changed_files: tuple[str, ...] = ()
    if changed_against is not None:
        discovered_changed_files = active_changed_file_provider(changed_against, cwd=config_path.parent)

    candidate_files = _normalize_changed_files((*requested_changed_files, *discovered_changed_files))
    if not candidate_files:
        return ()

    matched_sources = _match_source_names(
        changed_files=candidate_files,
        targets_config=targets_config,
        available_source_names=tuple(config.sources),
    )
    if targets_config.dbt_manifest_path is None:
        return matched_sources

    dbt_matches = _match_dbt_source_names(
        changed_files=candidate_files,
        manifest_path=targets_config.dbt_manifest_path,
        config=config,
    )
    if not matched_sources:
        return dbt_matches

    union = set(matched_sources)
    union.update(dbt_matches)
    return tuple(source_name for source_name in config.sources if source_name in union)


def load_dbt_downstream_models(
    config: ProjectConfig,
    *,
    targets_path: Path | None = None,
    targets_loader: TargetsLoader = load_targets_config,
) -> dict[str, tuple[str, ...]]:
    """Load downstream dbt model names for each configured Verixa source."""

    targets_config = targets_loader(targets_path)
    if targets_config is None or targets_config.dbt_manifest_path is None:
        return {}

    manifest = _load_dbt_manifest(targets_config.dbt_manifest_path)
    return _map_dbt_downstream_models(manifest, config)


def load_dbt_source_metadata(
    config: ProjectConfig,
    *,
    targets_path: Path | None = None,
    targets_loader: TargetsLoader = load_targets_config,
) -> dict[str, DbtSourceMetadata]:
    """Load optional owner and criticality metadata from dbt source nodes."""

    targets_config = targets_loader(targets_path)
    if targets_config is None or targets_config.dbt_manifest_path is None:
        return {}

    manifest = _load_dbt_manifest(targets_config.dbt_manifest_path)
    return _map_dbt_source_metadata(manifest, config)


def list_changed_files_against(base_ref: str, *, cwd: Path) -> tuple[str, ...]:
    """List repo-relative changed files from ``git diff <base_ref>...HEAD``."""

    if not isinstance(base_ref, str) or not base_ref.strip():
        raise ConfigError("--changed-against requires a non-empty git ref.")

    repo_root = _resolve_git_root(cwd)
    completed = subprocess.run(
        ["git", "diff", "--name-only", "--diff-filter=ACMRTUXB", f"{base_ref}...HEAD"],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip() or "git diff failed"
        raise ConfigError(
            f"Failed to list changed files against '{base_ref}': {detail}"
        )

    lines = tuple(line for line in completed.stdout.splitlines() if line.strip())
    return _normalize_changed_files(lines)


def _resolve_git_root(cwd: Path) -> Path:
    completed = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip() or "git rev-parse failed"
        raise ConfigError(f"Failed to locate git repository root: {detail}")
    return Path(completed.stdout.strip())


def _parse_target_sources(pattern: str, raw_sources: Any) -> tuple[str, ...]:
    if isinstance(raw_sources, str) and raw_sources.strip():
        return (raw_sources.strip(),)
    if not isinstance(raw_sources, list) or not raw_sources:
        raise ConfigError(
            f"Targets config pattern '{pattern}' must map to a non-empty source list or string."
        )

    sources: list[str] = []
    for source_name in raw_sources:
        if not isinstance(source_name, str) or not source_name.strip():
            raise ConfigError(
                f"Targets config pattern '{pattern}' must contain only non-empty source names."
            )
        sources.append(source_name.strip())
    return tuple(dict.fromkeys(sources))


def _parse_dbt_manifest_path(raw_dbt: Any, targets_path: Path) -> Path | None:
    if raw_dbt is None:
        return None
    if not isinstance(raw_dbt, dict):
        raise ConfigError("Targets config 'dbt' must be a mapping when provided.")

    raw_manifest_path = raw_dbt.get("manifest_path", "target/manifest.json")
    if not isinstance(raw_manifest_path, str) or not raw_manifest_path.strip():
        raise ConfigError("Targets config dbt.manifest_path must be a non-empty string.")

    manifest_path = Path(raw_manifest_path)
    if not manifest_path.is_absolute():
        manifest_path = (targets_path.parent / manifest_path).resolve()
    return manifest_path


def _validate_target_sources(
    targets_config: TargetsConfig,
    available_source_names: tuple[str, ...],
) -> None:
    available = set(available_source_names)
    referenced = {
        source_name
        for source_names in targets_config.paths.values()
        for source_name in source_names
    }
    unknown = sorted(referenced - available)
    if unknown:
        raise ConfigError(
            "Targets config references unknown sources: "
            f"{', '.join(unknown)}. Available sources: {', '.join(available_source_names)}."
        )


def _match_source_names(
    *,
    changed_files: tuple[str, ...],
    targets_config: TargetsConfig,
    available_source_names: tuple[str, ...],
) -> tuple[str, ...]:
    matched: set[str] = set()
    for changed_file in changed_files:
        for pattern, source_names in targets_config.paths.items():
            if _path_matches_pattern(changed_file, pattern):
                matched.update(source_names)

    if not matched:
        return ()
    return tuple(source_name for source_name in available_source_names if source_name in matched)


def _match_dbt_source_names(
    *,
    changed_files: tuple[str, ...],
    manifest_path: Path,
    config: ProjectConfig,
) -> tuple[str, ...]:
    manifest = _load_dbt_manifest(manifest_path)
    changed_node_ids = _changed_dbt_node_ids(changed_files, manifest)
    if not changed_node_ids:
        return ()

    verixa_source_lookup = _build_verixa_source_lookup(config)
    matched: set[str] = set()
    for node_id in changed_node_ids:
        for source_id in _collect_upstream_source_ids(node_id, manifest):
            source_node = manifest["sources"].get(source_id)
            if not isinstance(source_node, dict):
                continue
            for table_key in _dbt_source_table_keys(source_node):
                matched.update(verixa_source_lookup.get(table_key, ()))

    if not matched:
        return ()
    return tuple(source_name for source_name in config.sources if source_name in matched)


def _map_dbt_downstream_models(
    manifest: dict[str, Any],
    config: ProjectConfig,
) -> dict[str, tuple[str, ...]]:
    verixa_source_lookup = _build_verixa_source_lookup(config)
    impacts: dict[str, set[str]] = {source_name: set() for source_name in config.sources}

    for unique_id, node in manifest["nodes"].items():
        if not isinstance(node, dict) or node.get("resource_type") != "model":
            continue
        model_name = _dbt_model_name(unique_id, node)
        for source_id in _collect_upstream_source_ids(unique_id, manifest):
            source_node = manifest["sources"].get(source_id)
            if not isinstance(source_node, dict):
                continue
            for table_key in _dbt_source_table_keys(source_node):
                for source_name in verixa_source_lookup.get(table_key, ()):
                    impacts[source_name].add(model_name)

    return {
        source_name: tuple(sorted(model_names))
        for source_name, model_names in impacts.items()
        if model_names
    }


def _map_dbt_source_metadata(
    manifest: dict[str, Any],
    config: ProjectConfig,
) -> dict[str, DbtSourceMetadata]:
    verixa_source_lookup = _build_verixa_source_lookup(config)
    metadata: dict[str, DbtSourceMetadata] = {}

    for node in manifest["sources"].values():
        if not isinstance(node, dict):
            continue
        source_metadata = _dbt_source_metadata(node)
        if source_metadata is None:
            continue
        for table_key in _dbt_source_table_keys(node):
            for source_name in verixa_source_lookup.get(table_key, ()):
                existing = metadata.get(source_name)
                if existing is None:
                    metadata[source_name] = source_metadata
                    continue
                metadata[source_name] = DbtSourceMetadata(
                    owners=tuple(dict.fromkeys((*existing.owners, *source_metadata.owners))),
                    criticality=existing.criticality or source_metadata.criticality,
                )

    return metadata


def _load_dbt_manifest(manifest_path: Path) -> dict[str, Any]:
    if not manifest_path.exists():
        raise ConfigError(
            f"dbt manifest '{manifest_path}' was not found. "
            "Update verixa.targets.yaml or generate dbt artifacts first."
        )
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError(
            f"Failed to parse dbt manifest JSON from '{manifest_path}': {exc}"
        ) from exc

    if not isinstance(payload, dict):
        raise ConfigError("dbt manifest must be a JSON object.")

    nodes = payload.get("nodes", {})
    sources = payload.get("sources", {})
    macros = payload.get("macros", {})
    if not isinstance(nodes, dict) or not isinstance(sources, dict) or not isinstance(macros, dict):
        raise ConfigError("dbt manifest must contain object-valued 'nodes', 'sources', and 'macros' sections.")

    return {
        "nodes": nodes,
        "sources": sources,
        "macros": macros,
    }


def _changed_dbt_node_ids(
    changed_files: tuple[str, ...],
    manifest: dict[str, Any],
) -> set[str]:
    changed = set(changed_files)
    nodes_by_id = {
        **manifest["nodes"],
        **manifest["sources"],
        **manifest["macros"],
    }

    changed_node_ids = {
        unique_id
        for unique_id, node in nodes_by_id.items()
        if _dbt_node_matches_changed_files(node, changed)
    }

    changed_macro_ids = {
        unique_id
        for unique_id in changed_node_ids
        if unique_id.startswith("macro.")
    }
    if not changed_macro_ids:
        return changed_node_ids

    for unique_id, node in manifest["nodes"].items():
        depends_on = node.get("depends_on", {})
        macro_ids = depends_on.get("macros", ())
        if any(macro_id in changed_macro_ids for macro_id in macro_ids):
            changed_node_ids.add(unique_id)
    return changed_node_ids


def _dbt_node_matches_changed_files(node: Any, changed_files: set[str]) -> bool:
    if not isinstance(node, dict):
        return False

    candidate_paths = {
        _normalize_optional_path(node.get("original_file_path")),
        _normalize_optional_path(node.get("path")),
        _normalize_optional_path(node.get("patch_path")),
    }
    candidate_paths.discard(None)
    return any(path in changed_files for path in candidate_paths)


def _build_verixa_source_lookup(config: ProjectConfig) -> dict[str, tuple[str, ...]]:
    lookup: dict[str, list[str]] = {}
    for source_name, source in config.sources.items():
        for table_key in _verixa_table_keys(config, source.table):
            lookup.setdefault(table_key, []).append(source_name)
    return {
        table_key: tuple(dict.fromkeys(source_names))
        for table_key, source_names in sorted(lookup.items())
    }


def _verixa_table_keys(config: ProjectConfig, table: str) -> tuple[str, ...]:
    parts = [part.strip().lower() for part in table.split(".") if part.strip()]
    if len(parts) == 2:
        dataset, identifier = parts
        values = [f"{dataset}.{identifier}"]
        if config.warehouse.project:
            values.append(f"{config.warehouse.project.lower()}.{dataset}.{identifier}")
        return tuple(dict.fromkeys(values))
    if len(parts) == 3:
        project, dataset, identifier = parts
        return (f"{project}.{dataset}.{identifier}", f"{dataset}.{identifier}")
    return (".".join(parts),)


def _dbt_source_table_keys(node: dict[str, Any]) -> tuple[str, ...]:
    database = _normalize_optional_path(node.get("database"))
    schema = _normalize_optional_path(node.get("schema"))
    identifier = _normalize_optional_path(node.get("identifier"))
    name = _normalize_optional_path(node.get("name"))

    identifiers = [value for value in (identifier, name) if value]
    values: list[str] = []
    for resolved_identifier in identifiers:
        if schema:
            values.append(f"{schema}.{resolved_identifier}")
        if database and schema:
            values.append(f"{database}.{schema}.{resolved_identifier}")
    return tuple(dict.fromkeys(values))


def _dbt_model_name(unique_id: str, node: dict[str, Any]) -> str:
    alias = node.get("alias")
    if isinstance(alias, str) and alias.strip():
        return alias.strip()
    name = node.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return unique_id


def _dbt_source_metadata(node: dict[str, Any]) -> DbtSourceMetadata | None:
    meta = _extract_dbt_meta(node)
    verixa_meta = meta.get("verixa")
    if not isinstance(verixa_meta, dict):
        verixa_meta = {}

    owners = _normalize_optional_string_values(
        verixa_meta.get("owners", meta.get("owners", meta.get("owner")))
    )
    criticality = _normalize_optional_criticality(
        verixa_meta.get("criticality", meta.get("criticality"))
    )

    if not owners and criticality is None:
        return None
    return DbtSourceMetadata(
        owners=owners,
        criticality=criticality,
    )


def _collect_upstream_source_ids(
    node_id: str,
    manifest: dict[str, Any],
) -> tuple[str, ...]:
    seen: set[str] = set()
    source_ids: set[str] = set()
    nodes_by_id = {
        **manifest["nodes"],
        **manifest["sources"],
    }

    def _walk(current_id: str) -> None:
        if current_id in seen:
            return
        seen.add(current_id)
        if current_id.startswith("source."):
            source_ids.add(current_id)
            return
        node = nodes_by_id.get(current_id)
        if not isinstance(node, dict):
            return
        depends_on = node.get("depends_on", {})
        for dependency_id in depends_on.get("nodes", ()):
            if isinstance(dependency_id, str):
                _walk(dependency_id)

    _walk(node_id)
    return tuple(sorted(source_ids))


def _normalize_optional_path(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().replace("\\", "/")
    if not normalized:
        return None
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized.lower()


def _extract_dbt_meta(node: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    config = node.get("config")
    if isinstance(config, dict):
        config_meta = config.get("meta")
        if isinstance(config_meta, dict):
            merged.update(config_meta)
    node_meta = node.get("meta")
    if isinstance(node_meta, dict):
        merged.update(node_meta)
    return merged


def _normalize_optional_string_values(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        cleaned = value.strip()
        return (cleaned,) if cleaned else ()
    if not isinstance(value, (list, tuple)):
        return ()

    items: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            continue
        cleaned = item.strip()
        if not cleaned or cleaned in seen:
            continue
        items.append(cleaned)
        seen.add(cleaned)
    return tuple(items)


def _normalize_optional_criticality(value: Any) -> str | None:
    if value in {"low", "medium", "high"}:
        return value
    return None


def _normalize_changed_files(changed_files: tuple[str, ...] | list[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_path in changed_files:
        if not isinstance(raw_path, str):
            continue
        value = raw_path.strip().replace("\\", "/")
        if not value:
            continue
        while value.startswith("./"):
            value = value[2:]
        if value in {"", "."}:
            continue
        if value not in seen:
            normalized.append(value)
            seen.add(value)
    return tuple(normalized)


def _normalize_pattern(pattern: str) -> str:
    value = pattern.strip().replace("\\", "/")
    while value.startswith("./"):
        value = value[2:]
    return value


def _path_matches_pattern(path: str, pattern: str) -> bool:
    normalized_path = path.strip().replace("\\", "/")
    normalized_pattern = _normalize_pattern(pattern)
    if not _has_glob_magic(normalized_pattern):
        prefix = normalized_pattern.rstrip("/")
        return normalized_path == prefix or normalized_path.startswith(prefix + "/")
    path_value = PurePosixPath(normalized_path)
    return any(path_value.match(candidate) for candidate in _pattern_variants(normalized_pattern))


def _pattern_variants(pattern: str) -> tuple[str, ...]:
    variants = {pattern}
    pending = [pattern]
    while pending:
        current = pending.pop()
        index = current.find("**/")
        if index == -1:
            continue
        candidate = current[:index] + current[index + 3 :]
        if candidate not in variants:
            variants.add(candidate)
            pending.append(candidate)
    return tuple(sorted(variants))


def _has_glob_magic(pattern: str) -> bool:
    return any(char in pattern for char in "*?[]")
