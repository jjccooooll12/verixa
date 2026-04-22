"""Changed-file to source targeting for CI-oriented Verixa runs."""

from __future__ import annotations

from dataclasses import dataclass
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

    return TargetsConfig(paths=paths)


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
    return matched_sources


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
