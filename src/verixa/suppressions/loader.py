"""Load suppression rules from a standalone YAML file."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from verixa.suppressions.models import SuppressionRule

DEFAULT_SUPPRESSIONS_PATH = Path("verixa.suppressions.yaml")


class SuppressionError(RuntimeError):
    """Raised when suppression rules cannot be parsed safely."""


def load_suppressions(path: Path | None = None) -> tuple[SuppressionRule, ...]:
    """Load suppression rules from disk. Missing files are allowed."""

    resolved_path = path or DEFAULT_SUPPRESSIONS_PATH
    if not resolved_path.exists():
        return ()

    try:
        payload = yaml.safe_load(resolved_path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        raise SuppressionError(f"Failed to read suppressions from '{resolved_path}': {exc}") from exc

    items = payload.get("suppressions", ())
    if not isinstance(items, list):
        raise SuppressionError(
            f"Suppressions file '{resolved_path}' must contain a top-level 'suppressions' list."
        )

    rules: list[SuppressionRule] = []
    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            raise SuppressionError(
                f"Suppression #{index} in '{resolved_path}' must be a mapping."
            )
        rules.append(_parse_rule(item, index=index, path=resolved_path))
    return tuple(rules)


def split_active_and_expired(
    rules: tuple[SuppressionRule, ...],
    *,
    now: datetime | None = None,
) -> tuple[tuple[SuppressionRule, ...], tuple[SuppressionRule, ...]]:
    """Split suppression rules into active and expired sets."""

    effective_now = now or datetime.now(timezone.utc)
    active: list[SuppressionRule] = []
    expired: list[SuppressionRule] = []
    for rule in rules:
        if rule.expires_at <= effective_now:
            expired.append(rule)
        else:
            active.append(rule)
    return tuple(active), tuple(expired)


def _parse_rule(payload: dict[str, Any], *, index: int, path: Path) -> SuppressionRule:
    fingerprint = _require_non_empty_string(payload.get("fingerprint"), "fingerprint", index=index, path=path)
    owner = _require_non_empty_string(payload.get("owner"), "owner", index=index, path=path)
    reason = _require_non_empty_string(payload.get("reason"), "reason", index=index, path=path)
    expires_at = _parse_expires_at(payload.get("expires_at"), index=index, path=path)

    environments_raw = payload.get("environments", ())
    if environments_raw in (None, ()):
        environments = ()
    else:
        if not isinstance(environments_raw, list) or not all(
            isinstance(item, str) and item.strip() for item in environments_raw
        ):
            raise SuppressionError(
                f"Suppression #{index} in '{path}' has an invalid environments list."
            )
        environments = tuple(item.strip() for item in environments_raw)

    return SuppressionRule(
        fingerprint=fingerprint,
        owner=owner,
        reason=reason,
        expires_at=expires_at,
        environments=environments,
    )


def _require_non_empty_string(
    value: Any,
    field_name: str,
    *,
    index: int,
    path: Path,
) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise SuppressionError(
        f"Suppression #{index} in '{path}' requires a non-empty '{field_name}' field."
    )


def _parse_expires_at(
    value: Any,
    *,
    index: int,
    path: Path,
) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str) and value.strip():
        raw = value.strip()
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError as exc:
            raise SuppressionError(
                f"Suppression #{index} in '{path}' has invalid expires_at '{raw}'. "
                "Use an ISO-8601 UTC timestamp like 2026-05-15T00:00:00Z."
            ) from exc
    raise SuppressionError(
        f"Suppression #{index} in '{path}' requires a non-empty 'expires_at' field."
    )
