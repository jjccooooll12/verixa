"""Suppression models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class SuppressionRule:
    """A temporary suppression for a known finding fingerprint."""

    fingerprint: str
    owner: str
    reason: str
    expires_at: datetime
    environments: tuple[str, ...] = ()

    @property
    def applies_globally(self) -> bool:
        return not self.environments
