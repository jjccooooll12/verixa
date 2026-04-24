"""Typed extension APIs for Verixa."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from verixa.contracts.models import ProjectConfig, SourceContract
from verixa.diff.models import Finding
from verixa.snapshot.models import SourceSnapshot

if TYPE_CHECKING:
    from verixa.diff.risk import SourceRiskHints


class ExtensionError(RuntimeError):
    """Raised when a configured extension hook is invalid or fails."""


@dataclass(frozen=True, slots=True)
class CustomCheckContext:
    """Context passed to one custom check hook."""

    config: ProjectConfig
    contract: SourceContract
    current: SourceSnapshot
    baseline: SourceSnapshot | None
    historical: tuple[SourceSnapshot, ...]
    used_baseline: bool
    execution_mode: str


@dataclass(frozen=True, slots=True)
class FindingEnrichmentContext:
    """Context passed to one finding enricher hook."""

    config: ProjectConfig
    used_baseline: bool
    execution_mode: str


@dataclass(frozen=True, slots=True)
class SourceMetadataEnrichmentContext:
    """Context passed to one source metadata enricher hook."""

    config: ProjectConfig


class CustomCheckHook(Protocol):
    """Typed hook for custom source-level checks."""

    def __call__(self, context: CustomCheckContext) -> tuple[Finding, ...]:
        """Return zero or more findings for one source."""


class FindingEnricherHook(Protocol):
    """Typed hook for modifying one finding after built-in enrichment."""

    def __call__(self, finding: Finding, context: FindingEnrichmentContext) -> Finding:
        """Return a replacement finding."""


class SourceMetadataEnricherHook(Protocol):
    """Typed hook for contributing source risk hints."""

    def __call__(
        self,
        context: SourceMetadataEnrichmentContext,
    ) -> dict[str, "SourceRiskHints"]:
        """Return source-name to risk-hint mappings."""
