"""Public extension APIs for Verixa."""

from verixa.extensions.api import (
    CustomCheckContext,
    ExtensionError,
    FindingEnrichmentContext,
    SourceMetadataEnrichmentContext,
)
from verixa.extensions.loader import load_extensions

__all__ = [
    "CustomCheckContext",
    "ExtensionError",
    "FindingEnrichmentContext",
    "SourceMetadataEnrichmentContext",
    "load_extensions",
]

