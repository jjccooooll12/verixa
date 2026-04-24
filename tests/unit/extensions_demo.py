from __future__ import annotations

from verixa.diff.models import Finding
from verixa.diff.risk import SourceRiskHints
from verixa.extensions.api import (
    CustomCheckContext,
    FindingEnrichmentContext,
    SourceMetadataEnrichmentContext,
)


def custom_check(context: CustomCheckContext) -> tuple[Finding, ...]:
    if context.current.row_count in (None, 0):
        return ()
    return (
        Finding(
            source_name=context.contract.name,
            severity="warning",
            code="custom_demo_check",
            message=f"custom demo check saw {context.current.row_count} rows",
        ),
    )


def invalid_custom_check(context: CustomCheckContext) -> tuple[Finding, ...]:
    del context
    return ("not-a-finding",)  # type: ignore[return-value]


def failing_custom_check(context: CustomCheckContext) -> tuple[Finding, ...]:
    raise RuntimeError(f"boom:{context.contract.name}")


def finding_enricher(finding: Finding, context: FindingEnrichmentContext) -> Finding:
    return Finding(
        source_name=finding.source_name,
        severity=finding.severity,
        code=finding.code,
        message=finding.message,
        column=finding.column,
        risks=tuple(dict.fromkeys((*finding.risks, f"enriched:{context.execution_mode}"))),
        owners=finding.owners,
        source_criticality=finding.source_criticality,
        downstream_models=finding.downstream_models,
        confidence_override=finding.confidence_override,
        confidence_reason=finding.confidence_reason,
        history_metric=finding.history_metric,
        history_window=finding.history_window,
        history_sample_size=finding.history_sample_size,
        history_center_value=finding.history_center_value,
        history_lower_bound=finding.history_lower_bound,
        history_upper_bound=finding.history_upper_bound,
    )


def invalid_finding_enricher(
    finding: Finding,
    context: FindingEnrichmentContext,
) -> Finding:
    del finding, context
    return "not-a-finding"  # type: ignore[return-value]


def source_metadata_enricher(
    context: SourceMetadataEnrichmentContext,
) -> dict[str, SourceRiskHints]:
    del context
    return {
        "stripe.transactions": SourceRiskHints(
            general=("extension-general-risk",),
            columns={"amount": ("extension-column-risk",)},
            owners=("extension-owner",),
            criticality="medium",
            downstream_models=("ext_model",),
        )
    }


def invalid_source_metadata_enricher(
    context: SourceMetadataEnrichmentContext,
) -> dict[str, SourceRiskHints]:
    del context
    return {"stripe.transactions": "not-a-hint"}  # type: ignore[return-value]
