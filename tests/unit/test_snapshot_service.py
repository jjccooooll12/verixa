from __future__ import annotations

from dataguard.connectors.base import SourceCaptureRequest, WarehouseConnector
from dataguard.contracts.models import (
    AcceptedValuesTest,
    FreshnessConfig,
    NoNullsTest,
    ProjectConfig,
    SourceContract,
    WarehouseConfig,
)
from dataguard.snapshot.models import SourceSnapshot
from dataguard.snapshot.service import SnapshotService
from tests.unit.test_support import make_source_snapshot


class _RecordingConnector(WarehouseConnector):
    def __init__(self) -> None:
        self.calls: list[tuple[str, SourceCaptureRequest]] = []
        self.estimate_calls: list[tuple[str, SourceCaptureRequest]] = []

    def capture_source(
        self,
        source: SourceContract,
        capture_request: SourceCaptureRequest,
    ) -> SourceSnapshot:
        self.calls.append((source.name, capture_request))
        return make_source_snapshot(source_name=source.name, schema=source.schema)

    def estimate_source_bytes(
        self,
        source: SourceContract,
        capture_request: SourceCaptureRequest,
    ) -> int:
        self.estimate_calls.append((source.name, capture_request))
        return 1024


def _make_project_config() -> ProjectConfig:
    return ProjectConfig(
        warehouse=WarehouseConfig(kind="bigquery", project="demo", location="US"),
        sources={
            "payments.transactions": SourceContract(
                name="payments.transactions",
                table="raw.payments_transactions",
                schema={"amount": "FLOAT64", "created_at": "TIMESTAMP", "currency": "STRING"},
                freshness=FreshnessConfig(
                    column="created_at",
                    max_age="1h",
                    max_age_seconds=3600,
                ),
                tests=(
                    NoNullsTest(column="amount"),
                    AcceptedValuesTest(column="currency", values=("USD", "EUR")),
                ),
            ),
            "stripe.transactions": SourceContract(
                name="stripe.transactions",
                table="raw.stripe_transactions",
                schema={"amount": "FLOAT64", "created_at": "TIMESTAMP", "currency": "STRING"},
                freshness=FreshnessConfig(
                    column="created_at",
                    max_age="1h",
                    max_age_seconds=3600,
                ),
                tests=(
                    NoNullsTest(column="amount"),
                    AcceptedValuesTest(column="currency", values=("USD", "EUR")),
                ),
            ),
        },
    )


def test_snapshot_service_uses_test_capture_shape() -> None:
    connector = _RecordingConnector()
    service = SnapshotService(connector, max_workers=1)

    snapshot = service.capture(_make_project_config(), mode="test")

    assert tuple(snapshot.sources) == ("payments.transactions", "stripe.transactions")
    requests = {source_name: request for source_name, request in connector.calls}
    assert requests["stripe.transactions"] == SourceCaptureRequest(
        null_rate_columns=("amount",),
        freshness_column="created_at",
        accepted_values_tests=(
            AcceptedValuesTest(column="currency", values=("USD", "EUR")),
        ),
        include_exact_row_count=False,
    )


def test_snapshot_service_uses_full_capture_shape_for_plan() -> None:
    connector = _RecordingConnector()
    service = SnapshotService(connector, max_workers=1)

    service.capture(_make_project_config(), mode="plan")

    requests = {source_name: request for source_name, request in connector.calls}
    assert requests["stripe.transactions"] == SourceCaptureRequest(
        null_rate_columns=("amount", "created_at", "currency"),
        freshness_column="created_at",
        accepted_values_tests=(
            AcceptedValuesTest(column="currency", values=("USD", "EUR")),
        ),
        include_exact_row_count=True,
    )


def test_snapshot_service_can_estimate_bytes() -> None:
    connector = _RecordingConnector()
    service = SnapshotService(connector, max_workers=1)

    estimates = service.estimate_bytes(_make_project_config(), mode="test")

    assert estimates == {
        "payments.transactions": 1024,
        "stripe.transactions": 1024,
    }
    assert len(connector.estimate_calls) == 2
