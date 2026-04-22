"""YAML configuration loading for DataGuard."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from dataguard.config.errors import ConfigError
from dataguard.contracts.models import (
    AcceptedValuesTest,
    BaselineConfig,
    CheckConfig,
    FreshnessConfig,
    NoNullsTest,
    NullRateChangeThresholds,
    ProjectConfig,
    RowCountChangeThresholds,
    RulesConfig,
    ScanConfig,
    SourceContract,
    TestDefinition,
    WarehouseConfig,
)
from dataguard.contracts.normalize import (
    NormalizationError,
    normalize_schema_mapping,
    parse_duration_to_seconds,
    validate_identifier,
)

_DEFAULT_CONFIG_PATH = Path("dataguard.yaml")


def load_config(
    path: Path | None = None,
    *,
    source_names: tuple[str, ...] = (),
) -> ProjectConfig:
    """Load and validate the main project configuration file."""

    config_path = path or _DEFAULT_CONFIG_PATH
    if not config_path.exists():
        raise ConfigError(
            f"Config file '{config_path}' was not found. Run 'dataguard init' first."
        )

    try:
        raw_data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"Failed to parse YAML from '{config_path}': {exc}") from exc

    if not isinstance(raw_data, dict):
        raise ConfigError("Top-level config must be a mapping.")

    try:
        warehouse = _parse_warehouse(raw_data.get("warehouse"))
        rules = _parse_rules(raw_data.get("rules"))
        baseline = _parse_baseline(raw_data.get("baseline"))
        check = _parse_check(raw_data.get("check"))
        sources = _parse_sources(
            raw_data.get("sources"),
            default_rules=rules,
            default_check=check,
        )
    except NormalizationError as exc:
        raise ConfigError(str(exc)) from exc

    config = ProjectConfig(
        warehouse=warehouse,
        sources=sources,
        rules=rules,
        baseline=baseline,
        check=check,
    )
    return _select_sources(config, source_names=source_names)


def _parse_warehouse(raw_warehouse: Any) -> WarehouseConfig:
    if not isinstance(raw_warehouse, dict):
        raise NormalizationError("Config must define a 'warehouse' mapping.")

    kind = raw_warehouse.get("kind")
    if kind != "bigquery":
        raise NormalizationError(
            "DataGuard v1 supports only 'bigquery' warehouse.kind values."
        )

    project = raw_warehouse.get("project")
    location = raw_warehouse.get("location")
    if project is not None and not isinstance(project, str):
        raise NormalizationError("warehouse.project must be a string when provided.")
    if location is not None and not isinstance(location, str):
        raise NormalizationError("warehouse.location must be a string when provided.")

    return WarehouseConfig(kind=kind, project=project, location=location)


def _parse_sources(
    raw_sources: Any,
    default_rules: RulesConfig,
    default_check: CheckConfig,
) -> dict[str, SourceContract]:
    if not isinstance(raw_sources, dict) or not raw_sources:
        raise NormalizationError("Config must define at least one source under 'sources'.")

    parsed_sources: dict[str, SourceContract] = {}
    for source_name, raw_source in sorted(raw_sources.items()):
        if not isinstance(source_name, str) or not source_name:
            raise NormalizationError("Source names must be non-empty strings.")
        if not isinstance(raw_source, dict):
            raise NormalizationError(f"Source '{source_name}' must be a mapping.")

        table = raw_source.get("table")
        if not isinstance(table, str) or not table:
            raise NormalizationError(f"Source '{source_name}' must define a non-empty table.")

        schema = normalize_schema_mapping(raw_source.get("schema", {}))
        if not schema:
            raise NormalizationError(
                f"Source '{source_name}' must declare at least one schema column."
            )

        freshness = _parse_freshness(source_name, raw_source.get("freshness"), schema)
        scan = _parse_scan(source_name, raw_source.get("scan"), schema)
        tests = _parse_tests(source_name, raw_source.get("tests", []), schema)
        rules = _parse_rules(
            raw_source.get("rules"),
            prefix=f"source '{source_name}' rules",
            defaults=default_rules,
        )
        check = _parse_check(
            raw_source.get("check"),
            prefix=f"source '{source_name}' check",
            defaults=default_check,
        )

        parsed_sources[source_name] = SourceContract(
            name=source_name,
            table=table,
            schema=schema,
            freshness=freshness,
            scan=scan,
            tests=tests,
            check=check,
            rules=rules,
        )
    return parsed_sources


def _parse_baseline(raw_baseline: Any) -> BaselineConfig:
    if raw_baseline is None:
        return BaselineConfig()
    if not isinstance(raw_baseline, dict):
        raise NormalizationError("baseline must be a mapping when provided.")

    warning_age = raw_baseline.get("warning_age", BaselineConfig().warning_age)
    if warning_age is None:
        return BaselineConfig(warning_age=None, warning_age_seconds=None)
    if not isinstance(warning_age, str) or not warning_age:
        raise NormalizationError("baseline.warning_age must be a non-empty string or null.")

    return BaselineConfig(
        warning_age=warning_age,
        warning_age_seconds=parse_duration_to_seconds(warning_age),
    )


def _parse_check(
    raw_check: Any,
    prefix: str = "check",
    defaults: CheckConfig | None = None,
) -> CheckConfig:
    resolved_defaults = defaults or CheckConfig()
    if raw_check is None:
        return resolved_defaults
    if not isinstance(raw_check, dict):
        raise NormalizationError(f"{prefix} must be a mapping when provided.")

    fail_on_warning = raw_check.get("fail_on_warning", resolved_defaults.fail_on_warning)
    if not isinstance(fail_on_warning, bool):
        raise NormalizationError(f"{prefix}.fail_on_warning must be true or false.")

    return CheckConfig(fail_on_warning=fail_on_warning)


def _parse_rules(
    raw_rules: Any,
    prefix: str = "rules",
    defaults: RulesConfig | None = None,
) -> RulesConfig:
    resolved_defaults = defaults or RulesConfig()
    if raw_rules is None:
        return resolved_defaults
    if not isinstance(raw_rules, dict):
        raise NormalizationError(f"{prefix} must be a mapping when provided.")

    raw_null_rate = raw_rules.get("null_rate_change", {})
    if not isinstance(raw_null_rate, dict):
        raise NormalizationError(f"{prefix}.null_rate_change must be a mapping.")

    raw_row_count = raw_rules.get("row_count_change", {})
    if not isinstance(raw_row_count, dict):
        raise NormalizationError(f"{prefix}.row_count_change must be a mapping.")

    null_rate = NullRateChangeThresholds(
        warning_delta=_parse_fraction(
            raw_null_rate.get(
                "warning_delta", resolved_defaults.null_rate_change.warning_delta
            ),
            f"{prefix}.null_rate_change.warning_delta",
        ),
        error_delta=_parse_fraction(
            raw_null_rate.get(
                "error_delta", resolved_defaults.null_rate_change.error_delta
            ),
            f"{prefix}.null_rate_change.error_delta",
        ),
    )
    if null_rate.error_delta < null_rate.warning_delta:
        raise NormalizationError(
            f"{prefix}.null_rate_change.error_delta must be greater than or equal to warning_delta."
        )

    row_count = RowCountChangeThresholds(
        warning_drop_ratio=_parse_non_negative_number(
            raw_row_count.get(
                "warning_drop_ratio",
                resolved_defaults.row_count_change.warning_drop_ratio,
            ),
            f"{prefix}.row_count_change.warning_drop_ratio",
        ),
        error_drop_ratio=_parse_non_negative_number(
            raw_row_count.get(
                "error_drop_ratio",
                resolved_defaults.row_count_change.error_drop_ratio,
            ),
            f"{prefix}.row_count_change.error_drop_ratio",
        ),
        warning_growth_ratio=_parse_non_negative_number(
            raw_row_count.get(
                "warning_growth_ratio",
                resolved_defaults.row_count_change.warning_growth_ratio,
            ),
            f"{prefix}.row_count_change.warning_growth_ratio",
        ),
        error_growth_ratio=_parse_non_negative_number(
            raw_row_count.get(
                "error_growth_ratio",
                resolved_defaults.row_count_change.error_growth_ratio,
            ),
            f"{prefix}.row_count_change.error_growth_ratio",
        ),
    )
    if row_count.error_drop_ratio < row_count.warning_drop_ratio:
        raise NormalizationError(
            f"{prefix}.row_count_change.error_drop_ratio must be greater than or equal to warning_drop_ratio."
        )
    if row_count.error_growth_ratio < row_count.warning_growth_ratio:
        raise NormalizationError(
            f"{prefix}.row_count_change.error_growth_ratio must be greater than or equal to warning_growth_ratio."
        )

    return RulesConfig(null_rate_change=null_rate, row_count_change=row_count)


def _parse_freshness(
    source_name: str,
    raw_freshness: Any,
    schema: dict[str, str],
) -> FreshnessConfig | None:
    if raw_freshness is None:
        return None
    if isinstance(raw_freshness, str):
        raise NormalizationError(
            f"Source '{source_name}' uses shorthand freshness '{raw_freshness}', but v1 "
            "requires 'freshness: {column: ..., max_age: ...}'."
        )
    if not isinstance(raw_freshness, dict):
        raise NormalizationError(
            f"Source '{source_name}' freshness must be a mapping with 'column' and 'max_age'."
        )

    column = raw_freshness.get("column")
    max_age = raw_freshness.get("max_age")
    if not isinstance(column, str) or not column:
        raise NormalizationError(
            f"Source '{source_name}' freshness.column must be a non-empty string."
        )
    if column not in schema:
        raise NormalizationError(
            f"Source '{source_name}' freshness column '{column}' must be declared in schema."
        )
    validate_identifier(column, "column name")
    if not isinstance(max_age, str) or not max_age:
        raise NormalizationError(
            f"Source '{source_name}' freshness.max_age must be a non-empty string."
        )

    return FreshnessConfig(
        column=column,
        max_age=max_age,
        max_age_seconds=parse_duration_to_seconds(max_age),
    )


def _parse_scan(
    source_name: str,
    raw_scan: Any,
    schema: dict[str, str],
) -> ScanConfig | None:
    if raw_scan is None:
        return None
    if not isinstance(raw_scan, dict):
        raise NormalizationError(
            f"Source '{source_name}' scan must be a mapping with 'timestamp_column' and 'lookback'."
        )

    timestamp_column = raw_scan.get("timestamp_column")
    lookback = raw_scan.get("lookback")
    if not isinstance(timestamp_column, str) or not timestamp_column:
        raise NormalizationError(
            f"Source '{source_name}' scan.timestamp_column must be a non-empty string."
        )
    if timestamp_column not in schema:
        raise NormalizationError(
            f"Source '{source_name}' scan timestamp column '{timestamp_column}' must be declared in schema."
        )
    column_type = schema[timestamp_column]
    if column_type not in {"TIMESTAMP", "DATETIME", "DATE"}:
        raise NormalizationError(
            f"Source '{source_name}' scan timestamp column '{timestamp_column}' must use TIMESTAMP, DATETIME, or DATE type."
        )
    validate_identifier(timestamp_column, "column name")
    if not isinstance(lookback, str) or not lookback:
        raise NormalizationError(
            f"Source '{source_name}' scan.lookback must be a non-empty string."
        )

    return ScanConfig(
        timestamp_column=timestamp_column,
        column_type=column_type,
        lookback=lookback,
        lookback_seconds=parse_duration_to_seconds(lookback),
    )


def _parse_tests(
    source_name: str,
    raw_tests: Any,
    schema: dict[str, str],
) -> tuple[TestDefinition, ...]:
    if raw_tests is None:
        return ()
    if not isinstance(raw_tests, list):
        raise NormalizationError(f"Source '{source_name}' tests must be a list.")

    parsed_tests: list[TestDefinition] = []
    no_null_columns: set[str] = set()
    accepted_values_columns: set[str] = set()

    for index, raw_test in enumerate(raw_tests):
        if not isinstance(raw_test, dict) or len(raw_test) != 1:
            raise NormalizationError(
                f"Source '{source_name}' test at index {index} must be a one-item mapping."
            )
        test_name, test_value = next(iter(raw_test.items()))

        if test_name == "no_nulls":
            column = _parse_simple_column_reference(
                source_name=source_name,
                test_name=test_name,
                value=test_value,
                schema=schema,
            )
            if column in no_null_columns:
                raise NormalizationError(
                    f"Source '{source_name}' defines duplicate no_nulls test for '{column}'."
                )
            no_null_columns.add(column)
            parsed_tests.append(NoNullsTest(column=column))
            continue

        if test_name == "accepted_values":
            if not isinstance(test_value, dict):
                raise NormalizationError(
                    f"Source '{source_name}' accepted_values test must be a mapping."
                )
            column = _parse_simple_column_reference(
                source_name=source_name,
                test_name=test_name,
                value=test_value.get("column"),
                schema=schema,
            )
            if column in accepted_values_columns:
                raise NormalizationError(
                    f"Source '{source_name}' defines duplicate accepted_values test for '{column}'."
                )
            values = test_value.get("values")
            if not isinstance(values, list) or not values:
                raise NormalizationError(
                    f"Source '{source_name}' accepted_values for '{column}' must define a non-empty values list."
                )
            normalized_values = []
            for item in values:
                if not isinstance(item, str) or not item:
                    raise NormalizationError(
                        f"Source '{source_name}' accepted_values for '{column}' must contain only strings."
                    )
                normalized_values.append(item)
            accepted_values_columns.add(column)
            parsed_tests.append(
                AcceptedValuesTest(column=column, values=tuple(normalized_values))
            )
            continue

        raise NormalizationError(
            f"Source '{source_name}' uses unsupported test '{test_name}'."
        )

    return tuple(parsed_tests)


def _parse_simple_column_reference(
    source_name: str,
    test_name: str,
    value: Any,
    schema: dict[str, str],
) -> str:
    if not isinstance(value, str) or not value:
        raise NormalizationError(
            f"Source '{source_name}' {test_name} column must be a non-empty string."
        )
    if value not in schema:
        raise NormalizationError(
            f"Source '{source_name}' {test_name} column '{value}' must be declared in schema."
        )
    validate_identifier(value, "column name")
    return value


def _parse_fraction(value: Any, field_name: str) -> float:
    parsed = _parse_non_negative_number(value, field_name)
    if parsed > 1:
        raise NormalizationError(f"{field_name} must be between 0 and 1.")
    return parsed


def _parse_non_negative_number(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise NormalizationError(f"{field_name} must be a non-negative number.")
    parsed = float(value)
    if parsed < 0:
        raise NormalizationError(f"{field_name} must be a non-negative number.")
    return parsed


def _select_sources(config: ProjectConfig, source_names: tuple[str, ...]) -> ProjectConfig:
    if not source_names:
        return config

    requested = tuple(dict.fromkeys(source_names))
    missing = [name for name in requested if name not in config.sources]
    if missing:
        available = ", ".join(sorted(config.sources))
        raise ConfigError(
            f"Unknown source selection: {', '.join(missing)}. Available sources: {available}."
        )

    return ProjectConfig(
        warehouse=config.warehouse,
        sources={name: config.sources[name] for name in requested},
        rules=config.rules,
        baseline=config.baseline,
        check=config.check,
    )
