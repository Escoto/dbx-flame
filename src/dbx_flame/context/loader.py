"""Flat task parameters → nested dict → validated TaskConfig, and Context assembly."""

from __future__ import annotations

import enum
import types
from typing import TYPE_CHECKING, Any, Union, get_args, get_origin

from pydantic import BaseModel, ValidationError

from dbx_flame.context.config import IncrementStrategy, Origin, SnapshotScope, TaskConfig
from dbx_flame.context.context import Context, RunIdentity
from dbx_flame.observability.audit_logger import AuditLogger
from dbx_flame.output.registry import VERB_REQUIREMENTS
from dbx_flame.pipelines.preprocessors import PREPROCESSORS, RecordEnvelope
from dbx_flame.pipelines.registry import SOURCES
from dbx_flame.policies.checks import ChecksValidationError, load_checks

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from dbx_flame.output.base import Requirements

_RETIRED_PARAMS = {"source_type", "update_columns"}


class ConfigValidationError(Exception):
    """Raised when one or more configuration parameters are invalid."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__(
            "Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        )


def _unwrap_optional(annotation: Any) -> Any:
    origin = get_origin(annotation)
    if origin is Union or isinstance(annotation, types.UnionType):
        args = get_args(annotation)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return non_none[0]
    return annotation


def _unflatten(params: dict[str, str]) -> tuple[dict[str, Any], list[str]]:
    result: dict[str, Any] = {}
    errors: list[str] = []
    for key, value in params.items():
        parts = key.split(".")
        current = result
        conflict = False
        for i, part in enumerate(parts[:-1]):
            if part in current:
                if not isinstance(current[part], dict):
                    path = ".".join(parts[: i + 1])
                    errors.append(
                        f"Key conflict: '{path}' is used as both a value and a namespace."
                    )
                    conflict = True
                    break
                current = current[part]
            else:
                current[part] = {}
                current = current[part]
        if not conflict:
            leaf = parts[-1]
            if leaf in current and isinstance(current[leaf], dict):
                errors.append(f"Key conflict: '{key}' is used as both a value and a namespace.")
            else:
                current[leaf] = value
    return result, errors


_BOOLEAN_VALUES = {"true": True, "false": False}


def _coerce_bool(value: str) -> bool | None:
    """true/false in any casing. None means the value is not a boolean at all."""
    return _BOOLEAN_VALUES.get(value.strip().lower())


def _coerce_values(
    nested: dict[str, Any], model_class: type[BaseModel], path: str = ""
) -> tuple[dict[str, Any], list[str]]:
    result: dict[str, Any] = {}
    errors: list[str] = []
    for key, value in nested.items():
        full_path = f"{path}.{key}" if path else key

        if key not in model_class.model_fields:
            result[key] = value
            continue

        field_info = model_class.model_fields[key]
        annotation = _unwrap_optional(field_info.annotation)

        if (
            isinstance(value, dict)
            and isinstance(annotation, type)
            and issubclass(annotation, BaseModel)
        ):
            result[key], nested_errors = _coerce_values(value, annotation, full_path)
            errors.extend(nested_errors)
        elif isinstance(value, str):
            if annotation is bool:
                coerced = _coerce_bool(value)
                if coerced is None:
                    errors.append(f"{full_path}: expected true or false, got '{value}'")
                else:
                    result[key] = coerced
            elif get_origin(annotation) is list:
                if value.strip() == "":
                    result[key] = []
                else:
                    result[key] = [item.strip() for item in value.split(",")]
            elif isinstance(annotation, type) and issubclass(annotation, enum.StrEnum):
                result[key] = value.lower()
            else:
                result[key] = value
        else:
            result[key] = value
    return result, errors


def _collect_unknown_keys(
    nested: dict[str, Any], model_class: type[BaseModel], path: str = ""
) -> list[str]:
    errors: list[str] = []
    for key, value in nested.items():
        full_path = f"{path}.{key}" if path else key
        if key not in model_class.model_fields:
            errors.append(f"Unknown parameter: '{full_path}'")
        elif isinstance(value, dict):
            field_info = model_class.model_fields[key]
            annotation = _unwrap_optional(field_info.annotation)
            if isinstance(annotation, type) and issubclass(annotation, BaseModel):
                errors.extend(_collect_unknown_keys(value, annotation, full_path))
    return errors


def load_config(params: dict[str, str]) -> TaskConfig:
    """Parse flat key=value task parameters into a validated TaskConfig.

    Dotted keys are split into nested dicts. Coercion rules:
    - Booleans: true/false (any casing); any other value is an error
    - Lists: comma-separated strings
    - Enums: case-insensitive
    - Unknown keys: rejected

    All errors are aggregated into a single ConfigValidationError.
    """
    errors: list[str] = []

    for key in params:
        if key in _RETIRED_PARAMS:
            errors.append(
                f"Retired parameter '{key}' is no longer supported; "
                "remove it from your workflow YAML."
            )

    clean_params = {k: v for k, v in params.items() if k not in _RETIRED_PARAMS}

    nested, conflict_errors = _unflatten(clean_params)
    errors.extend(conflict_errors)

    errors.extend(_collect_unknown_keys(nested, TaskConfig))

    coerced, coercion_errors = _coerce_values(nested, TaskConfig)
    errors.extend(coercion_errors)

    try:
        config = TaskConfig(**coerced)
    except ValidationError as e:
        for err in e.errors():
            loc = ".".join(str(x) for x in err["loc"])
            msg = err["msg"].removeprefix("Value error, ")
            errors.append(f"{loc}: {msg}")
        config = None

    if errors:
        raise ConfigValidationError(errors)

    return config


def _increment_errors(config: TaskConfig, reqs: Requirements, verb: str) -> list[str]:
    """Check the increment strategy and its anchor against what the verb supports.

    Every rule here guards silent data loss rather than a crash, which is why they
    are refusals and not warnings.
    """
    errors: list[str] = []
    chosen = config.source.increment_strategy

    if chosen and chosen not in reqs.increment_strategies:
        supported = ", ".join(s.value for s in reqs.increment_strategies)
        return [
            f"output.verb={verb} does not support source.increment_strategy="
            f"{chosen.value} (supported: {supported})"
        ]

    strategy = chosen or reqs.increment_strategy

    origin = config.source.origin
    readable = SOURCES[origin].increment_strategies
    # An origin the verb refuses is already reported; naming its strategy too would
    # only repeat the same mistake.
    if origin in reqs.origins and strategy not in readable:
        supported = ", ".join(s.value for s in readable)
        return [
            f"source.origin={origin.value} does not support source.increment_strategy="
            f"{strategy.value} (supported: {supported})"
        ]

    if strategy == IncrementStrategy.DELTA_READ:
        errors.extend(_delta_read_errors(config))

    if config.source.increment_anchor and strategy != IncrementStrategy.WATERMARK:
        errors.append(
            "source.increment_anchor only applies to source.increment_strategy=watermark, "
            f"not {strategy.value}"
        )

    if config.source.increment_anchor and config.output.snapshot_scope == SnapshotScope.FULL:
        # The anchor reads only the records that moved, and full scope expires
        # everything the batch does not carry: every unchanged record would vanish.
        errors.append(
            "source.increment_anchor cannot be combined with output.snapshot_scope=full: "
            "a full export must be read whole, or the records it still carries are expired"
        )

    return errors


def _delta_read_errors(config: TaskConfig) -> list[str]:
    """A delta_read hands on only the rows that changed, found through one typed column."""
    errors: list[str] = []
    anchor = config.source.anchor_dt

    if anchor is None:
        errors.append(
            "source.increment_strategy=delta_read requires source.anchor_dt.column: "
            "the source column that records when each row last changed"
        )
    elif anchor.format:
        # Parsing a string per row would keep the filter from reaching the source.
        errors.append(
            "source.anchor_dt.format does not apply to source.increment_strategy=delta_read: "
            "the column must be a DATE or TIMESTAMP, so its filter reaches the source"
        )

    if config.output.snapshot_scope == SnapshotScope.FULL:
        errors.append(
            "source.increment_strategy=delta_read cannot be combined with "
            "output.snapshot_scope=full: it reads only the rows that changed, so every "
            "other one would be expired"
        )

    if config.source.deletes_table:
        # The target's highest anchor says nothing about which deletes were applied.
        errors.append(
            "source.increment_strategy=delta_read does not read a deletes feed: land "
            "source.deletes_table in Bronze with its own full_read task and apply it from there"
        )

    return errors


def validate_requirements(config: TaskConfig) -> list[str]:
    """Check the configured origin x verb against what the verb's writer declares.

    Returns one message per unmet requirement (empty list when the combination is
    valid) so the caller can aggregate these with any other configuration errors.
    """
    reqs = VERB_REQUIREMENTS[config.output.verb]
    verb = config.output.verb.value
    errors: list[str] = []

    if config.source.origin not in reqs.origins:
        supported = ", ".join(sorted(origin.value for origin in reqs.origins))
        errors.append(
            f"output.verb={verb} does not support source.origin="
            f"{config.source.origin.value} (supported: {supported})"
        )

    if reqs.keys and not config.output.keys:
        errors.append(f"output.verb={verb} requires output.keys")

    dedup = config.output.dedup
    if reqs.event_time and not config.output.event_time:
        errors.append(f"output.verb={verb} requires output.event_time.column")
    elif reqs.keys and dedup.enabled and not (dedup.order_by or config.output.event_time):
        # Without an order, "latest row per key" would keep an arbitrary one.
        errors.append(
            f"output.verb={verb} deduplicates by default and needs an order: set "
            "output.event_time.column or output.dedup.order_by, or output.dedup.enabled=false"
        )

    deletes = config.output.deletes
    deletes_configured = config.source.deletes_table or deletes
    if deletes_configured and not reqs.supports_deletes:
        errors.append(
            f"output.verb={verb} does not support a deletes feed "
            "(source.deletes_table / output.deletes.*)"
        )
    elif deletes_configured and not (
        config.source.deletes_table and deletes and deletes.keys and deletes.event_time
    ):
        # Half a deletes feed silently retires nothing, which looks like success.
        errors.append(
            "a deletes feed needs source.deletes_table, output.deletes.keys and "
            "output.deletes.event_time.column together"
        )

    if config.output.snapshot_scope == SnapshotScope.FULL and not reqs.supports_snapshot_scope:
        errors.append(f"output.verb={verb} does not support output.snapshot_scope=full")

    errors.extend(_increment_errors(config, reqs, verb))

    unknown = [name for name in config.source.preprocessors if name not in PREPROCESSORS]
    if unknown:
        errors.append(
            f"source.preprocessors has no registered pre-processor: {', '.join(unknown)} "
            f"(registered: {', '.join(sorted(PREPROCESSORS))})"
        )

    errors.extend(_envelope_errors(config))
    errors.extend(_metadata_errors(config))

    return errors


def _metadata_errors(config: TaskConfig) -> list[str]:
    """Checkpoints and schema locations live in the consumer's catalog: the target's.

    Whoever consumes a source tracks that consumption, so a task reading another catalog
    never keeps its progress there.
    """
    consumer = _env_catalog(config.catalog, config.env)
    parts = config.metadata_path.strip("/").split("/")
    if len(parts) > 1 and parts[0] == "Volumes" and parts[1].lower() == consumer.lower():
        return []
    return [
        f"metadata_path must be a Volume in the target's catalog (/Volumes/{consumer}/...), "
        f"got '{config.metadata_path}'"
    ]


def _envelope_errors(config: TaskConfig) -> list[str]:
    """The envelope and its field list are only meaningful together.

    Either half alone is a silent no-op: fields with no envelope are never read, and an
    envelope with no fields yields a batch with no key column for the merge to run on.
    """
    envelope = RecordEnvelope.name in config.source.preprocessors
    fields = config.source.envelope_fields

    if fields and not envelope:
        return [
            f"source.envelope_fields requires source.preprocessors to include "
            f"'{RecordEnvelope.name}'"
        ]
    if envelope and not fields:
        return [
            f"source.preprocessors='{RecordEnvelope.name}' requires source.envelope_fields "
            "(at least the key, e.g. 'uri')"
        ]
    return []


def _posix_join(*parts: str) -> str:
    """Join Volume path segments with '/', whatever platform this runs on.

    os.path.join would emit backslashes on Windows, and these are remote POSIX paths.
    """
    joined = "/".join(part.strip("/") for part in parts if part)
    return f"/{joined}" if parts and parts[0].startswith("/") else joined


def _any_case(extension: str) -> str:
    """'csv' -> '[cC][sS][vV]': the glob is case-sensitive, and vendors send '.CSV' too."""
    return "".join(f"[{c.lower()}{c.upper()}]" if c.isalpha() else c for c in extension)


def _backtick_fqn(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


def _env_catalog(catalog: str, env: str) -> str:
    return f"{catalog}_{env}"


def build_context(
    config: TaskConfig,
    spark: SparkSession,
    run: RunIdentity,
    _audit_table_override: str | None = None,
) -> Context:
    """Resolve derived fields from config and assemble a frozen Context.

    Validates origin × verb requirements and raises ConfigValidationError
    if the configuration is insufficient for the chosen verb.
    """
    errors = validate_requirements(config)

    checks: list[dict] = []
    if config.policies.checks_file:
        try:
            checks = load_checks(config.policies.checks_file)
        except ChecksValidationError as exc:
            errors.extend(exc.errors)

    if errors:
        raise ConfigValidationError(errors)

    catalog = _env_catalog(config.catalog, config.env)

    target_table = _backtick_fqn(catalog, config.output.schema_name, config.output.table)

    # One branch per origin: SourceConfig has already guaranteed that the fields
    # each branch needs are present, which is what the asserts record.
    source_table: str | None = None
    deletes_table: str | None = None
    inbound_glob: str | None = None
    source = config.source

    if source.origin == Origin.TABLE:
        schema_name, table = source.schema_name, source.table
        assert schema_name and table  # guaranteed by SourceConfig
        source_catalog = _env_catalog(source.catalog, config.env) if source.catalog else catalog
        source_table = _backtick_fqn(source_catalog, schema_name, table)
        if source.deletes_table:
            deletes_table = _backtick_fqn(source_catalog, schema_name, source.deletes_table)
    else:
        path, directory = source.path, source.directory
        assert path and directory  # guaranteed by SourceConfig
        extension = source.file_extension or source.origin.value
        inbound_glob = _posix_join(path, directory, f"*.{_any_case(extension)}")

    base_metadata = _posix_join(
        config.metadata_path, catalog, config.output.schema_name, config.output.table
    )
    checkpoint_location = f"{base_metadata}/_checkpoint/"
    schema_hints_location = f"{base_metadata}/_schema_hints/"

    logger = AuditLogger(
        spark,
        config.env,
        target_table,
        run,
        _audit_table_override=_audit_table_override,
    )

    return Context(
        config=config,
        spark=spark,
        run=run,
        logger=logger,
        catalog=catalog,
        source_table=source_table,
        deletes_table=deletes_table,
        target_table=target_table,
        inbound_glob=inbound_glob,
        checkpoint_location=checkpoint_location,
        schema_hints_location=schema_hints_location,
        increment_strategy=(
            config.source.increment_strategy
            or VERB_REQUIREMENTS[config.output.verb].increment_strategy
        ),
        checks=checks,
    )
