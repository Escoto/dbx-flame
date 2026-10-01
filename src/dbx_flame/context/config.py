"""Pydantic models for the typed TaskConfig and nested sub-models."""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Optional

from pydantic import BaseModel, field_validator, model_validator


class Origin(StrEnum):
    CSV = "csv"
    JSON = "json"
    SAS = "sas"
    DELTA = "delta"


FILE_ORIGINS = frozenset({Origin.CSV, Origin.JSON, Origin.SAS})


class Verb(StrEnum):
    APPEND = "append"
    FULL = "full"
    UPSERT = "upsert"
    SCD2 = "scd2"
    COMPLETE_DELTA = "complete_delta"


class SchemaEvolution(StrEnum):
    """Auto Loader's cloudFiles.schemaEvolutionMode, one value per documented mode."""

    ADD_NEW_COLUMNS = "add_new_columns"
    ADD_NEW_COLUMNS_WITH_TYPE_WIDENING = "add_new_columns_with_type_widening"
    FAIL_ON_NEW_COLUMNS = "fail_on_new_columns"
    NONE = "none"


class SnapshotTimePattern(StrEnum):
    """The export stamp a file name must carry, as agreed with the source."""

    DATETIME = "datetime"  # 20260930103500
    ISO = "iso"  # 2026-09-30T10:35:00


class IncrementStrategy(StrEnum):
    CHECKPOINT = "checkpoint"
    WATERMARK = "watermark"


class Severity(StrEnum):
    WARN = "warn"
    FAIL = "fail"


class SnapshotScope(StrEnum):
    DELTA = "delta"
    FULL = "full"


class EventTimeConfig(BaseModel):
    """A date column, plus the format to parse it with when it is stored as a string."""

    column: str
    format: Optional[str] = None


class SourceOptions(BaseModel):
    header: bool = True
    delimiter: str = ","
    quote: str = '"'
    escape: str = "\\"
    multiline: bool = True

    # Auto Loader schema hints, e.g. "metadata STRING, data STRING". Typing a column as
    # STRING keeps the reader out of it, so a vendor reshaping it is not a schema change.
    schema_hints: Optional[str] = None


class SourceConfig(BaseModel):
    origin: Origin
    path: Optional[str] = None
    directory: Optional[str] = None
    file_extension: Optional[str] = None
    # File origins only; unset means datetime. A file whose name doesn't carry it fails
    # the run rather than landing in Bronze without an __EXPORT_DATE.
    snapshot_time_pattern: Optional[SnapshotTimePattern] = None
    preprocessors: list[str] = []

    # JSON paths lifted out of each record_envelope item into their own columns. The
    # payload itself stays whole under DATA, so only these need to be stable.
    envelope_fields: list[str] = []
    rename_patterns: list[str] = []
    options: SourceOptions = SourceOptions()
    schema_name: Optional[str] = None
    table: Optional[str] = None
    deletes_table: Optional[str] = None

    # Each verb declares which strategies it supports and which is its default
    # (output/base.py); this only picks between them, and only where the verb
    # allows more than one. build_context resolves the result onto the Context.
    increment_strategy: Optional[IncrementStrategy] = None

    # Bronze only: the per-record date copied into __ANCHOR_DT as a timestamp, so a
    # later watermark compares a real column rather than parsing a string per read.
    anchor_dt: Optional[EventTimeConfig] = None

    # The watermark compares __ANCHOR_DT instead of __EXPORT_DATE, which tracks when a
    # file arrived. Use it when the source sends unchanged snapshots, so an idle run
    # reads nothing rather than the whole backlog since the target last changed.
    increment_anchor: bool = False

    @field_validator("file_extension")
    @classmethod
    def _extension_is_bare(cls, value: Optional[str]) -> Optional[str]:
        # '.csv' would build the glob '*..csv', which matches nothing and still succeeds.
        if value is None:
            return None
        bare = value.strip().removeprefix(".")
        if not bare.isascii() or not bare.isalnum():
            raise ValueError(f"expected letters and digits only, e.g. 'txt', got '{value}'")
        return bare.lower()

    @model_validator(mode="after")
    def _require_fields_for_origin(self) -> "SourceConfig":
        """Fail here rather than resolving Context paths to None downstream."""
        problems: list[str] = []

        if self.origin in FILE_ORIGINS:
            missing = [
                name
                for name, value in (
                    ("source.path", self.path),
                    ("source.directory", self.directory),
                )
                if not value
            ]
        else:
            missing = [
                name
                for name, value in (
                    ("source.schema_name", self.schema_name),
                    ("source.table", self.table),
                )
                if not value
            ]
        if missing:
            problems.append(f"source.origin={self.origin.value} requires: {', '.join(missing)}")

        if self.deletes_table and self.origin != Origin.DELTA:
            problems.append("source.deletes_table requires source.origin=delta")

        if self.snapshot_time_pattern and self.origin not in FILE_ORIGINS:
            # A delta origin reads the __EXPORT_DATE its Bronze already parsed.
            problems.append("source.snapshot_time_pattern applies to file origins only")

        if self.anchor_dt and self.origin not in FILE_ORIGINS:
            # Written once at ingestion; a delta origin carries the column its Bronze wrote.
            problems.append("source.anchor_dt applies to file origins only (csv, json, sas)")

        if problems:
            raise ValueError("; ".join(problems))
        return self

    @field_validator("rename_patterns")
    @classmethod
    def _patterns_are_regex_and_replacement(cls, patterns: list[str]) -> list[str]:
        """Each entry is 'regex=replacement'.

        Without the '=' check a typo silently means "delete every match", and an
        invalid regex would only surface once the pipeline reaches enrichment.
        """
        problems: list[str] = []
        for pattern in patterns:
            if "=" not in pattern:
                problems.append(f"'{pattern}' is missing '='")
                continue
            regex = pattern.split("=", 1)[0]
            try:
                re.compile(regex)
            except re.error as exc:
                problems.append(f"'{regex}' is not a valid regex ({exc})")
        if problems:
            raise ValueError("; ".join(problems))
        return patterns


class DedupConfig(BaseModel):
    """Empty columns and order_by fall back to output.keys and output.event_time."""

    enabled: bool = True
    columns: list[str] = []
    order_by: Optional[str] = None
    order_by_format: Optional[str] = None


class DeletesConfig(BaseModel):
    keys: list[str] = []
    event_time: Optional[EventTimeConfig] = None


class OutputConfig(BaseModel):
    verb: Verb
    schema_name: str
    table: str
    keys: list[str] = []
    event_time: Optional[EventTimeConfig] = None
    snapshot_scope: SnapshotScope = SnapshotScope.DELTA
    dedup: DedupConfig = DedupConfig()
    deletes: Optional[DeletesConfig] = None

    @field_validator("table")
    @classmethod
    def _table_must_be_uppercase(cls, value: str) -> str:
        if value != value.upper():
            raise ValueError(f"table names must be UPPERCASE, got '{value}'")
        return value


class PoliciesConfig(BaseModel):
    """Where the data quality rules live, not what they are.

    The rules themselves are a DQX ruleset in its own file; severity is a property of
    each check there, so nothing about them belongs in the task parameters.
    """

    checks_file: Optional[str] = None


class TypingConfig(BaseModel):
    cast_config: Optional[str] = None
    validate_casts: bool = True


class TaskConfig(BaseModel):
    catalog: str
    env: str
    metadata_path: str

    # Decide how the reader and writer react to new columns.
    schema_evolution: SchemaEvolution = SchemaEvolution.FAIL_ON_NEW_COLUMNS
    source: SourceConfig
    typing: TypingConfig = TypingConfig()
    policies: PoliciesConfig = PoliciesConfig()
    output: OutputConfig
