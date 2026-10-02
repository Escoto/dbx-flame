"""Enrichment — provenance columns, column sanitization, rename patterns."""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from dbx_flame.context.config import SnapshotTimePattern
from dbx_flame.policies.platform import PlatformPolicy, violate

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from dbx_flame.context.context import Context

# The export stamps a file name may carry, and how each parses. The digit boundaries
# keep a longer run of digits from being read as a stamp.
_PATTERNS: dict[SnapshotTimePattern, tuple[str, str]] = {
    SnapshotTimePattern.DATETIME: (r"(?<!\d)(\d{14})(?!\d)", "yyyyMMddHHmmss"),
    SnapshotTimePattern.ISO: (
        r"(?<!\d)(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?!\d)",
        "yyyy-MM-dd'T'HH:mm:ss",
    ),
}

# Characters a Delta column name cannot carry without column mapping, plus '.' and '/',
# which read as nested-field and path separators.
_UNSAFE_CHARS = re.compile(r"[ ,;{}()\n\t=./]")

ANCHOR_DT = "__ANCHOR_DT"

# Auto Loader's catch-all for values that did not fit the inferred schema.
_RESCUED_COLUMN = "_rescued_data"

_SOURCE = "enrichment"


def _export_date(file_path: Column, pattern: SnapshotTimePattern) -> Column:
    """NULL when the name doesn't match; reject_unstamped turns that into a failure.

    Only the file name is searched, so a stamp-shaped folder can't stand in for the file's own.
    """
    regex, fmt = _PATTERNS[pattern]
    file_name = F.element_at(F.split(file_path, "/"), -1)
    return F.to_timestamp(F.regexp_extract(file_name, regex, 1), fmt)


def add_provenance(df: DataFrame, ctx: Context) -> DataFrame:
    """Add __bronze_last_modified_dt, __filePath and __EXPORT_DATE.

    Runs before sanitization, so the patterns match the source's own casing and are
    uppercased into the metadata contract (__FILEPATH, __EXPORT_DATE, ...) there.
    """
    pattern = ctx.config.source.snapshot_time_pattern or SnapshotTimePattern.DATETIME
    file_path = F.col("_metadata.file_path")
    return (
        df.withColumn("__bronze_last_modified_dt", F.current_timestamp())
        .withColumn("__filePath", file_path)
        .withColumn("__EXPORT_DATE", _export_date(file_path, pattern))
    )


def reject_unstamped(df: DataFrame, ctx: Context) -> None:
    """Fail the batch before anything is written if a file name lacks the agreed stamp.

    The file is refused rather than loaded without an __EXPORT_DATE: every watermark
    would skip it silently, and Bronze would need cleaning by hand.
    """
    unstamped = df.filter(F.col("__EXPORT_DATE").isNull()).select("__filePath").first()
    if unstamped is None:
        return

    pattern = ctx.config.source.snapshot_time_pattern or SnapshotTimePattern.DATETIME
    message = (
        f"{unstamped[0]} doesn't carry a source.snapshot_time_pattern={pattern.value} stamp "
        f"(expected {_PATTERNS[pattern][1]}); nothing from this batch was written"
    )
    violate(ctx, PlatformPolicy.UNSTAMPED_FILE, _SOURCE, message)


def sanitize_column_names(df: DataFrame, ctx: Context) -> DataFrame:
    """Replace Delta-invalid characters with '_', uppercase every name, drop _rescued_data.

    Distinct source names can collide once renamed ('Id'/'ID', 'a(b'/'a_b'); the write then fails.
    """
    kept = [name for name in df.columns if name != _RESCUED_COLUMN]
    renames = {name: _UNSAFE_CHARS.sub("_", name).upper() for name in kept}

    changed = {old: new for old, new in renames.items() if old != new}
    if changed:
        ctx.logger.info(
            name="columns_sanitized",
            source=_SOURCE,
            description=f"Sanitized {len(changed)} column name(s)",
            total=len(changed),
            metadata=json.dumps(changed, sort_keys=True),
        )

    return _select_renamed(df, renames)


def apply_rename_patterns(df: DataFrame, patterns: list[str]) -> DataFrame:
    """Apply 'regex=replacement' renames, in order, to every column name.

    SourceConfig has already checked the shape and that each regex compiles.
    """
    if not patterns:
        return df

    rules = []
    for pattern in patterns:
        expression, _, replacement = pattern.partition("=")
        rules.append((re.compile(expression), replacement))

    renames = {}
    for name in df.columns:
        renamed = name
        for compiled, replacement in rules:
            renamed = compiled.sub(replacement, renamed)
        renames[name] = renamed

    return _select_renamed(df, renames)


def _select_renamed(df: DataFrame, renames: dict[str, str]) -> DataFrame:
    """Rename (and implicitly drop) in a single projection.

    Names are backticked: an unsanitized column containing '.' would otherwise
    read as nested-field access.
    """
    return df.select([F.col(f"`{old}`").alias(new) for old, new in renames.items()])


def stamp_anchor(df: DataFrame, ctx: Context) -> DataFrame:
    """Copy source.anchor_dt into __ANCHOR_DT as a timestamp.

    Runs after typing, so a column the cast config already typed is copied as is. An
    unparseable value fails the batch, like a silent NULL from a cast. A NULL is only
    logged: an anchored read will never pick that row up.
    """
    anchor = ctx.config.source.anchor_dt
    if anchor is None:
        return df

    kind = dict(df.dtypes).get(anchor.column)
    if kind is None:
        violate(
            ctx,
            PlatformPolicy.ANCHOR_COLUMN_MISSING,
            _SOURCE,
            f"source.anchor_dt.column={anchor.column} is not in the batch",
        )

    original = F.col(f"`{anchor.column}`")
    if kind == "string":
        parsed = (
            F.to_timestamp(original, anchor.format) if anchor.format else F.to_timestamp(original)
        )
        unparsed = original.isNotNull() & (F.trim(original) != "") & parsed.isNull()
    else:
        parsed = original.cast("timestamp")
        unparsed = original.isNotNull() & parsed.isNull()

    stamped = df.withColumn(ANCHOR_DT, parsed)
    found = stamped.agg(
        F.first(F.when(unparsed, original.cast("string")), ignorenulls=True).alias("example"),
        F.count(F.when(F.col(ANCHOR_DT).isNull() & ~unparsed, True)).alias("missing"),
    ).collect()[0]

    if found["example"] is not None:
        message = (
            f"{anchor.column} does not parse as a timestamp"
            f"{f' with format {anchor.format}' if anchor.format else ''} "
            f"(e.g. {found['example']})"
        )
        violate(ctx, PlatformPolicy.ANCHOR_UNPARSEABLE, _SOURCE, message)

    if found["missing"]:
        ctx.logger.warning(
            name="anchor_missing",
            source=_SOURCE,
            description=f"{found['missing']} row(s) have no {anchor.column}; "
            "an anchored read never picks them up",
            total=found["missing"],
        )
    return stamped
