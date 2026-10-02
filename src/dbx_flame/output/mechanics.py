"""Write mechanics shared by the Delta verbs: schema evolution, dedup, promotion."""

from __future__ import annotations

import json
from contextlib import contextmanager
from functools import reduce
from operator import and_
from typing import TYPE_CHECKING

from pyspark.sql import Window
from pyspark.sql import functions as F

from dbx_flame.context.config import EventTimeConfig, SchemaEvolution
from dbx_flame.policies.platform import PlatformPolicy, violate

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime

    from pyspark.sql import Column, DataFrame

    from dbx_flame.context.context import Context

BRONZE_TIMESTAMP = "__BRONZE_LAST_MODIFIED_DT"
SILVER_TIMESTAMP = "__SILVER_LAST_MODIFIED_DT"
FILE_PATH = "__FILEPATH"
EXPORT_DATE = "__EXPORT_DATE"

# History columns, written by SCD2 and COMPLETE_DELTA only.
START_DATE = "__START_DATE"
END_DATE = "__END_DATE"
CURRENT_FLAG = "__CURRENT_FLAG"
DELETED_FLAG = "__DELETED_FLAG"

CURRENT = "Y"
EXPIRED = "N"
DELETED = "Y"
LIVE = "N"

_DEDUP_RANK = "_dedup_rank"

_SOURCE = "mechanics"


# The modes under which a batch can arrive carrying a column the target lacks.
_EVOLVING = frozenset(
    {
        SchemaEvolution.ADD_NEW_COLUMNS,
        SchemaEvolution.ADD_NEW_COLUMNS_WITH_TYPE_WIDENING,
    }
)


# Delta's MERGE ignores the mergeSchema write option; this session conf is its only
# equivalent. Without it a MERGE does not refuse an unexpected column — it drops it.
_AUTO_MERGE = "spark.databricks.delta.schema.autoMerge.enabled"


def merge_schema(ctx: Context) -> str:
    """mergeSchema follows the declared schema evolution.

    Listed explicitly rather than compared against one mode: a mode that lets the reader
    grow the batch needs the write to accept the growth, and an equality test would
    quietly answer "false" and refuse the very column the reader just added.
    """
    return "true" if ctx.config.schema_evolution in _EVOLVING else "false"


def require_no_new_columns(df: DataFrame, ctx: Context) -> None:
    """Refuse a batch carrying columns the target lacks, when evolution is off.

    Every other verb gets this refusal from Delta itself, because mergeSchema=false
    rejects the write. A MERGE has no such option: with autoMerge off it accepts the
    batch and discards the unmatched column. Checking by hand here is what makes
    schema_evolution mean the same thing on an UPSERT as it does everywhere else.
    """
    if merge_schema(ctx) == "true":
        return

    existing = set(ctx.spark.table(ctx.target_table).columns)
    unexpected = [column for column in df.columns if column not in existing]
    if unexpected:
        violate(
            ctx,
            PlatformPolicy.UNEXPECTED_COLUMNS,
            _SOURCE,
            f"{ctx.target_table} has no column "
            + ", ".join(unexpected)
            + f"; schema_evolution={ctx.config.schema_evolution.value} does not add them",
        )


@contextmanager
def schema_auto_merge(ctx: Context) -> Iterator[None]:
    """Apply the same evolution decision to a MERGE, then put the session back.

    Restoring matters because this is session state, not a property of one write: a
    job cluster runs many tasks in one session, and leaving the conf set would hand the
    next task an evolution policy it never asked for. get() raises when the key was
    never set, so the previous value is captured with a default and unset on the way out.
    """
    previous = ctx.spark.conf.get(_AUTO_MERGE, None)
    ctx.spark.conf.set(_AUTO_MERGE, merge_schema(ctx))
    try:
        yield
    finally:
        if previous is None:
            ctx.spark.conf.unset(_AUTO_MERGE)
        else:
            ctx.spark.conf.set(_AUTO_MERGE, previous)


def as_timestamp(column: str, fmt: str | None, alias: str | None = None) -> Column:
    """Normalize a date column for comparison without persisting a helper column.

    The format is passed to to_timestamp as a value, never interpolated into SQL text,
    so merge conditions stay free of string-built predicates.
    """
    value = F.col(f"{alias}.`{column}`" if alias else f"`{column}`")
    return F.to_timestamp(value, fmt) if fmt else value.cast("timestamp")


def compared_times(ctx: Context) -> list[tuple[str, EventTimeConfig]]:
    """The date columns a keyed verb compares or orders rows by, with their setting."""
    output = ctx.config.output
    times = []
    if output.event_time:
        times.append(("output.event_time.column", output.event_time))
    if output.dedup.enabled and output.dedup.order_by:
        order = EventTimeConfig(column=output.dedup.order_by, format=output.dedup.order_by_format)
        times.append(("output.dedup.order_by", order))
    return times


def require_valid_times(
    df: DataFrame, ctx: Context, times: list[tuple[str, EventTimeConfig]]
) -> None:
    """Fail the batch if a compared date is missing or won't parse, before any write.

    A NULL doesn't drop the row, it falls through every comparison: SCD2 opens a second
    current row, UPSERT skips the update and dedup keeps an arbitrary row.
    """
    if not times:
        return

    absent = [
        f"{setting}={time.column}" for setting, time in times if time.column not in df.columns
    ]
    if absent:
        violate(
            ctx,
            PlatformPolicy.EVENT_TIME_INVALID,
            _SOURCE,
            f"{', '.join(absent)} is not in the batch",
        )

    checks = []
    for i, (_, time) in enumerate(times):
        raw = F.col(f"`{time.column}`")
        missing = raw.isNull() | (F.trim(raw.cast("string")) == "")
        unparsed = ~missing & as_timestamp(time.column, time.format).isNull()
        checks += [
            F.count(F.when(missing, True)).alias(f"missing_{i}"),
            F.count(F.when(unparsed, True)).alias(f"unparsed_{i}"),
            F.first(F.when(unparsed, raw.cast("string")), ignorenulls=True).alias(f"example_{i}"),
        ]
    found = df.agg(*checks).collect()[0]

    problems, offenders = [], {}
    for i, (setting, time) in enumerate(times):
        missing, unparsed, example = (
            found[f"{k}_{i}"] for k in ("missing", "unparsed", "example")
        )
        if not (missing or unparsed):
            continue
        offenders[setting] = {"column": time.column, "missing": missing, "unparseable": unparsed}
        parts = [f"{missing} row(s) missing"] if missing else []
        if unparsed:
            with_format = f" with format {time.format}" if time.format else ""
            parts.append(f"{unparsed} row(s) don't parse{with_format} (e.g. {example})")
        problems.append(f"{setting}={time.column}: {', '.join(parts)}")

    if problems:
        violate(
            ctx,
            PlatformPolicy.EVENT_TIME_INVALID,
            _SOURCE,
            "; ".join(problems) + "; nothing from this batch was written",
            total=sum(o["missing"] + o["unparseable"] for o in offenders.values()),
            metadata=json.dumps(offenders, sort_keys=True),
        )


def require_creatable(df: DataFrame, ctx: Context) -> None:
    """A first run with no columns cannot define the target — fail rather than guess."""
    if df.columns:
        return
    if ctx.spark.catalog.tableExists(ctx.target_table):
        return
    violate(
        ctx,
        PlatformPolicy.EMPTY_SOURCE_SCHEMA,
        _SOURCE,
        f"Cannot create {ctx.target_table}: the source produced no columns",
    )


def deduplicate(df: DataFrame, ctx: Context) -> DataFrame:
    """Keep the latest row per key within the batch, unless disabled.

    On by default because the keyed verbs assume one row per key per batch; a second
    one opens a duplicate current row. Ordering uses an expression rather than a
    derived column, so nothing extra is written to the target.
    """
    output = ctx.config.output
    dedup = output.dedup
    if not dedup.enabled:
        return df

    columns = dedup.columns or output.keys
    if dedup.order_by:
        order = as_timestamp(dedup.order_by, dedup.order_by_format)
    else:
        assert output.event_time  # guaranteed at Start when dedup is enabled
        order = as_timestamp(output.event_time.column, output.event_time.format)

    # A backlog often resends a key with the same event time; the newer export must win
    # rather than an arbitrary one.
    orders = [order.desc()]
    if EXPORT_DATE in df.columns:
        orders.append(F.col(EXPORT_DATE).desc())
    window = Window.partitionBy(*[F.col(f"`{c}`") for c in columns]).orderBy(*orders)
    return (
        df.withColumn(_DEDUP_RANK, F.row_number().over(window))
        .filter(F.col(_DEDUP_RANK) == 1)
        .drop(_DEDUP_RANK)
    )


def latest_export(df: DataFrame) -> datetime | None:
    """The newest __EXPORT_DATE in df, or None when it carries no stamp."""
    if EXPORT_DATE not in df.columns:
        return None
    return df.agg(F.max(EXPORT_DATE)).collect()[0][0]


def promote(df: DataFrame) -> DataFrame:
    """Stamp the framework's write time and drop the previous layer's."""
    return df.withColumn(SILVER_TIMESTAMP, F.current_timestamp()).drop(BRONZE_TIMESTAMP)


def key_condition(keys: list[str], source: str, target: str) -> Column:
    """The join predicate for a merge, as a Column rather than built SQL text."""
    return reduce(
        and_,
        [F.col(f"{source}.`{key}`") == F.col(f"{target}.`{key}`") for key in keys],
    )


def open_history(df: DataFrame, start: Column) -> DataFrame:
    """Stamp a batch as the current, live version of each record it carries.

    The validity window opens at the record's own event time rather than at write
    time, so replaying a backlog reconstructs the real history instead of collapsing
    it onto the moment the job happened to run.
    """
    return (
        df.withColumn(START_DATE, start)
        .withColumn(END_DATE, F.lit(None).cast("timestamp"))
        .withColumn(CURRENT_FLAG, F.lit(CURRENT))
        .withColumn(DELETED_FLAG, F.lit(LIVE))
    )


def log_rows_written(ctx: Context, event: str, source: str) -> None:
    """Report the row count from Delta's own commit metrics.

    Counting the batch would mean a second pass over the source; the table's last
    commit already knows exactly how many rows landed.
    """
    history = ctx.spark.sql(f"DESCRIBE HISTORY {ctx.target_table} LIMIT 1").collect()
    metrics = (history[0]["operationMetrics"] or {}) if history else {}
    written = metrics.get("numOutputRows") or metrics.get("numTargetRowsInserted") or 0

    ctx.logger.kpi(
        name=event, total=int(written), description=f"{source} wrote to {ctx.target_table}"
    )
