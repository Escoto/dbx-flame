"""SCD2 verb — close-and-insert history tracking with dedup and anti-filter.

The merge engine is exposed as `merge_history` because COMPLETE_DELTA replays it once
per snapshot. It takes a plain batch DataFrame and performs no streaming of its own, so
the same code runs inside foreachBatch and inside a replay loop.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from dbx_flame.context.config import IncrementStrategy, SnapshotScope, Verb
from dbx_flame.observability.kpi import Kpi
from dbx_flame.output.base import Requirements
from dbx_flame.output.mechanics import (
    CURRENT,
    CURRENT_FLAG,
    DELETED_FLAG,
    END_DATE,
    EXPIRED,
    LIVE,
    as_timestamp,
    key_condition,
    log_rows_written,
    merge_schema,
    open_history,
    promote,
    require_creatable,
    require_no_new_columns,
)
from dbx_flame.output.table_config import DeltaTableConfig

if TYPE_CHECKING:
    from datetime import datetime

    from pyspark.sql import Column, DataFrame

    from dbx_flame.context.context import Context

_SOURCE = "Scd2Writer"
_EVENT = Kpi.ROWS_HISTORIZED


class Scd2Writer:
    verb: ClassVar[Verb] = Verb.SCD2
    requires: ClassVar[Requirements] = Requirements(
        keys=True,
        event_time=True,
        # No snapshot_scope: every batch is a set of changes. Expiring and re-inserting
        # the whole table per load would make Silver a duplicate of Bronze; a
        # full-snapshot source belongs on FULL or COMPLETE_DELTA.
        # Checkpoint stays the default; watermark suits a source that carries a
        # per-record date. A full read is safe to repeat: the anti-filter drops every
        # row the target already holds at that event time, so only newer ones open a version.
        # A delta read is the same changes, without re-reading the rest.
        increment_strategies=(
            IncrementStrategy.CHECKPOINT,
            IncrementStrategy.WATERMARK,
            IncrementStrategy.FULL_READ,
            IncrementStrategy.DELTA_READ,
        ),
    )

    def write(self, df: DataFrame, ctx: Context) -> None:
        merge_history(df, ctx)


def event_time(ctx: Context, alias: str | None = None) -> Column:
    """The configured event time as a timestamp, built the same way on both sides.

    Bronze keeps unconfigured columns as strings, so a string date needs its format to
    compare chronologically rather than lexically. Nothing is persisted.
    """
    configured = ctx.config.output.event_time
    assert configured  # guaranteed by Requirements(event_time=True)
    return as_timestamp(configured.column, configured.format, alias)


def merge_history(df: DataFrame, ctx: Context, snapshot: Optional[datetime] = None) -> None:
    """Close each key's superseded version and insert the incoming one.

    `snapshot` is the export being merged; snapshot_scope=full expires at it. The
    pipeline has already deduplicated df.
    """
    prepared = promote(df)
    require_creatable(prepared, ctx)

    if not ctx.spark.catalog.tableExists(ctx.target_table):
        _create(prepared, ctx)
        return

    if prepared.isEmpty():
        # Nothing to merge — and, under snapshot_scope=full, nothing to expire either.
        # A full-snapshot source that sent no rows is a run with no news, not an
        # instruction to retire the whole table.
        ctx.logger.info(
            name="scd2_skipped",
            source=_SOURCE,
            description=f"No records to merge into {ctx.target_table}",
        )
        return

    # Before the first commit: the expiry and the close commit on their own, so an insert
    # refused after them would leave the keys they closed with no current row.
    require_no_new_columns(prepared, ctx)

    # Only COMPLETE_DELTA reaches this: SCD2 rejects snapshot_scope=full at Start.
    if ctx.config.output.snapshot_scope == SnapshotScope.FULL:
        assert snapshot  # COMPLETE_DELTA passes each snapshot it replays
        _expire_all_current(ctx, snapshot)

    # Cached because both statements below read it, and the first one changes the
    # table the anti-filter derives from: without this the insert would be computed
    # against a target that the close has already moved.
    incoming = _drop_already_current(prepared, ctx).cache()
    try:
        _close_superseded(incoming, ctx)
        _insert(incoming, ctx)
    finally:
        incoming.unpersist()


def _create(df: DataFrame, ctx: Context) -> None:
    """First run: every record opens a history of its own."""
    rows = open_history(df, event_time(ctx))
    (
        rows.write.format("delta")
        .mode("append")
        .options(**DeltaTableConfig(ctx).write_options(rows))
        .saveAsTable(ctx.target_table)
    )
    log_rows_written(ctx, event=_EVENT, source=_SOURCE)


def _expire_all_current(ctx: Context, snapshot: datetime) -> None:
    """snapshot_scope=full: expire everything before merging the snapshot.

    The source re-asserts its complete dataset each time, so a record it no longer
    contains is gone. Expiring first turns that absence into an implicit deletion: the
    snapshot re-inserts what it still carries, and what it dropped stays closed.

    Rows close at the export's own time, not the run's: that is the latest a dropped
    record can have left the source, and it is where a re-asserted one reopens.
    """
    DeltaTable.forName(ctx.spark, ctx.target_table).update(
        condition=F.col(f"`{CURRENT_FLAG}`") == CURRENT,
        set={
            f"`{CURRENT_FLAG}`": F.lit(EXPIRED),
            f"`{END_DATE}`": F.lit(snapshot),
        },
    )


def _drop_already_current(df: DataFrame, ctx: Context) -> DataFrame:
    """Drop rows the target already holds at that version or a newer one.

    This is what makes a re-run a no-op and a late or duplicated file harmless. It is a
    left-anti join rather than SQL built against a temp view.
    """
    current = ctx.spark.table(ctx.target_table).filter(F.col(f"`{CURRENT_FLAG}`") == CURRENT)
    keys = key_condition(ctx.config.output.keys, "s", "t")
    condition = keys & (event_time(ctx, "s") <= event_time(ctx, "t"))
    return df.alias("s").join(current.alias("t"), condition, "left_anti")


def _close_superseded(df: DataFrame, ctx: Context) -> None:
    """End the validity window of the version each incoming record replaces."""
    target = DeltaTable.forName(ctx.spark, ctx.target_table)
    superseded = (
        (event_time(ctx, "s") > event_time(ctx, "t"))
        & (F.col(f"t.`{CURRENT_FLAG}`") == CURRENT)
        & (F.col(f"t.`{DELETED_FLAG}`") == LIVE)
    )

    matched = key_condition(ctx.config.output.keys, "s", "t")
    target.alias("t").merge(df.alias("s"), matched).whenMatchedUpdate(
        condition=superseded,
        set={
            f"`{END_DATE}`": event_time(ctx, "s"),
            f"`{CURRENT_FLAG}`": F.lit(EXPIRED),
        },
    ).execute()


def _insert(df: DataFrame, ctx: Context) -> None:
    """Append the surviving records as the new current versions."""
    rows = open_history(df, event_time(ctx))
    (
        rows.write.format("delta")
        .mode("append")
        .option("mergeSchema", merge_schema(ctx))
        .saveAsTable(ctx.target_table)
    )
    log_rows_written(ctx, event=_EVENT, source=_SOURCE)
