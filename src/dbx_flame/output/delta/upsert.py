"""UPSERT verb — SCD Type 1: latest state per key, no history."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from delta.tables import DeltaTable

from dbx_flame.context.config import Verb
from dbx_flame.observability.kpi import Kpi
from dbx_flame.output.base import Requirements
from dbx_flame.output.mechanics import (
    as_timestamp,
    log_rows_written,
    merge_schema,
    require_no_new_columns,
    schema_auto_merge,
    promote,
    require_creatable,
)
from dbx_flame.output.table_config import DeltaTableConfig

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from dbx_flame.context.context import Context

_SOURCE = "UpsertWriter"
_EVENT = Kpi.ROWS_UPSERTED


class UpsertWriter:
    verb: ClassVar[Verb] = Verb.UPSERT
    requires: ClassVar[Requirements] = Requirements(keys=True)

    def write(self, df: DataFrame, ctx: Context) -> None:
        prepared = promote(df)
        require_creatable(prepared, ctx)

        if not ctx.spark.catalog.tableExists(ctx.target_table):
            self._create(prepared, ctx)
            return

        require_no_new_columns(prepared, ctx)

        target = DeltaTable.forName(ctx.spark, ctx.target_table)
        matched = " AND ".join(f"t.`{key}` = s.`{key}`" for key in ctx.config.output.keys)
        merge = target.alias("t").merge(prepared.alias("s"), matched)

        # `if newer` would evaluate the Column's truthiness, which raises.
        newer = _newer_than_target(ctx)
        if newer is None:
            merge = merge.whenMatchedUpdateAll()
        else:
            merge = merge.whenMatchedUpdateAll(condition=newer)

        # UpdateAll/InsertAll silently discard a column the target lacks, so the
        # conf has to be in force for the merge itself.
        with schema_auto_merge(ctx):
            merge.whenNotMatchedInsertAll().execute()

        log_rows_written(ctx, event=_EVENT, source=_SOURCE)

    def _create(self, df: DataFrame, ctx: Context) -> None:
        """First load defines the table; there is nothing to merge against yet."""
        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", merge_schema(ctx))
            .options(**DeltaTableConfig(ctx).write_options(df))
            .saveAsTable(ctx.target_table)
        )
        log_rows_written(ctx, event=_EVENT, source=_SOURCE)


def _newer_than_target(ctx: Context) -> Column | None:
    """Newer-wins when an event time is configured; otherwise the batch always wins."""
    event_time = ctx.config.output.event_time
    if not event_time:
        return None
    return as_timestamp(event_time.column, event_time.format, alias="s") > as_timestamp(
        event_time.column, event_time.format, alias="t"
    )
