"""FULL verb — replace the target with the current dataset."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from pyspark.sql import functions as F

from dbx_flame.context.config import IncrementStrategy, Verb
from dbx_flame.observability.kpi import Kpi
from dbx_flame.output.base import Requirements
from dbx_flame.output.mechanics import (
    EXPORT_DATE,
    latest_export,
    log_rows_written,
    merge_schema,
    promote,
    require_creatable,
)
from dbx_flame.output.table_config import DeltaTableConfig

if TYPE_CHECKING:
    from datetime import datetime

    from pyspark.sql import DataFrame

    from dbx_flame.context.context import Context

_SOURCE = "FullWriter"


class FullWriter:
    verb: ClassVar[Verb] = Verb.FULL
    requires: ClassVar[Requirements] = Requirements(
        # A full read is the current dataset by definition: exactly what FULL replaces with.
        increment_strategies=(IncrementStrategy.CHECKPOINT, IncrementStrategy.FULL_READ),
    )

    def write(self, df: DataFrame, ctx: Context) -> None:
        # Never wipe a table because an upstream export was missing or empty.
        # isEmpty short-circuits on the first row rather than counting.
        if df.isEmpty():
            ctx.logger.warning(
                name="full_load_skipped",
                source=_SOURCE,
                description=(
                    f"Source produced no rows; left {ctx.target_table} as it was "
                    "rather than overwriting it with nothing"
                ),
            )
            return

        require_creatable(df, ctx)

        # A backlog hands one batch several exports; each supersedes the ones before
        # it, so only the newest is the current dataset.
        latest = latest_export(df)
        if latest is not None:
            applied = _applied_export(ctx)
            if applied is not None and latest < applied:
                ctx.logger.warning(
                    name="full_load_skipped",
                    source=_SOURCE,
                    description=(
                        f"Export {latest} is older than {applied}, already in "
                        f"{ctx.target_table}; a stale snapshot must not replace a newer one"
                    ),
                )
                return
            df = df.filter(F.col(EXPORT_DATE) == F.lit(latest))

        promoted = promote(df)
        (
            promoted.write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", merge_schema(ctx))
            .options(**DeltaTableConfig(ctx).write_options(promoted))
            .saveAsTable(ctx.target_table)
        )
        log_rows_written(ctx, event=Kpi.ROWS_OVERWRITTEN, source=_SOURCE)


def _applied_export(ctx: Context) -> datetime | None:
    if not ctx.spark.catalog.tableExists(ctx.target_table):
        return None
    return latest_export(ctx.spark.table(ctx.target_table))
