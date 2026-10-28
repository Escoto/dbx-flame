"""APPEND verb — add the incoming records to the target, no keys, no history."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from dbx_flame.context.config import IncrementStrategy, Verb
from dbx_flame.observability.kpi import Kpi
from dbx_flame.output.base import Requirements
from dbx_flame.output.mechanics import (
    StepMode,
    log_rows_written,
    merge_schema,
    record_step,
    require_creatable,
    stamp_written,
    step_start,
)
from dbx_flame.output.table_config import DeltaTableConfig

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from dbx_flame.context.context import Context

_SOURCE = "AppendWriter"


class AppendWriter:
    verb: ClassVar[Verb] = Verb.APPEND
    requires: ClassVar[Requirements] = Requirements(
        # A full read lands each read of a foreign table in Bronze as one more snapshot;
        # a delta read, as the rows it changed since the last.
        increment_strategies=(
            IncrementStrategy.CHECKPOINT,
            IncrementStrategy.FULL_READ,
            IncrementStrategy.DELTA_READ,
        ),
    )

    def write(self, df: DataFrame, ctx: Context) -> None:
        require_creatable(df, ctx)

        before = step_start(ctx)
        # Cached because the step record reads the batch's sources after the write; an
        # Auto Loader micro-batch would otherwise read its files twice.
        stamped = stamp_written(df, ctx).cache()
        try:
            (
                stamped.write.format("delta")
                .mode("append")
                .option("mergeSchema", merge_schema(ctx))
                .options(**DeltaTableConfig(ctx).write_options(stamped))
                .saveAsTable(ctx.target_table)
            )
            log_rows_written(ctx, event=Kpi.ROWS_APPENDED, source=_SOURCE)
            record_step(ctx, before, stamped, StepMode.APPEND, _SOURCE)
        finally:
            stamped.unpersist()
