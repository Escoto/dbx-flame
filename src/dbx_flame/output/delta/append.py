"""APPEND verb — add the incoming records to the target, no keys, no history."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from dbx_flame.context.config import Verb
from dbx_flame.observability.kpi import Kpi
from dbx_flame.output.base import Requirements
from dbx_flame.output.mechanics import log_rows_written, merge_schema, require_creatable
from dbx_flame.output.table_config import DeltaTableConfig

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from dbx_flame.context.context import Context

_SOURCE = "AppendWriter"


class AppendWriter:
    verb: ClassVar[Verb] = Verb.APPEND
    requires: ClassVar[Requirements] = Requirements()

    def write(self, df: DataFrame, ctx: Context) -> None:
        require_creatable(df, ctx)

        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", merge_schema(ctx))
            .options(**DeltaTableConfig(ctx).write_options(df))
            .saveAsTable(ctx.target_table)
        )
        log_rows_written(ctx, event=Kpi.ROWS_APPENDED, source=_SOURCE)
