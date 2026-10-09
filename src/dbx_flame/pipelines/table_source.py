"""Table origin — checkpoint or watermark increments."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Optional

from pyspark.sql import functions as F

from dbx_flame.context.config import IncrementStrategy
from dbx_flame.pipelines.enrichment import ANCHOR_DT
from dbx_flame.policies.platform import PlatformPolicy, violate

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from dbx_flame.context.context import Context

# Earlier than any real export, so a first run takes the whole source.
DEFAULT_WATERMARK = datetime(1900, 1, 1)

_EXPORT_DATE = "__EXPORT_DATE"
_SOURCE = "TableSource"


def _anchor_column(ctx: Context) -> str:
    """__ANCHOR_DT when anchored, else the arrival timestamp the framework adds.

    Both are timestamps written at ingestion, so the filter compares a real column and
    can reach the scan.
    """
    return ANCHOR_DT if ctx.config.source.increment_anchor else _EXPORT_DATE


def _require_anchor(ctx: Context, table: str) -> None:
    """An unstamped table would read as NULL anchors: every row silently skipped."""
    if ctx.config.source.increment_anchor and ANCHOR_DT not in ctx.spark.table(table).columns:
        violate(
            ctx,
            PlatformPolicy.ANCHOR_NOT_STAMPED,
            _SOURCE,
            f"{table} has no {ANCHOR_DT}: set source.anchor_dt on the task that ingests it",
        )


class TableSource:
    """Read from a Delta table using the run's configured increment strategy."""

    def __init__(self) -> None:
        # Updates and deletes must be cut at the same point, so the target's
        # watermark is read once and reused.
        self._watermark: Optional[datetime] = None

    def read(self, ctx: Context) -> DataFrame:
        table = ctx.source_table
        assert table  # guaranteed by SourceConfig for table origins
        return self._read_table(ctx, table)

    def read_deletes(self, ctx: Context) -> Optional[DataFrame]:
        """Read the optional deletes feed table. Returns None if not configured."""
        table = ctx.deletes_table
        if not table:
            return None
        return self._read_table(ctx, table)

    def _read_table(self, ctx: Context, table: str) -> DataFrame:
        if ctx.increment_strategy == IncrementStrategy.CHECKPOINT:
            # Without this a single DELETE on the source breaks the stream for good.
            # It covers deletes only: a source that is updated or overwritten has no
            # incremental semantics to offer, and should fail loudly rather than be
            # silenced with skipChangeCommits, which would skip the changed data.
            return ctx.spark.readStream.option("ignoreDeletes", "true").table(table)
        return self._since_watermark(ctx, table)

    def _since_watermark(self, ctx: Context, table: str) -> DataFrame:
        """A plain comparison on a timestamp column, so it reaches the scan.

        Parsing a string here instead would be wrong whenever the format doesn't sort
        lexicographically ("M/d/yyyy"), and would hide the column from file skipping.
        """
        _require_anchor(ctx, table)
        watermark = self._watermark_for(ctx)
        return ctx.spark.table(table).filter(F.col(_anchor_column(ctx)) > F.lit(watermark))

    def _watermark_for(self, ctx: Context) -> datetime:
        if self._watermark is None:
            self._watermark = self._read_watermark(ctx)
        return self._watermark

    def _read_watermark(self, ctx: Context) -> datetime:
        """The anchor's high-water mark in the target; the default when there is none yet."""
        column = _anchor_column(ctx)
        watermark = DEFAULT_WATERMARK
        if ctx.spark.catalog.tableExists(ctx.target_table):
            _require_anchor(ctx, ctx.target_table)
            target = ctx.spark.table(ctx.target_table)
            highest = target.agg(F.max(column)).collect()[0][0]
            watermark = highest or DEFAULT_WATERMARK

        ctx.logger.info(
            name="watermark_resolved",
            source=_SOURCE,
            description=f"Reading rows with {column} > {watermark}",
        )
        return watermark
