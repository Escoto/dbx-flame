"""Table origin — checkpoint, watermark, full-read or delta-read increments."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, ClassVar, Optional

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampNTZType, TimestampType

from dbx_flame.context.config import IncrementStrategy
from dbx_flame.pipelines.enrichment import (
    ANCHOR_DT,
    EXPORT_DATE,
    PROVENANCE_MARKERS,
    stamp_table_read,
)
from dbx_flame.policies.platform import PlatformPolicy, violate

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from dbx_flame.context.context import Context

# Earlier than any real export, so a first run takes the whole source.
DEFAULT_WATERMARK = datetime(1900, 1, 1)

_SOURCE = "TableSource"
_MARKERS = ", ".join(PROVENANCE_MARKERS)

# The strategies for a table we didn't create: each read is stamped as one export.
_FOREIGN_READS = (IncrementStrategy.FULL_READ, IncrementStrategy.DELTA_READ)

# Compared as they are, with no parse, so a delta_read's filter reaches the source.
_ANCHOR_TYPES = (DateType, TimestampType, TimestampNTZType)


def _follows_anchor(ctx: Context) -> bool:
    return (
        ctx.config.source.increment_anchor
        or ctx.increment_strategy == IncrementStrategy.DELTA_READ
    )


def _anchor_column(ctx: Context) -> str:
    """__ANCHOR_DT when anchored, else the arrival timestamp the framework adds.

    Both are timestamps written at ingestion, so the filter compares a real column and
    can reach the scan.
    """
    return ANCHOR_DT if _follows_anchor(ctx) else EXPORT_DATE


def _is_stamped(ctx: Context, table: str) -> bool:
    """Whether we stamped this table: it carries all of our provenance markers, or none.

    A table with only some is neither ours nor foreign, so it fails rather than guessing.
    """
    columns = {column.upper() for column in ctx.spark.table(table).columns}
    missing = [marker for marker in PROVENANCE_MARKERS if marker not in columns]
    if 0 < len(missing) < len(PROVENANCE_MARKERS):
        violate(
            ctx,
            PlatformPolicy.MALFORMED_TABLE,
            _SOURCE,
            f"malformed table {table}: incomplete metadata: missing {', '.join(missing)}",
        )
    return not missing


def _require_anchor(ctx: Context, table: str) -> None:
    """An unstamped table would read as NULL anchors: every row silently skipped."""
    if _follows_anchor(ctx) and ANCHOR_DT not in ctx.spark.table(table).columns:
        violate(
            ctx,
            PlatformPolicy.ANCHOR_NOT_STAMPED,
            _SOURCE,
            f"{table} has no {ANCHOR_DT}: set source.anchor_dt on the task that ingests it",
        )


class TableSource:
    """Read from a table using the run's configured increment strategy."""

    increment_strategies: ClassVar[tuple[IncrementStrategy, ...]] = (
        IncrementStrategy.CHECKPOINT,
        IncrementStrategy.WATERMARK,
        IncrementStrategy.FULL_READ,
        IncrementStrategy.DELTA_READ,
    )

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
        stamped = _is_stamped(ctx, table)
        if ctx.increment_strategy in _FOREIGN_READS:
            return self._foreign_read(ctx, table, stamped)

        if not stamped:
            # Stamping here would label an increment as a whole snapshot, and per
            # micro-batch at that: FULL would keep the last slice and drop the rest.
            violate(
                ctx,
                PlatformPolicy.UNSTAMPED_TABLE,
                _SOURCE,
                f"{table} carries none of {_MARKERS}: a table we didn't create can only be "
                "read with source.increment_strategy=full_read or delta_read",
            )

        if ctx.increment_strategy == IncrementStrategy.CHECKPOINT:
            # Without this a single DELETE on the source breaks the stream for good.
            # It covers deletes only: a source that is updated or overwritten has no
            # incremental semantics to offer, and should fail loudly rather than be
            # silenced with skipChangeCommits, which would skip the changed data.
            return ctx.spark.readStream.option("ignoreDeletes", "true").table(table)
        return self._since_watermark(ctx, table)

    def _foreign_read(self, ctx: Context, table: str, stamped: bool) -> DataFrame:
        """The whole table, or the rows it changed, as one batch stamped at the read time."""
        strategy = ctx.increment_strategy.value
        if stamped:
            # Our own table already holds its exports: stamping them again would append
            # them twice, or replay snapshots older than the target already holds.
            violate(
                ctx,
                PlatformPolicy.STAMPED_TABLE,
                _SOURCE,
                f"{table} already carries {_MARKERS}: a {strategy} would stamp the exports "
                "it holds again; read it with source.increment_strategy=checkpoint or watermark",
            )

        df = ctx.spark.table(table)
        if ctx.increment_strategy == IncrementStrategy.DELTA_READ:
            df = self._changed_rows(ctx, table, df)

        ctx.logger.info(
            name="table_stamped",
            source=_SOURCE,
            description=(
                f"{strategy} of {table}, which carries no export stamp of its own: "
                f"stamped {EXPORT_DATE}={ctx.read_time.isoformat()}"
            ),
        )
        return stamp_table_read(df, table, ctx.read_time)

    def _changed_rows(self, ctx: Context, table: str, df: DataFrame) -> DataFrame:
        """Rows whose source.anchor_dt column is past the target's highest __ANCHOR_DT.

        The column is filtered as the source has it, and copied into __ANCHOR_DT for
        the next run's watermark.
        """
        anchor = ctx.config.source.anchor_dt
        assert anchor  # required for delta_read at Start
        found = next(
            (f for f in df.schema.fields if f.name.upper() == anchor.column.upper()), None
        )
        if found is None:
            violate(
                ctx,
                PlatformPolicy.ANCHOR_COLUMN_MISSING,
                _SOURCE,
                f"source.anchor_dt.column={anchor.column} is not in {table}",
            )
        if not isinstance(found.dataType, _ANCHOR_TYPES):
            violate(
                ctx,
                PlatformPolicy.ANCHOR_WRONG_TYPE,
                _SOURCE,
                f"source.anchor_dt.column={anchor.column} in {table} is "
                f"{found.dataType.simpleString()}: a delta_read follows a DATE or TIMESTAMP "
                "column, so its filter reaches the source",
            )

        column = F.col(f"`{found.name}`")
        # The watermark takes the column's type, not the other way round, so the filter
        # stays a plain predicate on the source column.
        watermark = F.lit(self._watermark_for(ctx)).cast(found.dataType)
        return df.filter(column > watermark).withColumn(ANCHOR_DT, column.cast("timestamp"))

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
