"""COMPLETE_DELTA verb — replay every pending snapshot, in order, through SCD2.

Plain SCD2 keeps the latest version per key within whatever it happens to process, so
a backlog of three exports collapses to the newest one and the intermediate states never
reach Silver. This verb splits the increment back into the snapshots it arrived as and
merges them one at a time, which is what makes every evolution of a record visible.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from dbx_flame.context.config import IncrementStrategy, Origin, SnapshotScope, Verb
from dbx_flame.observability.kpi import Kpi
from dbx_flame.output.base import Requirements
from dbx_flame.output.delta.scd2 import merge_history
from dbx_flame.output.mechanics import (
    CURRENT,
    CURRENT_FLAG,
    DELETED,
    DELETED_FLAG,
    END_DATE,
    EXPIRED,
    EXPORT_DATE,
    SILVER_TIMESTAMP,
    as_timestamp,
    require_valid_times,
    key_condition,
)
from dbx_flame.pipelines.table_source import TableSource
from dbx_flame.policies.runner import PolicyRunner

if TYPE_CHECKING:
    from datetime import datetime

    from pyspark.sql import Column, DataFrame

    from dbx_flame.context.context import Context

_SOURCE = "CompleteDeltaWriter"


class CompleteDeltaWriter:
    verb: ClassVar[Verb] = Verb.COMPLETE_DELTA
    requires: ClassVar[Requirements] = Requirements(
        keys=True,
        event_time=True,
        supports_deletes=True,
        supports_snapshot_scope=True,
        # snapshot replay needs a Delta updates table; file origins cannot feed it
        origins=frozenset({Origin.TABLE}),
        per_snapshot=True,
        # Never checkpoint: replay has to see every snapshot, so a strategy that keeps
        # just the newest one would defeat the purpose of the verb. A full read of a
        # table we didn't create is one complete snapshot per run, so under
        # snapshot_scope=full the keys it no longer carries are retired.
        increment_strategies=(IncrementStrategy.WATERMARK, IncrementStrategy.FULL_READ),
    )

    def write(self, df: DataFrame, ctx: Context) -> None:
        # The deletes feed is read here rather than by the pipeline spine because only
        # this verb has one. Both reads cut at the same point: the watermark comes from
        # the target, and nothing has been written to it yet; a full read stamps both
        # with the run's one read time, so they land in the same snapshot.
        deletes = TableSource().read_deletes(ctx)
        if deletes is not None:
            configured = ctx.config.output.deletes
            assert configured and configured.event_time  # guaranteed by config validation
            # Before any snapshot is replayed, so a bad delete can't leave a half-applied run.
            setting = "output.deletes.event_time.column"
            require_valid_times(deletes, ctx, [(setting, configured.event_time)])

        snapshots = _ordered_snapshots(df, deletes)
        if not snapshots:
            # Nothing pending. Hand the empty frame to the engine anyway: on a first run
            # that is what creates the target, so downstream readers find an empty table
            # rather than a missing one.
            merge_history(df, ctx)
            return

        if ctx.config.output.snapshot_scope == SnapshotScope.FULL:
            snapshots = _from_newest_export(df, snapshots, ctx)

        ctx.logger.info(
            name="snapshots_to_replay",
            source=_SOURCE,
            total=len(snapshots),
            description=f"Replaying {len(snapshots)} snapshot(s) into {ctx.target_table}",
        )

        # Each snapshot is judged as the rows its merge writes, and all of them before
        # the first merge, so one that fails leaves the target untouched.
        runner = PolicyRunner()
        for snapshot in snapshots:
            runner.run(_at(df, snapshot), ctx, batch=f"snapshot {snapshot}")

        for snapshot in snapshots:
            merge_history(_at(df, snapshot), ctx, snapshot)
            if deletes is not None:
                _apply_deletes(_at(deletes, snapshot), ctx)


def _ordered_snapshots(updates: DataFrame, deletes: Optional[DataFrame]) -> list[datetime]:
    """Every pending snapshot, oldest first — the order history has to be rebuilt in.

    __EXPORT_DATE identifies a snapshot. Bronze refuses a file without one, and a
    filter on the column itself lets each snapshot's merge skip the other exports' files.
    """
    stamps = updates.select(EXPORT_DATE)
    if deletes is not None:
        stamps = stamps.union(deletes.select(EXPORT_DATE))

    return [row[0] for row in stamps.distinct().orderBy(EXPORT_DATE).collect()]


def _from_newest_export(
    updates: DataFrame, snapshots: list[datetime], ctx: Context
) -> list[datetime]:
    """snapshot_scope=full: each export supersedes every one before it.

    Replaying the older ones would only write versions the newest immediately expires;
    Bronze keeps them. Deletes stamped after the newest export still apply.
    """
    newest = updates.agg(F.max(EXPORT_DATE)).collect()[0][0]
    if newest is None:
        return snapshots

    kept = [snapshot for snapshot in snapshots if snapshot >= newest]
    skipped = len(snapshots) - len(kept)
    if skipped:
        ctx.logger.info(
            name="snapshots_superseded",
            source=_SOURCE,
            total=skipped,
            description=f"Skipping {skipped} snapshot(s) older than the export of {newest}",
        )
    return kept


def _at(df: DataFrame, snapshot: datetime) -> DataFrame:
    return df.filter(F.col(EXPORT_DATE) == F.lit(snapshot))


def _delete_time(ctx: Context, alias: str | None = None) -> Column:
    configured = ctx.config.output.deletes
    assert configured and configured.event_time  # guaranteed by config validation
    return as_timestamp(configured.event_time.column, configured.event_time.format, alias)


def _apply_deletes(df: DataFrame, ctx: Context) -> None:
    """Soft-delete the entities this snapshot retired. History is never removed.

    Two merges, because they touch different row sets: the flag marks the entity across
    all of its versions, while the window only closes on the one still open.
    """
    if not ctx.spark.catalog.tableExists(ctx.target_table):
        return
    if df.isEmpty():
        return

    configured = ctx.config.output.deletes
    assert configured  # guaranteed by config validation
    matched = key_condition(configured.keys, "s", "t")

    target = DeltaTable.forName(ctx.spark, ctx.target_table)
    target.alias("t").merge(df.alias("s"), matched).whenMatchedUpdate(
        set={f"`{DELETED_FLAG}`": F.lit(DELETED)}
    ).execute()

    still_open = (F.col(f"t.`{END_DATE}`").isNull()) & (F.col(f"t.`{CURRENT_FLAG}`") == CURRENT)
    target.alias("t").merge(df.alias("s"), matched).whenMatchedUpdate(
        condition=still_open,
        set={
            f"`{END_DATE}`": _delete_time(ctx, "s"),
            f"`{CURRENT_FLAG}`": F.lit(EXPIRED),
            f"`{SILVER_TIMESTAMP}`": F.current_timestamp(),
        },
    ).execute()

    ctx.logger.kpi(
        name=Kpi.ROWS_RETIRED,
        total=df.count(),
        description=f"{_SOURCE} soft-deleted records in {ctx.target_table}",
    )
