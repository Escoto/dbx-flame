"""Tests for pipelines.delta_source — checkpoint and watermark increments."""

from __future__ import annotations

import re
import uuid
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from dbx_flame.context.config import (
    IncrementStrategy,
    Origin,
    OutputConfig,
    SourceConfig,
    TaskConfig,
    Verb,
)
from dbx_flame.context.context import Context, RunIdentity
from dbx_flame.pipelines.delta_source import (
    DEFAULT_WATERMARK,
    DeltaSource,
    MissingAnchorError,
)

RUN = RunIdentity(
    workflow_id="wf-1",
    workflow_run_id="wfrun-1",
    task_key="bronze_to_silver",
    task_run_id="taskrun-1",
)

SCHEMA = "ID string, __EXPORT_DATE timestamp"


@pytest.fixture
def database(spark):
    name = f"test_delta_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{name}`")
    yield name
    spark.sql(f"DROP DATABASE IF EXISTS `{name}` CASCADE")


def _write(spark, database, table, rows):
    spark.createDataFrame(rows, SCHEMA).write.format("delta").mode("overwrite").saveAsTable(
        f"`{database}`.`{table}`"
    )


def _context(spark, database, strategy, *, deletes=None, **source_overrides):
    """A Context built by hand: build_context resolves Unity Catalog three-part names."""
    source = dict(origin=Origin.DELTA, schema_name="bronze_cro", table="UPDATES")
    source.update(source_overrides)
    config = TaskConfig(
        catalog="cro",
        env="dev_01",
        metadata_path="/Volumes/meta/",
        source=SourceConfig(**source),
        output=OutputConfig(verb=Verb.APPEND, schema_name="silver_cro", table="SUBJECTS"),
    )
    return Context(
        config=config,
        spark=spark,
        run=RUN,
        logger=MagicMock(),
        catalog="cro_dev_01",
        source_table=f"`{database}`.`UPDATES`",
        deletes_table=f"`{database}`.`{deletes}`" if deletes else None,
        target_table=f"`{database}`.`SUBJECTS`",
        inbound_glob=None,
        checkpoint_location="/tmp/checkpoint/",
        schema_hints_location="/tmp/hints/",
        increment_strategy=IncrementStrategy(strategy),
    )


def test_checkpoint_strategy_returns_a_stream(spark, database):
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.CHECKPOINT)

    result = DeltaSource().read(ctx)

    assert result.isStreaming


def test_watermark_strategy_returns_a_batch(spark, database):
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK)

    result = DeltaSource().read(ctx)

    assert not result.isStreaming


def test_an_absent_target_takes_the_whole_source(spark, database):
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2)), ("2", datetime(1999, 5, 5))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK)

    assert DeltaSource().read(ctx).count() == 2


def test_the_default_watermark_predates_any_export(spark, database):
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK)
    source = DeltaSource()

    source.read(ctx)

    assert source._watermark == DEFAULT_WATERMARK


def test_only_rows_newer_than_the_target_are_read(spark, database):
    _write(spark, database, "SUBJECTS", [("1", datetime(2024, 1, 3))])
    _write(
        spark,
        database,
        "UPDATES",
        [("1", datetime(2024, 1, 2)), ("2", datetime(2024, 1, 4))],
    )
    ctx = _context(spark, database, IncrementStrategy.WATERMARK)

    rows = DeltaSource().read(ctx).collect()

    assert [row["ID"] for row in rows] == ["2"]


def test_a_row_exactly_on_the_watermark_is_excluded(spark, database):
    """The comparison is a typed, strictly-greater-than timestamp test."""
    boundary = datetime(2024, 1, 3, 12, 30, 45, 123000)
    _write(spark, database, "SUBJECTS", [("1", boundary)])
    _write(
        spark,
        database,
        "UPDATES",
        [("1", boundary), ("2", datetime(2024, 1, 3, 12, 30, 45, 124000))],
    )
    ctx = _context(spark, database, IncrementStrategy.WATERMARK)

    rows = DeltaSource().read(ctx).collect()

    assert [row["ID"] for row in rows] == ["2"]


def test_an_empty_target_falls_back_to_the_default_watermark(spark, database):
    spark.createDataFrame([], SCHEMA).write.format("delta").saveAsTable(f"`{database}`.`SUBJECTS`")
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK)

    assert DeltaSource().read(ctx).count() == 1


def test_no_deletes_feed_returns_none(spark, database):
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK)

    assert DeltaSource().read_deletes(ctx) is None


def test_the_deletes_feed_is_filtered_like_the_updates(spark, database):
    _write(spark, database, "SUBJECTS", [("1", datetime(2024, 1, 3))])
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 4))])
    _write(
        spark,
        database,
        "DELETES",
        [("9", datetime(2024, 1, 1)), ("8", datetime(2024, 1, 5))],
    )
    ctx = _context(spark, database, IncrementStrategy.WATERMARK, deletes="DELETES")

    rows = DeltaSource().read_deletes(ctx).collect()

    assert [row["ID"] for row in rows] == ["8"]


def test_the_deletes_feed_streams_under_the_checkpoint_strategy(spark, database):
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))])
    _write(spark, database, "DELETES", [("9", datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.CHECKPOINT, deletes="DELETES")

    assert DeltaSource().read_deletes(ctx).isStreaming


def test_updates_and_deletes_are_cut_at_the_same_watermark(spark, database):
    """Both feeds belong to one run; a target that moves mid-run must not split them."""
    _write(spark, database, "SUBJECTS", [("1", datetime(2024, 1, 1))])
    _write(spark, database, "UPDATES", [("2", datetime(2024, 1, 2))])
    _write(spark, database, "DELETES", [("3", datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK, deletes="DELETES")
    source = DeltaSource()

    assert source.read(ctx).count() == 1

    _write(spark, database, "SUBJECTS", [("1", datetime(2024, 1, 5))])

    assert source.read_deletes(ctx).count() == 1


def test_the_resolved_watermark_is_logged(spark, database):
    _write(spark, database, "SUBJECTS", [("1", datetime(2024, 1, 3))])
    _write(spark, database, "UPDATES", [("2", datetime(2024, 1, 4))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK)

    DeltaSource().read(ctx)

    logged = ctx.logger.info.call_args
    assert logged.kwargs["name"] == "watermark_resolved"
    assert "2024-01-03" in logged.kwargs["description"]


def _drain(ctx, sink: str, checkpoint: str) -> None:
    query = (
        DeltaSource()
        .read(ctx)
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start(sink)
    )
    query.awaitTermination()


def test_a_delete_on_the_source_does_not_break_the_stream(spark, database, tmp_path):
    """Without ignoreDeletes, a single DELETE kills the stream for good."""
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2)), ("2", datetime(2024, 1, 3))])
    ctx = _context(spark, database, IncrementStrategy.CHECKPOINT)
    sink = str(tmp_path / "sink")
    checkpoint = str(tmp_path / "checkpoint")

    _drain(ctx, sink, checkpoint)
    spark.sql(f"DELETE FROM `{database}`.`UPDATES` WHERE ID = '1'")
    _drain(ctx, sink, checkpoint)  # raises "Detected deleted data" without the option

    assert spark.read.format("delta").load(sink).count() == 2


SNAPSHOT_SCHEMA = "ID string, __ANCHOR_DT timestamp, __EXPORT_DATE timestamp"


def _snapshot(spark, database, table, rows, mode="append", schema=SNAPSHOT_SCHEMA):
    spark.createDataFrame(rows, schema).write.format("delta").mode(mode).saveAsTable(
        f"`{database}`.`{table}`"
    )


def test_a_record_anchor_reads_nothing_when_no_record_changed(spark, database):
    """The other snapshot case: the source carries a per-record date.

    The anchor is then a property of the record, so an unchanged snapshot falls wholly
    below the watermark and never enters the pipeline at all.
    """
    _snapshot(spark, database, "SUBJECTS", [("1", datetime(2024, 1, 1), datetime(2024, 1, 1))])
    for day in range(1, 6):
        _snapshot(
            spark, database, "UPDATES", [("1", datetime(2024, 1, 1), datetime(2024, 1, day))]
        )
    ctx = _context(spark, database, IncrementStrategy.WATERMARK, increment_anchor=True)

    assert DeltaSource().read(ctx).count() == 0


def test_a_record_anchor_still_picks_up_a_changed_record(spark, database):
    _snapshot(spark, database, "SUBJECTS", [("1", datetime(2024, 1, 1), datetime(2024, 1, 1))])
    _snapshot(spark, database, "UPDATES", [("1", datetime(2024, 1, 1), datetime(2024, 1, 2))])
    _snapshot(spark, database, "UPDATES", [("2", datetime(2024, 3, 1), datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK, increment_anchor=True)

    rows = DeltaSource().read(ctx).collect()

    assert [row["ID"] for row in rows] == ["2"]


def test_the_anchor_filter_reaches_the_scan(spark, database):
    """A plain comparison on __ANCHOR_DT, not a parse, so file skipping can use it."""
    _snapshot(spark, database, "UPDATES", [("1", datetime(2024, 1, 2), datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK, increment_anchor=True)

    plan = DeltaSource().read(ctx)._jdf.queryExecution().executedPlan().toString()

    assert re.search(r"DataFilters: \[[^\]]*\(__ANCHOR_DT#\d+ >", plan), plan


@pytest.mark.parametrize("unstamped", ["UPDATES", "DELETIONS", "SUBJECTS"])
def test_an_anchored_read_refuses_a_table_without_anchor_dt(spark, database, unstamped):
    """Every row would read as a NULL anchor and be skipped without a word."""
    for table in ("UPDATES", "DELETIONS", "SUBJECTS"):
        if table == unstamped:
            _snapshot(spark, database, table, [("1", datetime(2024, 1, 2))], schema=SCHEMA)
        else:
            _snapshot(spark, database, table, [("1", datetime(2024, 1, 1), datetime(2024, 1, 1))])
    ctx = _context(
        spark, database, IncrementStrategy.WATERMARK, deletes="DELETIONS", increment_anchor=True
    )
    source = DeltaSource()

    with pytest.raises(MissingAnchorError, match=f"`{unstamped}` has no __ANCHOR_DT"):
        source.read(ctx)
        source.read_deletes(ctx)


def test_the_resolved_anchor_is_named_in_the_log(spark, database):
    _snapshot(spark, database, "SUBJECTS", [("1", datetime(2024, 1, 1), datetime(2024, 1, 1))])
    _snapshot(spark, database, "UPDATES", [("1", datetime(2024, 1, 2), datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK, increment_anchor=True)

    DeltaSource().read(ctx)

    assert "__ANCHOR_DT" in ctx.logger.info.call_args.kwargs["description"]
