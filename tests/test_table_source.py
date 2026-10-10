"""Tests for pipelines.table_source — checkpoint, watermark, full and delta reads."""

from __future__ import annotations

import re
import uuid
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from dbx_flame.context.config import (
    EventTimeConfig,
    IncrementStrategy,
    Origin,
    OutputConfig,
    SourceConfig,
    TaskConfig,
    Verb,
)
from dbx_flame.context.context import Context, RunIdentity
from dbx_flame.pipelines.table_source import DEFAULT_WATERMARK, TableSource
from dbx_flame.policies.platform import PlatformPolicyViolation

RUN = RunIdentity(
    workflow_id="wf-1",
    workflow_run_id="wfrun-1",
    task_key="bronze_to_silver",
    task_run_id="taskrun-1",
)

SCHEMA = "ID string, __SOURCE string, __EXPORT_DATE timestamp"

# Provenance a Bronze we stamped carries; its value is irrelevant to every read here.
_FILE = "FILE:/Volumes/in/updates/UPDATES_20240101000000.csv"

READ_TIME = datetime(2026, 10, 9, 12, 0)


@pytest.fixture
def database(spark):
    name = f"test_delta_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{name}`")
    yield name
    spark.sql(f"DROP DATABASE IF EXISTS `{name}` CASCADE")


def _with_source(rows):
    """__SOURCE sits just before __EXPORT_DATE, the last column of every stamped schema."""
    return [row[:-1] + (_FILE,) + row[-1:] for row in rows]


def _write(spark, database, table, rows):
    spark.createDataFrame(_with_source(rows), SCHEMA).write.format("delta").mode(
        "overwrite"
    ).saveAsTable(f"`{database}`.`{table}`")


def _foreign(spark, database, table, rows, schema="ID string, NAME string"):
    """A table we didn't create: none of our metadata columns."""
    spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite").saveAsTable(
        f"`{database}`.`{table}`"
    )


def _context(spark, database, strategy, *, deletes=None, **source_overrides):
    """A Context built by hand: build_context resolves Unity Catalog three-part names."""
    source = dict(
        origin=Origin.TABLE,
        schema_name="bronze_cro",
        table="UPDATES",
        increment_strategy=IncrementStrategy(strategy),
    )
    if strategy == IncrementStrategy.DELTA_READ:
        source["anchor_dt"] = EventTimeConfig(column="MODIFIED_AT")
    source.update(source_overrides)
    config = TaskConfig(
        catalog="cro",
        env="dev_01",
        metadata_path="/Volumes/cro_dev_01/meta/",
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
        read_time=READ_TIME,
    )


def test_checkpoint_strategy_returns_a_stream(spark, database):
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.CHECKPOINT)

    result = TableSource().read(ctx)

    assert result.isStreaming


def test_watermark_strategy_returns_a_batch(spark, database):
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK)

    result = TableSource().read(ctx)

    assert not result.isStreaming


def test_an_absent_target_takes_the_whole_source(spark, database):
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2)), ("2", datetime(1999, 5, 5))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK)

    assert TableSource().read(ctx).count() == 2


def test_the_default_watermark_predates_any_export(spark, database):
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK)
    source = TableSource()

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

    rows = TableSource().read(ctx).collect()

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

    rows = TableSource().read(ctx).collect()

    assert [row["ID"] for row in rows] == ["2"]


def test_an_empty_target_falls_back_to_the_default_watermark(spark, database):
    spark.createDataFrame([], SCHEMA).write.format("delta").saveAsTable(f"`{database}`.`SUBJECTS`")
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK)

    assert TableSource().read(ctx).count() == 1


def test_no_deletes_feed_returns_none(spark, database):
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK)

    assert TableSource().read_deletes(ctx) is None


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

    rows = TableSource().read_deletes(ctx).collect()

    assert [row["ID"] for row in rows] == ["8"]


def test_the_deletes_feed_streams_under_the_checkpoint_strategy(spark, database):
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))])
    _write(spark, database, "DELETES", [("9", datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.CHECKPOINT, deletes="DELETES")

    assert TableSource().read_deletes(ctx).isStreaming


def test_updates_and_deletes_are_cut_at_the_same_watermark(spark, database):
    """Both feeds belong to one run; a target that moves mid-run must not split them."""
    _write(spark, database, "SUBJECTS", [("1", datetime(2024, 1, 1))])
    _write(spark, database, "UPDATES", [("2", datetime(2024, 1, 2))])
    _write(spark, database, "DELETES", [("3", datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK, deletes="DELETES")
    source = TableSource()

    assert source.read(ctx).count() == 1

    _write(spark, database, "SUBJECTS", [("1", datetime(2024, 1, 5))])

    assert source.read_deletes(ctx).count() == 1


def test_the_resolved_watermark_is_logged(spark, database):
    _write(spark, database, "SUBJECTS", [("1", datetime(2024, 1, 3))])
    _write(spark, database, "UPDATES", [("2", datetime(2024, 1, 4))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK)

    TableSource().read(ctx)

    logged = ctx.logger.info.call_args
    assert logged.kwargs["name"] == "watermark_resolved"
    assert "2024-01-03" in logged.kwargs["description"]


def _drain(ctx, sink: str, checkpoint: str) -> None:
    query = (
        TableSource()
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


SNAPSHOT_SCHEMA = "ID string, __ANCHOR_DT timestamp, __SOURCE string, __EXPORT_DATE timestamp"


def _snapshot(spark, database, table, rows, mode="append", schema=SNAPSHOT_SCHEMA):
    spark.createDataFrame(_with_source(rows), schema).write.format("delta").mode(mode).saveAsTable(
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

    assert TableSource().read(ctx).count() == 0


def test_a_record_anchor_still_picks_up_a_changed_record(spark, database):
    _snapshot(spark, database, "SUBJECTS", [("1", datetime(2024, 1, 1), datetime(2024, 1, 1))])
    _snapshot(spark, database, "UPDATES", [("1", datetime(2024, 1, 1), datetime(2024, 1, 2))])
    _snapshot(spark, database, "UPDATES", [("2", datetime(2024, 3, 1), datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK, increment_anchor=True)

    rows = TableSource().read(ctx).collect()

    assert [row["ID"] for row in rows] == ["2"]


def test_the_anchor_filter_reaches_the_scan(spark, database):
    """A plain comparison on __ANCHOR_DT, not a parse, so file skipping can use it."""
    _snapshot(spark, database, "UPDATES", [("1", datetime(2024, 1, 2), datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK, increment_anchor=True)

    plan = TableSource().read(ctx)._jdf.queryExecution().executedPlan().toString()

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
    source = TableSource()

    with pytest.raises(PlatformPolicyViolation, match=f"`{unstamped}` has no __ANCHOR_DT"):
        source.read(ctx)
        source.read_deletes(ctx)


def test_the_resolved_anchor_is_named_in_the_log(spark, database):
    _snapshot(spark, database, "SUBJECTS", [("1", datetime(2024, 1, 1), datetime(2024, 1, 1))])
    _snapshot(spark, database, "UPDATES", [("1", datetime(2024, 1, 2), datetime(2024, 1, 2))])
    ctx = _context(spark, database, IncrementStrategy.WATERMARK, increment_anchor=True)

    TableSource().read(ctx)

    assert "__ANCHOR_DT" in ctx.logger.info.call_args.kwargs["description"]


# --- full read -------------------------------------------------------------------


def test_a_full_read_returns_the_whole_table_as_one_batch(spark, database):
    _foreign(spark, database, "UPDATES", [("1", "alice"), ("2", "bob")])
    ctx = _context(spark, database, IncrementStrategy.FULL_READ)

    result = TableSource().read(ctx)

    assert not result.isStreaming
    assert sorted(row["ID"] for row in result.collect()) == ["1", "2"]


def test_a_full_read_stamps_every_row_as_one_snapshot_taken_at_the_read_time(spark, database):
    _foreign(spark, database, "UPDATES", [("1", "alice"), ("2", "bob")])
    ctx = _context(spark, database, IncrementStrategy.FULL_READ)

    rows = TableSource().read(ctx).collect()

    for row in rows:
        assert row["__EXPORT_DATE"] == READ_TIME
        assert row["__BRONZE_LAST_MODIFIED_DT"] == READ_TIME
        assert row["__SOURCE"] == f"TABLE:{database}.UPDATES@{READ_TIME.isoformat()}"


def test_a_full_read_logs_that_it_stamped_the_table(spark, database):
    _foreign(spark, database, "UPDATES", [("1", "alice")])
    ctx = _context(spark, database, IncrementStrategy.FULL_READ)

    TableSource().read(ctx)

    logged = ctx.logger.info.call_args.kwargs
    assert logged["name"] == "table_stamped"
    assert READ_TIME.isoformat() in logged["description"]


def test_updates_and_deletes_read_in_full_share_one_snapshot(spark, database):
    """Two stamps would split one read into two snapshots, the deletes replayed apart."""
    _foreign(spark, database, "UPDATES", [("1", "alice")])
    _foreign(spark, database, "DELETES", [("9", "gone")])
    ctx = _context(spark, database, IncrementStrategy.FULL_READ, deletes="DELETES")
    source = TableSource()

    updates = source.read(ctx).select("__EXPORT_DATE").first()[0]
    deletes = source.read_deletes(ctx).select("__EXPORT_DATE").first()[0]

    assert updates == deletes == READ_TIME


@pytest.mark.parametrize("strategy", [IncrementStrategy.FULL_READ, IncrementStrategy.DELTA_READ])
def test_a_foreign_read_refuses_a_table_we_already_stamped(spark, database, strategy):
    """Our own Bronze holds its exports: re-reading them appends or replays them twice."""
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))])
    ctx = _context(spark, database, strategy)

    with pytest.raises(PlatformPolicyViolation, match=f"stamped_table: .* a {strategy.value}"):
        TableSource().read(ctx)
    assert ctx.logger.error.call_args.kwargs["name"] == "stamped_table"


@pytest.mark.parametrize("strategy", [IncrementStrategy.CHECKPOINT, IncrementStrategy.WATERMARK])
def test_only_a_full_or_delta_read_may_read_a_table_we_did_not_create(spark, database, strategy):
    """Stamping an increment would pass it off as a whole snapshot, one per micro-batch.

    FULL would then keep the last micro-batch and drop the rest, so only a read that
    stamps once per run, full or delta, may read a table we didn't create.
    """
    _foreign(spark, database, "UPDATES", [("1", "alice")])
    ctx = _context(spark, database, strategy)

    with pytest.raises(
        PlatformPolicyViolation,
        match="unstamped_table: .*increment_strategy=full_read or delta_read",
    ):
        TableSource().read(ctx)
    assert ctx.logger.error.call_args.kwargs["name"] == "unstamped_table"


@pytest.mark.parametrize(
    ("schema", "missing"),
    [
        ("ID string, __EXPORT_DATE timestamp", "__SOURCE"),
        ("ID string, __SOURCE string", "__EXPORT_DATE"),
    ],
)
@pytest.mark.parametrize("strategy", list(IncrementStrategy))
def test_a_table_with_part_of_our_metadata_is_malformed(
    spark, database, schema, missing, strategy
):
    """Neither ours nor foreign: guessing which would either re-stamp or misread it."""
    value = datetime(2024, 1, 2) if "__EXPORT_DATE" in schema else _FILE
    _foreign(spark, database, "UPDATES", [("1", value)], schema=schema)
    ctx = _context(spark, database, strategy)

    with pytest.raises(PlatformPolicyViolation, match=f"incomplete metadata: missing {missing}"):
        TableSource().read(ctx)
    assert ctx.logger.error.call_args.kwargs["name"] == "malformed_table"


# --- delta read ------------------------------------------------------------------

CHANGED = "ID string, MODIFIED_AT timestamp"


def _target(spark, database, anchor):
    """The target a previous delta read filled, its highest __ANCHOR_DT at `anchor`."""
    _snapshot(spark, database, "SUBJECTS", [("1", anchor, READ_TIME)])


def _ids(df):
    return sorted(row["ID"] for row in df.collect())


def test_a_first_delta_read_takes_every_row(spark, database):
    _foreign(
        spark,
        database,
        "UPDATES",
        [("1", datetime(2024, 1, 2)), ("2", datetime(1999, 1, 1))],
        CHANGED,
    )
    ctx = _context(spark, database, IncrementStrategy.DELTA_READ)

    assert _ids(TableSource().read(ctx)) == ["1", "2"]


def test_a_delta_read_takes_only_rows_changed_since_the_target(spark, database):
    _target(spark, database, datetime(2024, 1, 3))
    _foreign(
        spark,
        database,
        "UPDATES",
        [("1", datetime(2024, 1, 2)), ("2", datetime(2024, 1, 3)), ("3", datetime(2024, 1, 4))],
        CHANGED,
    )
    ctx = _context(spark, database, IncrementStrategy.DELTA_READ)

    assert _ids(TableSource().read(ctx)) == ["3"]


def test_a_delta_read_stamps_the_read_and_copies_the_column_it_follows(spark, database):
    _foreign(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))], CHANGED)
    ctx = _context(spark, database, IncrementStrategy.DELTA_READ)

    row = TableSource().read(ctx).collect()[0]

    assert row["__ANCHOR_DT"] == datetime(2024, 1, 2)
    assert row["__EXPORT_DATE"] == READ_TIME
    assert row["__SOURCE"] == f"TABLE:{database}.UPDATES@{READ_TIME.isoformat()}"
    assert ctx.logger.info.call_args.kwargs["name"] == "table_stamped"


def test_a_delta_read_on_a_date_skips_the_watermark_day(spark, database):
    """The documented trade-off: a row changed later on the last day read is missed."""
    _target(spark, database, datetime(2024, 1, 3))
    _foreign(
        spark,
        database,
        "UPDATES",
        [("1", datetime(2024, 1, 3).date()), ("2", datetime(2024, 1, 4).date())],
        "ID string, MODIFIED_AT date",
    )
    ctx = _context(spark, database, IncrementStrategy.DELTA_READ)

    rows = TableSource().read(ctx).collect()

    assert [row["ID"] for row in rows] == ["2"]
    assert rows[0]["__ANCHOR_DT"] == datetime(2024, 1, 4)


def test_a_delta_read_follows_a_timestamp_without_time_zone(spark, database):
    _target(spark, database, datetime(2024, 1, 3))
    _foreign(
        spark,
        database,
        "UPDATES",
        [("1", datetime(2024, 1, 3)), ("2", datetime(2024, 1, 4))],
        "ID string, MODIFIED_AT timestamp_ntz",
    )
    ctx = _context(spark, database, IncrementStrategy.DELTA_READ)

    assert _ids(TableSource().read(ctx)) == ["2"]


def test_a_delta_read_names_the_column_as_the_source_has_it_in_any_case(spark, database):
    _foreign(
        spark,
        database,
        "UPDATES",
        [("1", datetime(2024, 1, 2))],
        "ID string, modified_at timestamp",
    )
    ctx = _context(spark, database, IncrementStrategy.DELTA_READ)

    assert _ids(TableSource().read(ctx)) == ["1"]


def test_the_delta_read_filter_reaches_the_scan(spark, database):
    """A plain comparison on the source column, so the source can apply it."""
    _foreign(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))], CHANGED)
    ctx = _context(spark, database, IncrementStrategy.DELTA_READ)

    plan = TableSource().read(ctx)._jdf.queryExecution().executedPlan().toString()

    assert re.search(r"DataFilters: \[[^\]]*\(MODIFIED_AT#\d+ >", plan), plan


def test_a_delta_read_refuses_a_column_the_source_lacks(spark, database):
    _foreign(spark, database, "UPDATES", [("1", "alice")])
    ctx = _context(spark, database, IncrementStrategy.DELTA_READ)

    with pytest.raises(PlatformPolicyViolation, match="MODIFIED_AT is not in"):
        TableSource().read(ctx)
    assert ctx.logger.error.call_args.kwargs["name"] == "anchor_column_missing"


def test_a_delta_read_refuses_a_column_that_is_not_a_date(spark, database):
    _foreign(spark, database, "UPDATES", [("1", "2024-01-02")], "ID string, MODIFIED_AT string")
    ctx = _context(spark, database, IncrementStrategy.DELTA_READ)

    with pytest.raises(PlatformPolicyViolation, match="is string: a delta_read follows a DATE"):
        TableSource().read(ctx)
    assert ctx.logger.error.call_args.kwargs["name"] == "anchor_wrong_type"


def test_a_delta_read_refuses_a_target_without_anchor_dt(spark, database):
    """Its watermark would be NULL, so every run would read the whole source again."""
    _write(spark, database, "SUBJECTS", [("1", datetime(2024, 1, 3))])
    _foreign(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))], CHANGED)
    ctx = _context(spark, database, IncrementStrategy.DELTA_READ)

    with pytest.raises(PlatformPolicyViolation, match="`SUBJECTS` has no __ANCHOR_DT"):
        TableSource().read(ctx)


def test_the_deletes_table_is_held_to_the_same_rule(spark, database):
    _write(spark, database, "UPDATES", [("1", datetime(2024, 1, 2))])
    _foreign(spark, database, "DELETES", [("9", datetime(2024, 1, 2))], "ID string, X timestamp")
    ctx = _context(spark, database, IncrementStrategy.WATERMARK, deletes="DELETES")

    with pytest.raises(PlatformPolicyViolation, match="`DELETES` carries none"):
        TableSource().read_deletes(ctx)
