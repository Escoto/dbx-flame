"""Tests for the composition root: dispatch, the batch chain, and the drivers."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import functions as F

from dbx_flame.context.config import (
    EventTimeConfig,
    IncrementStrategy,
    Origin,
    OutputConfig,
    SourceConfig,
    TaskConfig,
    TypingConfig,
    Verb,
)
from dbx_flame.context.context import Context, RunIdentity
from dbx_flame.entrypoints.pipeline import _gate_and_write, prepare, run_pipeline
from dbx_flame.output.base import Requirements
from dbx_flame.output.registry import WRITER_BY_VERB
from dbx_flame.pipelines.registry import SOURCES
from dbx_flame.policies.platform import PlatformPolicyViolation

RUN = RunIdentity("wf", "wfrun", "task", "taskrun")


def _ctx(spark, origin=Origin.CSV, **source_overrides):
    source = dict(origin=origin)
    if origin == Origin.TABLE:
        source.update(schema_name="bronze", table="PEOPLE")
    else:
        source.update(path="/Volumes/in/", directory="people")
    source.update(source_overrides)
    config = TaskConfig(
        catalog="cro",
        env="dev_01",
        metadata_path="/Volumes/cro_dev_01/meta/",
        source=SourceConfig(**source),
        typing=TypingConfig(),
        output=OutputConfig(verb=Verb.APPEND, schema_name="bronze", table="PEOPLE"),
    )
    return Context(
        config=config,
        spark=spark,
        run=RUN,
        logger=MagicMock(),
        catalog="cro_dev_01",
        source_table="`db`.`PEOPLE`",
        deletes_table=None,
        target_table="`db`.`PEOPLE`",
        inbound_glob="/Volumes/in/people/*.csv",
        checkpoint_location="/tmp/cp/",
        schema_hints_location="/tmp/hints/",
        increment_strategy=IncrementStrategy.CHECKPOINT,
    )


def test_every_origin_has_a_reader():
    assert set(SOURCES) == set(Origin)


def test_every_verb_has_a_writer():
    assert set(WRITER_BY_VERB) == set(Verb)


def test_a_file_origin_gets_provenance(spark, tmp_path):
    """run_pipeline attaches it to the source, so prepare() never touches _metadata."""
    fixture = tmp_path / "PEOPLE_20240115103000.csv"
    fixture.write_text("agent id,Full Name\n1,alice\n", encoding="utf-8")
    raw = spark.read.option("header", "true").csv(str(fixture))

    ctx = _ctx(spark)
    writer = MagicMock(requires=Requirements())

    with patch.dict(SOURCES, {Origin.CSV: lambda: MagicMock(read=lambda _c: raw)}):
        with patch.dict(WRITER_BY_VERB, {Verb.APPEND: lambda: writer}):
            run_pipeline(ctx)

    result = writer.write.call_args.args[0]
    row = result.collect()[0]
    assert result.columns == [
        "AGENT_ID",
        "FULL_NAME",
        "__SOURCE",
        "__EXPORT_DATE",
        "__RECORD_HASH",
    ]
    assert row["__EXPORT_DATE"] == datetime(2024, 1, 15, 10, 30)
    assert row["AGENT_ID"] == "1"


def test_provenance_is_attached_before_the_stream_not_inside_it(spark, tmp_path):
    """_metadata resolves on the file source only; a foreachBatch micro-batch has lost it.

    Adding it inside prepare() passed every local batch test and then failed on the
    first real Auto Loader run, so this pins it to the source DataFrame instead.
    """
    fixture = tmp_path / "PEOPLE_20240115103000.csv"
    fixture.write_text("id" + chr(10) + "1" + chr(10), encoding="utf-8")
    raw = spark.read.option("header", "true").csv(str(fixture))

    assert "__SOURCE" not in prepare(raw, _ctx(spark)).columns


def test_a_table_origin_keeps_the_provenance_it_arrived_with(spark):
    """Re-deriving it would need _metadata.file_path, which a table read does not have.

    The hash too: a later layer's rename or cast must not change the record's fingerprint.
    """
    df = spark.createDataFrame(
        [("1", "alice", datetime(2024, 1, 15), "upstream")],
        "ID string, NAME string, __EXPORT_DATE timestamp, __RECORD_HASH string",
    )

    result = prepare(df, _ctx(spark, origin=Origin.TABLE, rename_patterns=["^NAME$=FULL_NAME"]))

    assert result.columns == ["ID", "FULL_NAME", "__EXPORT_DATE", "__RECORD_HASH"]
    row = result.collect()[0]
    assert row["__EXPORT_DATE"] == datetime(2024, 1, 15)
    assert row["__RECORD_HASH"] == "upstream"


def test_the_hash_is_taken_after_the_rename_so_the_stored_row_reproduces_it(spark):
    """An auditor holding only the stored row, with its final names, gets the same hash.

    The rename flips the sort order (A_NAME < B_ID, but ID < NAME), so a hash ordered by
    the names before the rename would not match.
    """
    raw = spark.createDataFrame([("alice", "1")], "A_NAME string, B_ID string")
    stored = spark.createDataFrame([("1", "alice")], "ID string, NAME string")

    hashed = prepare(raw, _ctx(spark, rename_patterns=["^[AB]_="]))
    again = prepare(stored, _ctx(spark))

    assert hashed.collect()[0]["__RECORD_HASH"] == again.collect()[0]["__RECORD_HASH"]


def test_rename_patterns_run_after_sanitization(spark):
    df = spark.createDataFrame([("1",)], "`agent id__v` string")

    result = prepare(df, _ctx(spark, origin=Origin.TABLE, rename_patterns=["__[Vv]$="]))

    assert result.columns == ["AGENT_ID", "__RECORD_HASH"]


def test_a_batch_source_writes_directly(spark):
    ctx = _ctx(spark, origin=Origin.TABLE)
    df = spark.createDataFrame([("1",)], "ID string")
    writer = MagicMock(requires=Requirements())

    with patch.dict(SOURCES, {Origin.TABLE: lambda: MagicMock(read=lambda _ctx: df)}):
        with patch.dict(WRITER_BY_VERB, {Verb.APPEND: lambda: writer}):
            run_pipeline(ctx)

    writer.write.assert_called_once()
    assert writer.write.call_args.args[0].columns == ["ID", "__RECORD_HASH"]


def test_a_streaming_source_is_driven_and_awaited():
    """Returning without awaiting would let a task report success before the write."""
    ctx = MagicMock()
    # A table source, so the driver is exercised without provenance wrapping the mock.
    ctx.config.source.origin = Origin.TABLE
    ctx.config.output.verb = Verb.APPEND
    streaming = MagicMock()
    streaming.isStreaming = True

    with patch.dict(SOURCES, {Origin.TABLE: lambda: MagicMock(read=lambda _c: streaming)}):
        with patch.dict(WRITER_BY_VERB, {Verb.APPEND: lambda: MagicMock()}):
            run_pipeline(ctx)

    chain = streaming.writeStream.foreachBatch.return_value.option.return_value.trigger
    chain.assert_called_once_with(availableNow=True)
    chain.return_value.start.return_value.awaitTermination.assert_called_once()


@pytest.mark.parametrize("origin", [Origin.SAS])
def test_unimplemented_origins_fail_at_read_not_at_dispatch(origin):
    """The config is valid; the reader simply is not written yet."""
    assert origin in SOURCES

    with pytest.raises(NotImplementedError, match="P6"):
        SOURCES[origin]().read(MagicMock())


def test_only_a_keyed_verb_checks_its_event_time(spark):
    """On APPEND and FULL an event time is unused, so a NULL there mustn't fail the batch."""
    ctx = _ctx(spark, origin=Origin.TABLE)
    ctx.config.output.event_time = EventTimeConfig(column="UPDATED")
    df = spark.createDataFrame([("1", None)], "ID string, UPDATED string")

    unkeyed = MagicMock(requires=Requirements())
    _gate_and_write(df, ctx, unkeyed)
    unkeyed.write.assert_called_once()

    keyed = MagicMock(requires=Requirements(keys=True))
    with pytest.raises(PlatformPolicyViolation, match="event_time_invalid"):
        _gate_and_write(df, ctx, keyed)
    keyed.write.assert_not_called()


def _keyed_ctx(spark):
    ctx = _ctx(spark, origin=Origin.TABLE)
    ctx.config.output.keys = ["ID"]
    ctx.config.output.event_time = EventTimeConfig(column="UPDATED")
    return ctx


def _resent_key(spark):
    return spark.createDataFrame(
        [("1", "old", "2024-01-01"), ("1", "new", "2024-06-01"), ("2", "only", "2024-01-01")],
        "ID string, NAME string, UPDATED string",
    )


def _names(df) -> set[tuple[str, str]]:
    return {(row["ID"], row["NAME"]) for row in df.collect()}


def test_the_gate_and_the_writer_see_the_same_deduplicated_rows(spark):
    """Judging rows dedup is about to collapse would fail is_unique on data that lands fine."""
    writer = MagicMock(requires=Requirements(keys=True))

    with patch("dbx_flame.entrypoints.pipeline.PolicyRunner") as runner:
        _gate_and_write(_resent_key(spark), _keyed_ctx(spark), writer)

    gated = runner.return_value.run.call_args.args[0]
    written = writer.write.call_args.args[0]
    assert _names(gated) == {("1", "new"), ("2", "only")}
    assert _names(written) == _names(gated)


def test_with_dedup_off_the_batch_reaches_the_gate_unchanged(spark):
    ctx = _keyed_ctx(spark)
    ctx.config.output.dedup.enabled = False
    writer = MagicMock(requires=Requirements(keys=True))

    with patch("dbx_flame.entrypoints.pipeline.PolicyRunner") as runner:
        _gate_and_write(_resent_key(spark), ctx, writer)

    assert runner.return_value.run.call_args.args[0].count() == 3


def test_a_per_snapshot_verb_is_left_to_gate_its_own_snapshots(spark):
    writer = MagicMock(requires=Requirements(keys=True, per_snapshot=True))
    backlog = _resent_key(spark).withColumn("__EXPORT_DATE", F.current_timestamp())

    with patch("dbx_flame.entrypoints.pipeline.PolicyRunner") as runner:
        _gate_and_write(backlog, _keyed_ctx(spark), writer)

    runner.return_value.run.assert_not_called()
    writer.write.assert_called_once()
