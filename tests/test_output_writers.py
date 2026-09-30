"""Tests for the P3 write verbs against a local Delta table."""

from __future__ import annotations

import uuid
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from pyspark.sql import functions as F

from dbx_flame.context.config import (
    DedupConfig,
    EventTimeConfig,
    IncrementStrategy,
    Origin,
    OutputConfig,
    SchemaEvolution,
    SourceConfig,
    TaskConfig,
    Verb,
)
from dbx_flame.context.context import Context, RunIdentity
from dbx_flame.output.delta.append import AppendWriter
from dbx_flame.output.delta.full import FullWriter
from dbx_flame.output.delta.upsert import UpsertWriter
from dbx_flame.output.mechanics import (
    EmptySourceSchemaError,
    UnexpectedColumnsError,
    schema_auto_merge,
)

RUN = RunIdentity(
    workflow_id="wf-1",
    workflow_run_id="wfrun-1",
    task_key="write",
    task_run_id="taskrun-1",
)

PEOPLE = "ID string, NAME string, UPDATED string"


@pytest.fixture
def database(spark):
    name = f"test_write_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{name}`")
    yield name
    spark.sql(f"DROP DATABASE IF EXISTS `{name}` CASCADE")


def _ctx(spark, database, verb=Verb.APPEND, schema_evolution=None, **output_overrides):
    output = dict(verb=verb, schema_name="silver", table="TARGET")
    output.update(output_overrides)
    config = TaskConfig(
        catalog="cro",
        env="dev_01",
        metadata_path="/Volumes/meta/",
        **({"schema_evolution": schema_evolution} if schema_evolution else {}),
        source=SourceConfig(origin=Origin.CSV, path="/Volumes/in/", directory="people"),
        output=OutputConfig(**output),
    )
    return Context(
        config=config,
        spark=spark,
        run=RUN,
        logger=MagicMock(),
        catalog="cro_dev_01",
        source_table=None,
        deletes_table=None,
        target_table=f"`{database}`.`TARGET`",
        inbound_glob="/Volumes/in/people/*.csv",
        checkpoint_location="/tmp/cp/",
        schema_hints_location="/tmp/hints/",
        increment_strategy=IncrementStrategy.CHECKPOINT,
    )


def _rows(spark, database):
    return {(row["ID"], row["NAME"]) for row in spark.table(f"`{database}`.`TARGET`").collect()}


def _people(spark, rows):
    return spark.createDataFrame(rows, PEOPLE)


# --- APPEND ----------------------------------------------------------------


def test_append_creates_the_target_on_first_write(spark, database):
    ctx = _ctx(spark, database)

    AppendWriter().write(_people(spark, [("1", "alice", "2024-01-01")]), ctx)

    assert _rows(spark, database) == {("1", "alice")}


def test_append_accumulates_across_writes(spark, database):
    ctx = _ctx(spark, database)
    writer = AppendWriter()

    writer.write(_people(spark, [("1", "alice", "2024-01-01")]), ctx)
    writer.write(_people(spark, [("2", "bob", "2024-01-02")]), ctx)

    assert _rows(spark, database) == {("1", "alice"), ("2", "bob")}


def test_append_reports_the_rows_it_wrote(spark, database):
    ctx = _ctx(spark, database)

    AppendWriter().write(_people(spark, [("1", "a", "x"), ("2", "b", "y")]), ctx)

    kpi = ctx.logger.kpi.call_args
    assert kpi.kwargs["name"] == "rows_appended"
    assert kpi.kwargs["total"] == 2


def test_a_columnless_batch_cannot_create_a_target(spark, database):
    """A target cannot be created from a batch with no schema to create it from."""
    ctx = _ctx(spark, database)

    with pytest.raises(EmptySourceSchemaError, match="no columns"):
        AppendWriter().write(spark.range(0).drop("id"), ctx)


# --- FULL ------------------------------------------------------------------


def test_full_replaces_the_target(spark, database):
    ctx = _ctx(spark, database, verb=Verb.FULL)
    writer = FullWriter()

    writer.write(_people(spark, [("1", "alice", "x")]), ctx)
    writer.write(_people(spark, [("2", "bob", "y")]), ctx)

    assert _rows(spark, database) == {("2", "bob")}


def test_full_never_wipes_the_target_with_an_empty_batch(spark, database):
    """A missing export must not destroy yesterday's data."""
    ctx = _ctx(spark, database, verb=Verb.FULL)
    writer = FullWriter()
    writer.write(_people(spark, [("1", "alice", "x")]), ctx)

    writer.write(_people(spark, []), ctx)

    assert _rows(spark, database) == {("1", "alice")}


EXPORTS = f"{PEOPLE}, __EXPORT_DATE timestamp"


def _exports(spark, rows):
    """People rows tagged with the day of the export they arrived in."""
    return spark.createDataFrame([(*row[:3], datetime(2024, 1, row[3])) for row in rows], EXPORTS)


def test_full_stamps_the_write_time_and_drops_the_bronze_one(spark, database):
    ctx = _ctx(spark, database, verb=Verb.FULL)
    bronze = _people(spark, [("1", "alice", "x")]).withColumn(
        "__BRONZE_LAST_MODIFIED_DT", F.current_timestamp()
    )

    FullWriter().write(bronze, ctx)

    target = spark.table(f"`{database}`.`TARGET`")
    assert "__BRONZE_LAST_MODIFIED_DT" not in target.columns
    assert isinstance(target.collect()[0]["__SILVER_LAST_MODIFIED_DT"], datetime)


def test_full_keeps_only_the_newest_export_of_a_backlog(spark, database):
    """Export 2 supersedes export 1: bob is dropped and alice is not written twice."""
    ctx = _ctx(spark, database, verb=Verb.FULL)

    FullWriter().write(
        _exports(
            spark,
            [
                ("1", "alice", "x", 1),
                ("2", "bob", "x", 1),
                ("1", "alice", "y", 2),
            ],
        ),
        ctx,
    )

    assert _rows(spark, database) == {("1", "alice")}
    assert spark.table(f"`{database}`.`TARGET`").count() == 1


def test_full_never_replaces_a_newer_export_with_an_older_one(spark, database):
    ctx = _ctx(spark, database, verb=Verb.FULL)
    writer = FullWriter()
    writer.write(_exports(spark, [("2", "bob", "y", 2)]), ctx)

    writer.write(_exports(spark, [("1", "alice", "x", 1)]), ctx)

    assert _rows(spark, database) == {("2", "bob")}
    assert ctx.logger.warning.call_args.kwargs["name"] == "full_load_skipped"
    assert ctx.logger.warning.call_args.kwargs["name"] == "full_load_skipped"


# --- UPSERT ----------------------------------------------------------------


def _upsert_ctx(spark, database, **overrides):
    defaults = dict(verb=Verb.UPSERT, keys=["ID"], event_time=EventTimeConfig(column="UPDATED"))
    defaults.update(overrides)
    return _ctx(spark, database, **defaults)


def test_upsert_creates_the_target_on_first_write(spark, database):
    ctx = _upsert_ctx(spark, database)

    UpsertWriter().write(_people(spark, [("1", "alice", "2024-01-01")]), ctx)

    assert _rows(spark, database) == {("1", "alice")}


def test_upsert_updates_a_matched_key_and_inserts_a_new_one(spark, database):
    ctx = _upsert_ctx(spark, database)
    writer = UpsertWriter()
    writer.write(_people(spark, [("1", "alice", "2024-01-01")]), ctx)

    writer.write(_people(spark, [("1", "ALICE", "2024-01-02"), ("2", "bob", "2024-01-02")]), ctx)

    assert _rows(spark, database) == {("1", "ALICE"), ("2", "bob")}


def test_upsert_keeps_the_target_when_the_batch_is_older(spark, database):
    """Newer-wins: a late-arriving old version must not overwrite a newer one."""
    ctx = _upsert_ctx(spark, database, event_time=EventTimeConfig(column="UPDATED"))
    writer = UpsertWriter()
    writer.write(_people(spark, [("1", "current", "2024-06-01")]), ctx)

    writer.write(_people(spark, [("1", "stale", "2024-01-01")]), ctx)

    assert _rows(spark, database) == {("1", "current")}


def test_upsert_applies_a_newer_batch(spark, database):
    ctx = _upsert_ctx(spark, database, event_time=EventTimeConfig(column="UPDATED"))
    writer = UpsertWriter()
    writer.write(_people(spark, [("1", "old", "2024-01-01")]), ctx)

    writer.write(_people(spark, [("1", "fresh", "2024-06-01")]), ctx)

    assert _rows(spark, database) == {("1", "fresh")}


def test_upsert_without_an_event_time_lets_the_batch_win(spark, database):
    ctx = _upsert_ctx(spark, database, event_time=None, dedup=DedupConfig(enabled=False))
    writer = UpsertWriter()
    writer.write(_people(spark, [("1", "first", "2024-06-01")]), ctx)

    writer.write(_people(spark, [("1", "second", "2024-01-01")]), ctx)

    assert _rows(spark, database) == {("1", "second")}


def test_upsert_collapses_duplicate_keys_within_one_batch(spark, database):
    ctx = _upsert_ctx(
        spark,
        database,
        dedup=DedupConfig(enabled=True, columns=["ID"], order_by="UPDATED"),
    )

    UpsertWriter().write(
        _people(spark, [("1", "old", "2024-01-01"), ("1", "new", "2024-06-01")]), ctx
    )

    assert _rows(spark, database) == {("1", "new")}


def test_upsert_dedups_by_keys_and_event_time_by_default(spark, database):
    ctx = _upsert_ctx(spark, database)

    UpsertWriter().write(
        _people(spark, [("1", "new", "2024-06-01"), ("1", "old", "2024-01-01")]), ctx
    )

    assert _rows(spark, database) == {("1", "new")}


def test_upsert_stamps_the_write_time(spark, database):
    ctx = _upsert_ctx(spark, database)

    UpsertWriter().write(_people(spark, [("1", "alice", "2024-01-01")]), ctx)

    row = spark.table(f"`{database}`.`TARGET`").collect()[0]
    assert isinstance(row["__SILVER_LAST_MODIFIED_DT"], datetime)


# --- schema evolution: one knob, the same meaning on every verb -------------

_AUTO_MERGE = "spark.databricks.delta.schema.autoMerge.enabled"

WIDER = "ID string, NAME string, UPDATED string, NICKNAME string"


def test_auto_merge_conf_is_set_from_the_knob_and_then_restored(spark, database):
    ctx = _upsert_ctx(spark, database, schema_evolution=SchemaEvolution.ADD_NEW_COLUMNS)

    assert spark.conf.get(_AUTO_MERGE, None) is None
    with schema_auto_merge(ctx):
        assert spark.conf.get(_AUTO_MERGE) == "true"
    assert spark.conf.get(_AUTO_MERGE, None) is None


def test_auto_merge_conf_restores_a_value_the_session_already_had(spark, database):
    """A job cluster runs many tasks in one session; this must not leak into the next."""
    spark.conf.set(_AUTO_MERGE, "true")
    ctx = _upsert_ctx(spark, database, schema_evolution=SchemaEvolution.FAIL_ON_NEW_COLUMNS)
    try:
        with schema_auto_merge(ctx):
            assert spark.conf.get(_AUTO_MERGE) == "false"
        assert spark.conf.get(_AUTO_MERGE) == "true"
    finally:
        spark.conf.unset(_AUTO_MERGE)


def test_upsert_adds_a_new_column_when_evolution_is_on(spark, database):
    ctx = _upsert_ctx(spark, database, schema_evolution=SchemaEvolution.ADD_NEW_COLUMNS)
    UpsertWriter().write(_people(spark, [("1", "alice", "2024-01-01")]), ctx)

    wider = spark.createDataFrame([("1", "alice", "2024-01-02", "al")], WIDER)
    UpsertWriter().write(wider, ctx)

    assert "NICKNAME" in spark.table(f"`{database}`.`TARGET`").columns


def test_upsert_refuses_a_new_column_when_evolution_is_off(spark, database):
    """Without this the MERGE accepts the batch and silently discards the column."""
    ctx = _upsert_ctx(spark, database, schema_evolution=SchemaEvolution.FAIL_ON_NEW_COLUMNS)
    UpsertWriter().write(_people(spark, [("1", "alice", "2024-01-01")]), ctx)

    wider = spark.createDataFrame([("1", "alice", "2024-01-02", "al")], WIDER)
    with pytest.raises(UnexpectedColumnsError, match="NICKNAME"):
        UpsertWriter().write(wider, ctx)

    assert "NICKNAME" not in spark.table(f"`{database}`.`TARGET`").columns
