"""Tests for the step_promoted record: the recipe for a target's state on any date (gh #69)."""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from dbx_flame.context.config import (
    DeletesConfig,
    EventTimeConfig,
    IncrementStrategy,
    Origin,
    OutputConfig,
    SnapshotScope,
    SourceConfig,
    TaskConfig,
    Verb,
)
from dbx_flame.context.context import Context, RunIdentity
from dbx_flame.output.delta import complete_delta
from dbx_flame.output.delta.append import AppendWriter
from dbx_flame.output.delta.complete_delta import CompleteDeltaWriter
from dbx_flame.output.delta.full import FullWriter
from dbx_flame.output.delta.scd2 import Scd2Writer
from dbx_flame.output.delta.upsert import UpsertWriter
from dbx_flame.output.mechanics import table_version
from dbx_flame.pipelines.enrichment import _record_hash

RUN = RunIdentity(
    workflow_id="wf-1",
    workflow_run_id="wfrun-1",
    task_key="steps",
    task_run_id="taskrun-1",
)

EVENT_FORMAT = "yyyy-MM-dd HH:mm:ss"
EXPORTS = "ID string, PAYLOAD string, UPDATEDTIME string, __SOURCE string, __EXPORT_DATE timestamp"
DELETIONS = "ID string, DELETEDTIME string, __SOURCE string, __EXPORT_DATE timestamp"

KEYED = dict(
    keys=["ID"],
    event_time=EventTimeConfig(column="UPDATEDTIME", format=EVENT_FORMAT),
)
DELETES = dict(
    deletes=DeletesConfig(
        keys=["ID"],
        event_time=EventTimeConfig(column="DELETEDTIME", format=EVENT_FORMAT),
    )
)


@pytest.fixture
def database(spark):
    name = f"test_steps_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{name}`")
    yield name
    spark.sql(f"DROP DATABASE IF EXISTS `{name}` CASCADE")


def _ctx(spark, database, verb, deletes_table=None, **output):
    config = TaskConfig(
        catalog="cro",
        env="dev_01",
        metadata_path="/Volumes/cro_dev_01/meta/",
        source=SourceConfig(
            origin=Origin.TABLE,
            schema_name="bronze",
            table="SOURCE",
            **({"deletes_table": deletes_table} if deletes_table else {}),
        ),
        output=OutputConfig(verb=verb, schema_name="silver", table="TARGET", **output),
    )
    return Context(
        config=config,
        spark=spark,
        run=RUN,
        logger=MagicMock(),
        catalog="cro_dev_01",
        source_table=f"`{database}`.`SOURCE`",
        deletes_table=f"`{database}`.`{deletes_table}`" if deletes_table else None,
        target_table=f"`{database}`.`TARGET`",
        inbound_glob=None,
        checkpoint_location="/tmp/cp/",
        schema_hints_location="/tmp/hints/",
        increment_strategy=IncrementStrategy.WATERMARK,
    )


def _file(day: int, name: str = "subjects") -> str:
    return f"FILE:/Volumes/in/{name}/{name}_2026010{day}120000.csv"


def _exports(spark, rows):
    """(id, payload, day) rows, each from the export of that day."""
    return _hashed(
        spark.createDataFrame(
            [
                (key, payload, f"2026-01-0{day} 00:00:00", _file(day), datetime(2026, 1, day, 12))
                for key, payload, day in rows
            ],
            EXPORTS,
        )
    )


def _hashed(df):
    """Hashed as the pipeline hashes a row where it enters, before any writer sees it."""
    return df.withColumn("__RECORD_HASH", _record_hash(df))


def _deletions(spark, database, rows):
    """(id, day) delete records, each from the deletes export of that day."""
    _hashed(
        spark.createDataFrame(
            [
                (
                    key,
                    f"2026-01-0{day} 00:00:00",
                    _file(day, "deletions"),
                    datetime(2026, 1, day, 12),
                )
                for key, day in rows
            ],
            DELETIONS,
        )
    ).write.format("delta").saveAsTable(f"`{database}`.`DELETIONS`")


def _steps(ctx) -> list[tuple[str, str, str, int]]:
    """The step_promoted records as (mode, feed, source, version), in the order logged."""
    records = [
        json.loads(call.kwargs["metadata"])
        for call in ctx.logger.info.call_args_list
        if call.kwargs["name"] == "step_promoted"
    ]
    return [(r["mode"], r["feed"], r["source"], r["version"]) for r in records]


def _version(spark, database) -> int:
    version = table_version(spark, f"`{database}`.`TARGET`")
    assert version is not None
    return version


# --- one record per source, with the version after the step ------------------


def test_append_records_each_source_it_appended(spark, database):
    ctx = _ctx(spark, database, Verb.APPEND)

    AppendWriter().write(_exports(spark, [("A", "v1", 1), ("B", "v1", 2)]), ctx)

    assert _steps(ctx) == [
        ("APPEND", "updates", _file(1), 0),
        ("APPEND", "updates", _file(2), 0),
    ]


def test_a_step_reaches_the_audit_table_as_soon_as_it_commits(spark, database):
    """Flushed per step, not at the end of the run: a driver crash cannot lose it (gh #5)."""
    ctx = _ctx(spark, database, Verb.APPEND)

    AppendWriter().write(_exports(spark, [("A", "v1", 1)]), ctx)

    ctx.logger.flush.assert_called_once()


def test_full_records_only_the_export_it_kept(spark, database):
    """Export 1 is superseded within the batch: it never reached the target."""
    ctx = _ctx(spark, database, Verb.FULL)

    FullWriter().write(_exports(spark, [("A", "v1", 1), ("A", "v2", 2)]), ctx)

    assert _steps(ctx) == [("REPLACE", "updates", _file(2), 0)]


@pytest.mark.parametrize("verb, writer", [(Verb.UPSERT, UpsertWriter), (Verb.SCD2, Scd2Writer)])
def test_a_merge_records_every_source_handed_to_it(spark, database, verb, writer):
    ctx = _ctx(spark, database, verb, **KEYED)

    writer().write(_exports(spark, [("A", "v1", 1)]), ctx)
    writer().write(_exports(spark, [("A", "v2", 2), ("B", "v1", 2)]), ctx)

    after = _version(spark, database)
    assert _steps(ctx) == [
        ("MERGE", "updates", _file(1), 0),
        ("MERGE", "updates", _file(2), after),
    ]


# --- no step without a change -------------------------------------------------


def test_an_empty_append_records_no_step(spark, database):
    ctx = _ctx(spark, database, Verb.APPEND)
    AppendWriter().write(_exports(spark, [("A", "v1", 1)]), ctx)

    AppendWriter().write(_exports(spark, []), ctx)

    assert _steps(ctx) == [("APPEND", "updates", _file(1), 0)]


def test_a_skipped_full_load_records_no_step(spark, database):
    ctx = _ctx(spark, database, Verb.FULL)
    FullWriter().write(_exports(spark, [("A", "v2", 2)]), ctx)

    FullWriter().write(_exports(spark, [("A", "v1", 1)]), ctx)

    assert _steps(ctx) == [("REPLACE", "updates", _file(2), 0)]


@pytest.mark.parametrize("verb, writer", [(Verb.UPSERT, UpsertWriter), (Verb.SCD2, Scd2Writer)])
def test_a_rerun_with_nothing_new_records_no_step(spark, database, verb, writer):
    """The merge may still commit, but it changed no rows: there is nothing to replay."""
    ctx = _ctx(spark, database, verb, **KEYED)
    batch = [("A", "v1", 1)]
    writer().write(_exports(spark, batch), ctx)

    writer().write(_exports(spark, batch), ctx)

    assert _steps(ctx) == [("MERGE", "updates", _file(1), 0)]


# --- COMPLETE_DELTA: one step per snapshot ------------------------------------


def test_complete_delta_records_each_snapshot_after_its_deletes(spark, database):
    """Snapshot 2's updates and deletes are one step: they share the version after both."""
    ctx = _ctx(spark, database, Verb.COMPLETE_DELTA, deletes_table="DELETIONS", **KEYED, **DELETES)
    _deletions(spark, database, [("B", 2)])

    CompleteDeltaWriter().write(
        _exports(spark, [("A", "v1", 1), ("B", "v1", 1), ("A", "v2", 2)]), ctx
    )

    after = _version(spark, database)
    assert _steps(ctx) == [
        ("MERGE", "updates", _file(1), 0),
        ("MERGE", "updates", _file(2), after),
        ("MERGE", "deletes", _file(2, "deletions"), after),
    ]
    assert ctx.logger.flush.call_count == 2


def test_complete_delta_full_scope_records_the_newest_export_as_a_replace(spark, database):
    """The superseded export is not a step; deletes stay a merge."""
    ctx = _ctx(
        spark,
        database,
        Verb.COMPLETE_DELTA,
        deletes_table="DELETIONS",
        snapshot_scope=SnapshotScope.FULL,
        **KEYED,
        **DELETES,
    )
    _deletions(spark, database, [("A", 3)])

    CompleteDeltaWriter().write(_exports(spark, [("A", "v1", 1), ("A", "v2", 2)]), ctx)

    first, after = 0, _version(spark, database)
    assert _steps(ctx) == [
        ("REPLACE", "updates", _file(2), first),
        ("MERGE", "deletes", _file(3, "deletions"), after),
    ]


def test_complete_delta_with_nothing_pending_records_no_step(spark, database):
    ctx = _ctx(spark, database, Verb.COMPLETE_DELTA, **KEYED)
    CompleteDeltaWriter().write(_exports(spark, [("A", "v1", 1)]), ctx)

    CompleteDeltaWriter().write(_exports(spark, []), ctx)

    assert _steps(ctx) == [("MERGE", "updates", _file(1), 0)]


def test_a_run_that_dies_mid_snapshot_records_only_the_snapshots_it_finished(
    spark, database, monkeypatch
):
    """Snapshot 2's updates commit, its deletes don't: it is half-applied, not a step (gh #12)."""
    ctx = _ctx(spark, database, Verb.COMPLETE_DELTA, deletes_table="DELETIONS", **KEYED, **DELETES)
    _deletions(spark, database, [("A", 2)])
    applied = complete_delta._apply_deletes

    def dies_on_snapshot_2(df, ctx):
        if df.isEmpty():
            return applied(df, ctx)
        raise RuntimeError("driver lost")

    monkeypatch.setattr(complete_delta, "_apply_deletes", dies_on_snapshot_2)

    with pytest.raises(RuntimeError, match="driver lost"):
        CompleteDeltaWriter().write(_exports(spark, [("A", "v1", 1), ("A", "v2", 2)]), ctx)

    assert _steps(ctx) == [("MERGE", "updates", _file(1), 0)]
