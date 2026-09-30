"""Tests for the P4 history verbs — SCD2 and COMPLETE_DELTA — on a local Delta table.

COMPLETE_DELTA replays the SCD2 engine, so both share one context builder here: what
the verbs differ in is how the increment is cut, not what a history row looks like.
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from dbx_flame.context.config import (
    DedupConfig,
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
from dbx_flame.output.delta.complete_delta import CompleteDeltaWriter
from dbx_flame.output.delta.complete_delta import _at as snapshot_rows
from dbx_flame.output.delta.scd2 import Scd2Writer

RUN = RunIdentity(
    workflow_id="wf-1",
    workflow_run_id="wfrun-1",
    task_key="history",
    task_run_id="taskrun-1",
)

# The event time arrives as a string, as it does out of Bronze, so every test also
# exercises the typed comparison.
EVENT_FORMAT = "yyyy-MM-dd HH:mm:ss"

SUBJECTS = "ID string, PAYLOAD string, UPDATEDTIME string"
SNAPSHOT_SUBJECTS = f"{SUBJECTS}, __FILEPATH string, __EXPORT_DATE timestamp"
DELETIONS = "ID string, DELETEDTIME string, __FILEPATH string, __EXPORT_DATE timestamp"


@pytest.fixture
def database(spark):
    name = f"test_history_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{name}`")
    yield name
    spark.sql(f"DROP DATABASE IF EXISTS `{name}` CASCADE")


def _ctx(spark, database, verb=Verb.SCD2, source=None, **output_overrides):
    output = dict(
        verb=verb,
        schema_name="silver",
        table="TARGET",
        keys=["ID"],
        event_time=EventTimeConfig(column="UPDATEDTIME", format=EVENT_FORMAT),
    )
    output.update(output_overrides)
    config = TaskConfig(
        catalog="cro",
        env="dev_01",
        metadata_path="/Volumes/meta/",
        source=source or SourceConfig(origin=Origin.DELTA, schema_name="bronze", table="SOURCE"),
        output=OutputConfig(**output),
    )
    deletes_table = config.source.deletes_table
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


def _history(spark, database):
    """The target as (id, payload, start, end, current, deleted) tuples."""
    rows = spark.table(f"`{database}`.`TARGET`").collect()
    return {
        (
            row["ID"],
            row["PAYLOAD"],
            row["__START_DATE"],
            row["__END_DATE"],
            row["__CURRENT_FLAG"],
            row["__DELETED_FLAG"],
        )
        for row in rows
    }


def _at(day: int, hour: int = 0) -> datetime:
    return datetime(2026, 1, day, hour)


def _subjects(spark, rows):
    return spark.createDataFrame(rows, SUBJECTS)


# --- SCD2 ------------------------------------------------------------------


def test_scd2_creates_the_target_with_open_history_rows(spark, database):
    ctx = _ctx(spark, database)

    Scd2Writer().write(_subjects(spark, [("A", "v1", "2026-01-01 00:00:00")]), ctx)

    assert _history(spark, database) == {("A", "v1", _at(1), None, "Y", "N")}


def test_scd2_closes_the_previous_version_and_opens_a_new_one(spark, database):
    ctx = _ctx(spark, database)
    writer = Scd2Writer()

    writer.write(_subjects(spark, [("A", "v1", "2026-01-01 00:00:00")]), ctx)
    writer.write(_subjects(spark, [("A", "v2", "2026-01-02 00:00:00")]), ctx)

    assert _history(spark, database) == {
        ("A", "v1", _at(1), _at(2), "N", "N"),
        ("A", "v2", _at(2), None, "Y", "N"),
    }


def test_scd2_ignores_a_record_it_already_holds(spark, database):
    """Re-running the same batch must be a no-op, not a second identical version."""
    ctx = _ctx(spark, database)
    writer = Scd2Writer()
    batch = [("A", "v1", "2026-01-01 00:00:00")]

    writer.write(_subjects(spark, batch), ctx)
    writer.write(_subjects(spark, batch), ctx)

    assert _history(spark, database) == {("A", "v1", _at(1), None, "Y", "N")}


def test_scd2_ignores_an_older_version_of_a_record(spark, database):
    """A late file must not overwrite a newer state that is already current."""
    ctx = _ctx(spark, database)
    writer = Scd2Writer()

    writer.write(_subjects(spark, [("A", "v2", "2026-01-02 00:00:00")]), ctx)
    writer.write(_subjects(spark, [("A", "v1", "2026-01-01 00:00:00")]), ctx)

    assert _history(spark, database) == {("A", "v2", _at(2), None, "Y", "N")}


def test_scd2_keeps_only_the_latest_row_per_key_within_a_batch(spark, database):
    ctx = _ctx(
        spark,
        database,
        dedup=DedupConfig(
            enabled=True,
            columns=["ID"],
            order_by="UPDATEDTIME",
            order_by_format=EVENT_FORMAT,
        ),
    )

    Scd2Writer().write(
        _subjects(
            spark,
            [
                ("A", "v1", "2026-01-01 00:00:00"),
                ("A", "v2", "2026-01-02 00:00:00"),
            ],
        ),
        ctx,
    )

    assert _history(spark, database) == {("A", "v2", _at(2), None, "Y", "N")}


def test_scd2_leaves_a_key_the_newest_export_omits_untouched(spark, database):
    """SCD2 takes changes: B's absence from export 2 is not a deletion."""
    ctx = _ctx(spark, database)
    backlog = _snapshot_subjects(
        spark,
        [
            ("A", "v1", "2026-01-01 00:00:00", 1),
            ("B", "v1", "2026-01-01 00:00:00", 1),
            ("A", "v2", "2026-01-02 00:00:00", 2),
        ],
    )

    Scd2Writer().write(backlog, ctx)

    assert _history(spark, database) == {
        ("A", "v2", _at(2), None, "Y", "N"),
        ("B", "v1", _at(1), None, "Y", "N"),
    }


def test_scd2_prefers_the_newer_export_when_event_times_tie(spark, database):
    ctx = _ctx(spark, database)
    backlog = _snapshot_subjects(
        spark,
        [
            ("A", "v2", "2026-01-01 00:00:00", 2),
            ("A", "v1", "2026-01-01 00:00:00", 1),
        ],
    )

    Scd2Writer().write(backlog, ctx)

    assert _history(spark, database) == {("A", "v2", _at(1), None, "Y", "N")}


# --- COMPLETE_DELTA --------------------------------------------------------


def _file(day: int, name: str = "subjects") -> str:
    return f"/Volumes/in/{name}/{name}_2026010{day}120000.csv"


def _snapshot_subjects(spark, rows):
    return spark.createDataFrame(
        [(*row[:3], _file(row[3]), _at(row[3], 12)) for row in rows],
        SNAPSHOT_SUBJECTS,
    )


def _complete_delta_ctx(spark, database, output=None, **source_overrides):
    source = SourceConfig(
        origin=Origin.DELTA,
        schema_name="bronze",
        table="SOURCE",
        **source_overrides,
    )
    return _ctx(spark, database, verb=Verb.COMPLETE_DELTA, source=source, **(output or {}))


def test_complete_delta_replays_every_snapshot_in_order(spark, database):
    """The worked example from 04_write_verbs.md: a backlog must not collapse.

    Plain SCD2 over these three snapshots would dedup to A v3 and lose the v1 and v2
    transitions entirely.
    """
    ctx = _complete_delta_ctx(spark, database)
    updates = _snapshot_subjects(
        spark,
        [
            ("A", "v1", "2026-01-01 00:00:00", 1),
            ("B", "v1", "2026-01-01 00:00:00", 1),
            ("A", "v2", "2026-01-02 00:00:00", 2),
            ("A", "v3", "2026-01-03 00:00:00", 3),
        ],
    )

    CompleteDeltaWriter().write(updates, ctx)

    assert _history(spark, database) == {
        ("A", "v1", _at(1), _at(2), "N", "N"),
        ("A", "v2", _at(2), _at(3), "N", "N"),
        ("A", "v3", _at(3), None, "Y", "N"),
        ("B", "v1", _at(1), None, "Y", "N"),
    }


def test_complete_delta_applies_the_deletes_feed_of_each_snapshot(spark, database):
    """B is deleted in snapshot 2: flagged, closed, and still present in history."""
    ctx = _complete_delta_ctx(
        spark,
        database,
        deletes_table="DELETIONS",
        output={
            "deletes": DeletesConfig(
                keys=["ID"],
                event_time=EventTimeConfig(column="DELETEDTIME", format=EVENT_FORMAT),
            )
        },
    )
    spark.createDataFrame(
        [("B", "2026-01-02 00:00:00", _file(2), _at(2, 12))], DELETIONS
    ).write.format("delta").saveAsTable(f"`{database}`.`DELETIONS`")

    updates = _snapshot_subjects(
        spark,
        [
            ("A", "v1", "2026-01-01 00:00:00", 1),
            ("B", "v1", "2026-01-01 00:00:00", 1),
            ("A", "v2", "2026-01-02 00:00:00", 2),
        ],
    )

    CompleteDeltaWriter().write(updates, ctx)

    assert _history(spark, database) == {
        ("A", "v1", _at(1), _at(2), "N", "N"),
        ("A", "v2", _at(2), None, "Y", "N"),
        ("B", "v1", _at(1), _at(2), "N", "Y"),
    }


def test_complete_delta_full_scope_expires_records_a_snapshot_no_longer_carries(spark, database):
    """A full-snapshot source deletes by omission: C is absent from snapshot 2."""
    ctx = _complete_delta_ctx(spark, database, output={"snapshot_scope": SnapshotScope.FULL})
    writer = CompleteDeltaWriter()
    first = [("A", "v1", "2026-01-01 00:00:00", 1), ("C", "v1", "2026-01-01 00:00:00", 1)]

    writer.write(_snapshot_subjects(spark, first), ctx)
    writer.write(_snapshot_subjects(spark, [("A", "v2", "2026-01-02 00:00:00", 2)]), ctx)

    # Both close at snapshot 2's export time, not the run's. C is omitted rather than
    # retired by a deletes feed, so its flag stays clear.
    assert _history(spark, database) == {
        ("A", "v1", _at(1), _at(2, 12), "N", "N"),
        ("A", "v2", _at(2), None, "Y", "N"),
        ("C", "v1", _at(1), _at(2, 12), "N", "N"),
    }


def test_complete_delta_full_scope_replays_only_the_newest_pending_export(spark, database):
    """The newest export supersedes the older ones, so they never reach Silver."""
    ctx = _complete_delta_ctx(spark, database, output={"snapshot_scope": SnapshotScope.FULL})
    updates = _snapshot_subjects(
        spark,
        [
            ("A", "v1", "2026-01-01 00:00:00", 1),
            ("C", "v1", "2026-01-01 00:00:00", 1),
            ("A", "v2", "2026-01-02 00:00:00", 2),
        ],
    )

    CompleteDeltaWriter().write(updates, ctx)

    assert _history(spark, database) == {("A", "v2", _at(2), None, "Y", "N")}


def test_complete_delta_full_scope_leaves_the_table_alone_when_nothing_is_pending(spark, database):
    """No news is not an instruction to retire the whole table."""
    ctx = _complete_delta_ctx(spark, database, output={"snapshot_scope": SnapshotScope.FULL})
    writer = CompleteDeltaWriter()

    writer.write(_snapshot_subjects(spark, [("A", "v1", "2026-01-01 00:00:00", 1)]), ctx)
    writer.write(_snapshot_subjects(spark, []), ctx)

    assert _history(spark, database) == {("A", "v1", _at(1), None, "Y", "N")}


def test_complete_delta_creates_an_empty_target_when_nothing_is_pending(spark, database):
    ctx = _complete_delta_ctx(spark, database)

    CompleteDeltaWriter().write(_snapshot_subjects(spark, []), ctx)

    target = spark.table(f"`{database}`.`TARGET`")
    assert target.count() == 0
    assert "__CURRENT_FLAG" in target.columns


def test_complete_delta_splits_snapshots_on_the_export_date_alone(spark, database):
    """Bronze already parsed the stamp; the file name is not read again here."""
    ctx = _complete_delta_ctx(spark, database)
    updates = spark.createDataFrame(
        [
            ("A", "v1", "2026-01-01 00:00:00", "/in/subjects_2026-01-01T12:00:00.csv", _at(1, 12)),
            ("A", "v2", "2026-01-02 00:00:00", "/in/no_stamp_at_all.csv", _at(2, 12)),
        ],
        SNAPSHOT_SUBJECTS,
    )

    CompleteDeltaWriter().write(updates, ctx)

    assert _history(spark, database) == {
        ("A", "v1", _at(1), _at(2), "N", "N"),
        ("A", "v2", _at(2), None, "Y", "N"),
    }


def test_each_snapshot_is_cut_by_a_filter_that_reaches_the_scan(spark, database):
    """A plain comparison on __EXPORT_DATE, so a merge can skip the other exports' files."""
    rows = [("A", "v1", "2026-01-01 00:00:00", 1), ("A", "v2", "2026-01-02 00:00:00", 2)]
    _snapshot_subjects(spark, rows).write.format("delta").saveAsTable(f"`{database}`.`SOURCE`")
    pending = spark.table(f"`{database}`.`SOURCE`")

    plan = snapshot_rows(pending, _at(1, 12))._jdf.queryExecution().executedPlan().toString()

    assert re.search(r"DataFilters: \[[^\]]*\(__EXPORT_DATE#\d+ = ", plan), plan
