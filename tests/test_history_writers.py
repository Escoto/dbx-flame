"""Tests for the P4 history verbs — SCD2 and COMPLETE_DELTA — on a local Delta table.

COMPLETE_DELTA replays the SCD2 engine, so both share one context builder here: what
the verbs differ in is how the increment is cut, not what a history row looks like.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import replace
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from pyspark.sql import functions as F

from dbx_flame.context.config import (
    DedupConfig,
    DeletesConfig,
    EventTimeConfig,
    IncrementStrategy,
    Origin,
    OutputConfig,
    SchemaEvolution,
    SnapshotScope,
    SourceConfig,
    TaskConfig,
    Verb,
)
from dbx_flame.context.context import Context, RunIdentity
from dbx_flame.entrypoints.pipeline import _gate_and_write
from dbx_flame.output.delta.complete_delta import CompleteDeltaWriter
from dbx_flame.output.delta.complete_delta import _at as snapshot_rows
from dbx_flame.output.delta.scd2 import Scd2Writer
from dbx_flame.output.delta.upsert import UpsertWriter
from dbx_flame.pipelines.enrichment import _record_hash
from dbx_flame.policies.base import PolicyViolation
from dbx_flame.policies.platform import PlatformPolicyViolation

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
SNAPSHOT_SUBJECTS = f"{SUBJECTS}, __SOURCE string, __EXPORT_DATE timestamp"
DELETIONS = "ID string, DELETEDTIME string, __SOURCE string, __EXPORT_DATE timestamp"


@pytest.fixture
def database(spark):
    name = f"test_history_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{name}`")
    yield name
    spark.sql(f"DROP DATABASE IF EXISTS `{name}` CASCADE")


def _ctx(
    spark,
    database,
    verb=Verb.SCD2,
    source=None,
    schema_evolution=SchemaEvolution.FAIL_ON_NEW_COLUMNS,
    **output_overrides,
):
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
        metadata_path="/Volumes/cro_dev_01/meta/",
        schema_evolution=schema_evolution,
        source=source or SourceConfig(origin=Origin.TABLE, schema_name="bronze", table="SOURCE"),
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


def _subjects(spark, rows, source="FILE:/Volumes/in/subjects/subjects.csv"):
    """Subjects as a writer receives them: provenance is stamped where data enters."""
    return _hashed(spark.createDataFrame(rows, SUBJECTS).withColumn("__SOURCE", F.lit(source)))


def _hashed(df):
    """Hashed as the pipeline hashes a row where it enters, before any writer sees it."""
    return df.withColumn("__RECORD_HASH", _record_hash(df))


FIRST_RUN = datetime(2030, 1, 1)
SECOND_RUN = datetime(2030, 1, 2)


def _stamps(spark, database):
    """The target as (id, payload, current, deleted, __LAST_MODIFIED_DT) tuples."""
    rows = spark.table(f"`{database}`.`TARGET`").collect()
    return {
        (
            row["ID"],
            row["PAYLOAD"],
            row["__CURRENT_FLAG"],
            row["__DELETED_FLAG"],
            row["__LAST_MODIFIED_DT"],
        )
        for row in rows
    }


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

    _gate_and_write(
        _subjects(
            spark,
            [
                ("A", "v1", "2026-01-01 00:00:00"),
                ("A", "v2", "2026-01-02 00:00:00"),
            ],
        ),
        ctx,
        Scd2Writer(),
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

    _gate_and_write(backlog, ctx, Scd2Writer())

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

    _gate_and_write(backlog, ctx, Scd2Writer())

    assert _history(spark, database) == {("A", "v2", _at(1), None, "Y", "N")}


def _with_nickname(df):
    return df.withColumn("NICKNAME", F.lit("al"))


def test_scd2_refuses_a_new_column_before_closing_anything(spark, database):
    """Refused after the close, the insert would leave A with no current row."""
    ctx = _ctx(spark, database)
    writer = Scd2Writer()
    writer.write(_subjects(spark, [("A", "v1", "2026-01-01 00:00:00")]), ctx)

    wider = _with_nickname(_subjects(spark, [("A", "v2", "2026-01-02 00:00:00")]))
    with pytest.raises(PlatformPolicyViolation, match="NICKNAME"):
        writer.write(wider, ctx)

    assert _history(spark, database) == {("A", "v1", _at(1), None, "Y", "N")}
    assert ctx.logger.error.call_args.kwargs["name"] == "unexpected_columns"


def test_scd2_adds_a_new_column_when_evolution_is_on(spark, database):
    ctx = _ctx(spark, database, schema_evolution=SchemaEvolution.ADD_NEW_COLUMNS)
    writer = Scd2Writer()
    writer.write(_subjects(spark, [("A", "v1", "2026-01-01 00:00:00")]), ctx)

    writer.write(_with_nickname(_subjects(spark, [("A", "v2", "2026-01-02 00:00:00")])), ctx)

    assert _history(spark, database) == {
        ("A", "v1", _at(1), _at(2), "N", "N"),
        ("A", "v2", _at(2), None, "Y", "N"),
    }
    assert "NICKNAME" in spark.table(f"`{database}`.`TARGET`").columns


def test_scd2_stamps_the_version_it_closes_and_leaves_the_rest(spark, database):
    """An incremental reader keyed on the stamp must see a close, not only an insert."""
    ctx = _ctx(spark, database)
    writer = Scd2Writer()
    first = [("A", "v1", "2026-01-01 00:00:00"), ("B", "v1", "2026-01-01 00:00:00")]
    writer.write(_subjects(spark, first), replace(ctx, write_time=FIRST_RUN))

    second = [("A", "v2", "2026-01-02 00:00:00")]
    writer.write(_subjects(spark, second), replace(ctx, write_time=SECOND_RUN))

    assert _stamps(spark, database) == {
        ("A", "v1", "N", "N", SECOND_RUN),
        ("A", "v2", "Y", "N", SECOND_RUN),
        ("B", "v1", "Y", "N", FIRST_RUN),
    }


# --- COMPLETE_DELTA --------------------------------------------------------


def _file(day: int, name: str = "subjects") -> str:
    return f"FILE:/Volumes/in/{name}/{name}_2026010{day}120000.csv"


def _snapshot_subjects(spark, rows):
    return _hashed(
        spark.createDataFrame(
            [(*row[:3], _file(row[3]), _at(row[3], 12)) for row in rows],
            SNAPSHOT_SUBJECTS,
        )
    )


def _complete_delta_ctx(spark, database, output=None, **source_overrides):
    source = SourceConfig(
        origin=Origin.TABLE,
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
    _hashed(
        spark.createDataFrame([("B", "2026-01-02 00:00:00", _file(2), _at(2, 12))], DELETIONS)
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


def test_complete_delta_full_scope_stamps_the_rows_it_expires(spark, database):
    ctx = _complete_delta_ctx(spark, database, output={"snapshot_scope": SnapshotScope.FULL})
    writer = CompleteDeltaWriter()
    first = [("A", "v1", "2026-01-01 00:00:00", 1), ("C", "v1", "2026-01-01 00:00:00", 1)]
    writer.write(_snapshot_subjects(spark, first), replace(ctx, write_time=FIRST_RUN))

    second = [("A", "v2", "2026-01-02 00:00:00", 2)]
    writer.write(_snapshot_subjects(spark, second), replace(ctx, write_time=SECOND_RUN))

    assert _stamps(spark, database) == {
        ("A", "v1", "N", "N", SECOND_RUN),
        ("A", "v2", "Y", "N", SECOND_RUN),
        ("C", "v1", "N", "N", SECOND_RUN),
    }


def test_complete_delta_stamps_every_version_a_delete_flags(spark, database):
    """The flag marks B's closed v1 too, so that row changes and is stamped as well."""
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
    deletions = f"`{database}`.`DELETIONS`"
    _hashed(spark.createDataFrame([], DELETIONS)).write.format("delta").saveAsTable(deletions)
    writer = CompleteDeltaWriter()
    updates = [
        ("A", "v1", "2026-01-01 00:00:00", 1),
        ("B", "v1", "2026-01-01 00:00:00", 1),
        ("B", "v2", "2026-01-02 00:00:00", 2),
    ]
    writer.write(_snapshot_subjects(spark, updates), replace(ctx, write_time=FIRST_RUN))

    _hashed(
        spark.createDataFrame([("B", "2026-01-03 00:00:00", _file(3), _at(3, 12))], DELETIONS)
    ).write.format("delta").mode("append").saveAsTable(deletions)
    writer.write(_snapshot_subjects(spark, []), replace(ctx, write_time=SECOND_RUN))

    assert _stamps(spark, database) == {
        ("A", "v1", "Y", "N", FIRST_RUN),
        ("B", "v1", "N", "Y", SECOND_RUN),
        ("B", "v2", "N", "Y", SECOND_RUN),
    }


def test_complete_delta_full_scope_refuses_a_new_column_before_expiring_anything(spark, database):
    """Refused after the expiry, the insert would leave the whole table with no current row."""
    ctx = _complete_delta_ctx(spark, database, output={"snapshot_scope": SnapshotScope.FULL})
    writer = CompleteDeltaWriter()
    writer.write(_snapshot_subjects(spark, [("A", "v1", "2026-01-01 00:00:00", 1)]), ctx)

    wider = _with_nickname(_snapshot_subjects(spark, [("A", "v2", "2026-01-02 00:00:00", 2)]))
    with pytest.raises(PlatformPolicyViolation, match="NICKNAME"):
        writer.write(wider, ctx)

    assert _history(spark, database) == {("A", "v1", _at(1), None, "Y", "N")}


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
    updates = _hashed(
        spark.createDataFrame(
            [
                (
                    "A",
                    "v1",
                    "2026-01-01 00:00:00",
                    "/in/subjects_2026-01-01T12:00:00.csv",
                    _at(1, 12),
                ),
                ("A", "v2", "2026-01-02 00:00:00", "/in/no_stamp_at_all.csv", _at(2, 12)),
            ],
            SNAPSHOT_SUBJECTS,
        )
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


# --- event_time_invalid (CODE_REVIEW finding 2) ----------------------------


def test_scd2_refuses_a_batch_with_a_null_event_time(spark, database):
    """Unchecked, a NULL falls through the anti-filter and the close: a second current row."""
    ctx = _ctx(spark, database)
    Scd2Writer().write(_subjects(spark, [("A", "v1", "2026-01-01 00:00:00")]), ctx)

    with pytest.raises(PlatformPolicyViolation, match=r"UPDATEDTIME: 1 row\(s\) missing"):
        _gate_and_write(_subjects(spark, [("A", "v2", None)]), ctx, Scd2Writer())

    assert _history(spark, database) == {("A", "v1", _at(1), None, "Y", "N")}


def test_upsert_refuses_a_batch_whose_event_time_does_not_parse(spark, database):
    """Unchecked, newer-wins compares against NULL and silently keeps the old row."""
    ctx = _ctx(spark, database, verb=Verb.UPSERT)
    UpsertWriter().write(_subjects(spark, [("A", "v1", "2026-01-01 00:00:00")]), ctx)

    with pytest.raises(PlatformPolicyViolation, match=r"don't parse .* \(e\.g\. soon\)"):
        _gate_and_write(_subjects(spark, [("A", "v2", "soon")]), ctx, UpsertWriter())

    rows = spark.table(f"`{database}`.`TARGET`").collect()
    assert {(row["ID"], row["PAYLOAD"]) for row in rows} == {("A", "v1")}


def test_complete_delta_refuses_a_backlog_with_a_null_event_time(spark, database):
    """Checked across the whole backlog, so no snapshot is replayed before it fails."""
    ctx = _complete_delta_ctx(spark, database)
    updates = _snapshot_subjects(
        spark,
        [("A", "v1", "2026-01-01 00:00:00", 1), ("A", "v2", None, 2)],
    )

    with pytest.raises(PlatformPolicyViolation, match="UPDATEDTIME"):
        _gate_and_write(updates, ctx, CompleteDeltaWriter())

    assert not spark.catalog.tableExists(f"`{database}`.`TARGET`")


def test_complete_delta_refuses_a_deletes_feed_with_a_null_delete_time(spark, database):
    """The deletes feed skips the pipeline, so the writer checks it before replaying."""
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
    _hashed(spark.createDataFrame([("B", None, _file(2), _at(2, 12))], DELETIONS)).write.format(
        "delta"
    ).saveAsTable(f"`{database}`.`DELETIONS`")
    updates = _snapshot_subjects(spark, [("B", "v1", "2026-01-01 00:00:00", 1)])

    with pytest.raises(
        PlatformPolicyViolation, match=r"output.deletes.event_time.column=DELETEDTIME"
    ):
        CompleteDeltaWriter().write(updates, ctx)

    assert not spark.catalog.tableExists(f"`{database}`.`TARGET`")


# --- dedup and the gate per snapshot (gh #2) ---------------------------------

UNIQUE_ID = [
    {
        "name": "id_is_unique",
        "criticality": "error",
        "check": {"function": "is_unique", "arguments": {"columns": ["ID"]}},
    }
]


def _passed(ctx) -> list[str]:
    infos = ctx.logger.info.call_args_list
    return [c.kwargs["description"] for c in infos if c.kwargs["name"] == "policies_passed"]


def test_complete_delta_dedups_each_snapshot_on_its_own(spark, database):
    """Deduplicating the backlog as a whole would collapse A to v2 and lose v1."""
    ctx = _complete_delta_ctx(spark, database)
    updates = _snapshot_subjects(
        spark,
        [
            ("A", "v0", "2026-01-01 00:00:00", 1),
            ("A", "v1", "2026-01-01 06:00:00", 1),
            ("A", "v2", "2026-01-02 00:00:00", 2),
        ],
    )

    _gate_and_write(updates, ctx, CompleteDeltaWriter())

    assert _history(spark, database) == {
        ("A", "v1", _at(1, 6), _at(2), "N", "N"),
        ("A", "v2", _at(2), None, "Y", "N"),
    }


def test_complete_delta_gates_each_snapshot_so_a_resent_key_passes(spark, database):
    """Every snapshot re-sends A; judged as a backlog, is_unique(ID) would always fail."""
    ctx = replace(_complete_delta_ctx(spark, database), checks=UNIQUE_ID)
    updates = _snapshot_subjects(
        spark,
        [("A", "v1", "2026-01-01 00:00:00", 1), ("A", "v2", "2026-01-02 00:00:00", 2)],
    )

    _gate_and_write(updates, ctx, CompleteDeltaWriter())

    assert len(_history(spark, database)) == 2
    passed = _passed(ctx)
    assert len(passed) == 2
    assert passed[0].endswith(f"(snapshot {_at(1, 12)})")
    assert passed[1].endswith(f"(snapshot {_at(2, 12)})")


def test_complete_delta_writes_nothing_when_a_later_snapshot_fails_its_gate(spark, database):
    """Every snapshot is judged before the first merge, so none is half-replayed."""
    ctx = replace(
        _complete_delta_ctx(spark, database, output={"dedup": DedupConfig(enabled=False)}),
        checks=UNIQUE_ID,
    )
    updates = _snapshot_subjects(
        spark,
        [
            ("A", "v1", "2026-01-01 00:00:00", 1),
            ("A", "v2", "2026-01-02 00:00:00", 2),
            ("A", "v3", "2026-01-02 06:00:00", 2),
        ],
    )

    with pytest.raises(PolicyViolation):
        _gate_and_write(updates, ctx, CompleteDeltaWriter())

    assert not spark.catalog.tableExists(f"`{database}`.`TARGET`")
    assert ctx.logger.error.call_args.kwargs["description"].endswith(f"(snapshot {_at(2, 12)})")


def test_complete_delta_full_scope_does_not_judge_superseded_exports(spark, database):
    """Only the newest export is replayed, so the older ones' rows never reach the gate."""
    output = {"snapshot_scope": SnapshotScope.FULL, "dedup": DedupConfig(enabled=False)}
    ctx = replace(_complete_delta_ctx(spark, database, output=output), checks=UNIQUE_ID)
    updates = _snapshot_subjects(
        spark,
        [
            ("A", "v1", "2026-01-01 00:00:00", 1),
            ("A", "v1", "2026-01-01 00:00:00", 1),
            ("A", "v2", "2026-01-02 00:00:00", 2),
        ],
    )

    _gate_and_write(updates, ctx, CompleteDeltaWriter())

    assert _history(spark, database) == {("A", "v2", _at(2), None, "Y", "N")}
    assert len(_passed(ctx)) == 1


# --- change detection by __RECORD_HASH --------------------------------------


def test_scd2_opens_no_version_for_an_unchanged_record_whatever_its_event_time(spark, database):
    """gh #51: a full export with no change date moves only __EXPORT_DATE, never content."""
    ctx = _ctx(spark, database, event_time=EventTimeConfig(column="__EXPORT_DATE"))
    writer = Scd2Writer()
    unchanged = ("A", "v1", "2026-01-01 00:00:00")

    writer.write(_snapshot_subjects(spark, [(*unchanged, 1)]), replace(ctx, write_time=FIRST_RUN))
    writer.write(_snapshot_subjects(spark, [(*unchanged, 2)]), replace(ctx, write_time=SECOND_RUN))

    assert _stamps(spark, database) == {("A", "v1", "Y", "N", FIRST_RUN)}


def test_scd2_versions_a_changed_record_at_its_export(spark, database):
    ctx = _ctx(spark, database, event_time=EventTimeConfig(column="__EXPORT_DATE"))
    writer = Scd2Writer()

    writer.write(_snapshot_subjects(spark, [("A", "v1", "2026-01-01 00:00:00", 1)]), ctx)
    writer.write(_snapshot_subjects(spark, [("A", "v2", "2026-01-01 00:00:00", 2)]), ctx)

    assert _history(spark, database) == {
        ("A", "v1", _at(1, 12), _at(2, 12), "N", "N"),
        ("A", "v2", _at(2, 12), None, "Y", "N"),
    }


def test_complete_delta_full_scope_leaves_an_unchanged_record_untouched(spark, database):
    """Only what the snapshot dropped or changed closes; A is re-asserted as it was."""
    ctx = _complete_delta_ctx(spark, database, output={"snapshot_scope": SnapshotScope.FULL})
    writer = CompleteDeltaWriter()
    first = [
        ("A", "v1", "2026-01-01 00:00:00", 1),
        ("B", "v1", "2026-01-01 00:00:00", 1),
        ("C", "v1", "2026-01-01 00:00:00", 1),
    ]
    writer.write(_snapshot_subjects(spark, first), replace(ctx, write_time=FIRST_RUN))

    second = [("A", "v1", "2026-01-01 00:00:00", 2), ("B", "v2", "2026-01-02 00:00:00", 2)]
    writer.write(_snapshot_subjects(spark, second), replace(ctx, write_time=SECOND_RUN))

    assert _history(spark, database) == {
        ("A", "v1", _at(1), None, "Y", "N"),
        ("B", "v1", _at(1), _at(2, 12), "N", "N"),
        ("B", "v2", _at(2), None, "Y", "N"),
        ("C", "v1", _at(1), _at(2, 12), "N", "N"),
    }
    assert ("A", "v1", "Y", "N", FIRST_RUN) in _stamps(spark, database)


def test_complete_delta_full_scope_expires_a_changed_record_whatever_its_event_time(
    spark, database
):
    """The snapshot asserts each record's state: A changed, though its date didn't move."""
    ctx = _complete_delta_ctx(spark, database, output={"snapshot_scope": SnapshotScope.FULL})
    writer = CompleteDeltaWriter()

    writer.write(_snapshot_subjects(spark, [("A", "v1", "2026-01-01 00:00:00", 1)]), ctx)
    writer.write(_snapshot_subjects(spark, [("A", "v2", "2026-01-01 00:00:00", 2)]), ctx)

    assert _history(spark, database) == {
        ("A", "v1", _at(1), _at(2, 12), "N", "N"),
        ("A", "v2", _at(1), None, "Y", "N"),
    }


def test_complete_delta_full_scope_reopens_a_record_that_returns_unchanged(spark, database):
    """Compared with the current version only: B's closed v1 is history, not its state."""
    ctx = _complete_delta_ctx(spark, database, output={"snapshot_scope": SnapshotScope.FULL})
    writer = CompleteDeltaWriter()
    a, b = ("A", "v1", "2026-01-01 00:00:00"), ("B", "v1", "2026-01-01 00:00:00")

    writer.write(_snapshot_subjects(spark, [(*a, 1), (*b, 1)]), ctx)
    writer.write(_snapshot_subjects(spark, [(*a, 2)]), ctx)
    writer.write(_snapshot_subjects(spark, [(*a, 3), (*b, 3)]), ctx)

    assert _history(spark, database) == {
        ("A", "v1", _at(1), None, "Y", "N"),
        ("B", "v1", _at(1), _at(2, 12), "N", "N"),
        ("B", "v1", _at(1), None, "Y", "N"),
    }


def test_promotion_keeps_the_hash_a_record_entered_with(spark, database):
    ctx = _ctx(spark, database)
    batch = _subjects(spark, [("A", "v1", "2026-01-01 00:00:00")])
    entered = batch.collect()[0]["__RECORD_HASH"]

    Scd2Writer().write(batch, ctx)

    stored = spark.table(f"`{database}`.`TARGET`").collect()[0]
    assert stored["__RECORD_HASH"] == entered
