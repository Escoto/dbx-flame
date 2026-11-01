"""Tests for pipelines.enrichment — provenance, sanitization, rename patterns."""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from pyspark.sql import functions as F

from dbx_flame.context.config import EventTimeConfig, Origin, SnapshotTimePattern
from dbx_flame.pipelines.enrichment import (
    _export_date,
    add_provenance,
    apply_rename_patterns,
    reject_unstamped,
    sanitize_column_names,
    stamp_anchor,
    stamp_record_hash,
)
from dbx_flame.policies.platform import PlatformPolicyViolation


@pytest.fixture
def ctx() -> MagicMock:
    """Only ctx.logger and the source's pattern are touched by provenance."""
    mock = MagicMock()
    mock.config.source.snapshot_time_pattern = None
    return mock


def _read_csv(spark, tmp_path, filename: str):
    path = tmp_path / filename
    path.write_text("id,name\n1,alice\n", encoding="utf-8")
    return spark.read.option("header", "true").csv(str(path))


def test_provenance_columns_added(spark, tmp_path, ctx):
    df = _read_csv(spark, tmp_path, "AGENTS_20240115103000.csv")

    row = add_provenance(df, ctx).collect()[0]

    assert row["__SOURCE"].endswith("AGENTS_20240115103000.csv")
    assert row["__EXPORT_DATE"] == datetime(2024, 1, 15, 10, 30, 0)


def test_source_is_the_file_path_unity_catalog_reports_prefixed_by_file(spark, tmp_path, ctx):
    """The prefix says how the record was read; the path is kept exactly, never normalized."""
    df = _read_csv(spark, tmp_path, "AGENTS_20240115103000.csv")
    reported = df.select("_metadata.file_path").first()[0]

    row = add_provenance(df, ctx).collect()[0]

    assert row["__SOURCE"] == f"FILE:{reported}"


@pytest.mark.parametrize(
    ("name", "pattern", "expected"),
    [
        (
            "AGENTS_20240115103000Z.csv",
            SnapshotTimePattern.DATETIME,
            datetime(2024, 1, 15, 10, 30),
        ),
        ("AGENTS_2024-01-15T10:30:00.csv", SnapshotTimePattern.ISO, datetime(2024, 1, 15, 10, 30)),
        # The agreed pattern is the only one read: the other shape is no stamp at all.
        ("AGENTS_2024-01-15T10:30:00.csv", SnapshotTimePattern.DATETIME, None),
        ("AGENTS_20240115103000.csv", SnapshotTimePattern.ISO, None),
        # A longer run of digits is an ID, not a stamp.
        ("AGENTS_2024011510300099.csv", SnapshotTimePattern.DATETIME, None),
        ("AGENTS_20241315103000.csv", SnapshotTimePattern.DATETIME, None),  # month 13
    ],
)
def test_export_date_reads_only_the_agreed_pattern(spark, name, pattern, expected):
    df = spark.createDataFrame([(f"/Volumes/in/agents/{name}",)], "path string")

    assert df.select(_export_date(F.col("path"), pattern)).first()[0] == expected


def test_export_date_ignores_a_stamp_in_the_folder_names(spark):
    df = spark.createDataFrame(
        [
            ("/Volumes/in/20240101000000/AGENTS_20240115103000.csv",),
            ("/Volumes/in/20240101000000/AGENTS.csv",),
        ],
        "path string",
    )

    stamps = [
        row[0]
        for row in df.select(_export_date(F.col("path"), SnapshotTimePattern.DATETIME)).collect()
    ]

    assert stamps == [datetime(2024, 1, 15, 10, 30), None]


def test_an_unstamped_file_is_refused_before_anything_is_written(spark, tmp_path, ctx):
    df = add_provenance(_read_csv(spark, tmp_path, "AGENTS.csv"), ctx)

    with pytest.raises(
        PlatformPolicyViolation, match=r"unstamped_file: FILE:.*AGENTS\.csv doesn't carry"
    ):
        reject_unstamped(df, ctx)
    assert ctx.logger.error.call_args.kwargs["name"] == "unstamped_file"


def test_stamped_files_pass(spark, tmp_path, ctx):
    df = add_provenance(_read_csv(spark, tmp_path, "AGENTS_20240115103000.csv"), ctx)

    reject_unstamped(df, ctx)

    ctx.logger.error.assert_not_called()


def test_provenance_keeps_the_source_columns(spark, tmp_path, ctx):
    df = _read_csv(spark, tmp_path, "AGENTS_20240115103000.csv")

    result = add_provenance(df, ctx)

    assert result.columns[:2] == ["id", "name"]
    assert result.collect()[0]["name"] == "alice"


def test_sanitize_replaces_unsafe_chars_and_uppercases(spark, ctx):
    df = spark.createDataFrame([(1, 2, 3, 4, 5)]).toDF("order id", "a,b", "c;d", "e.f", "g/h")

    result = sanitize_column_names(df, ctx)

    assert result.columns == ["ORDER_ID", "A_B", "C_D", "E_F", "G_H"]


def test_sanitize_replaces_every_delta_invalid_char(spark, ctx):
    names = ["Weight (kg)", "a{b}", "k=v", "tab\there", "new\nline"]
    df = spark.createDataFrame([(1, 2, 3, 4, 5)]).toDF(*names)

    result = sanitize_column_names(df, ctx)

    assert result.columns == ["WEIGHT__KG_", "A_B_", "K_V", "TAB_HERE", "NEW_LINE"]


def test_sanitize_preserves_values(spark, ctx):
    df = spark.createDataFrame([("1", "alice")]).toDF("agent id", "name")

    row = sanitize_column_names(df, ctx).collect()[0]

    assert row["AGENT_ID"] == "1"
    assert row["NAME"] == "alice"


def test_sanitize_drops_rescued_data(spark, ctx):
    df = spark.createDataFrame([(1, "x")]).toDF("id", "_rescued_data")

    assert sanitize_column_names(df, ctx).columns == ["ID"]


def test_sanitize_logs_only_the_columns_it_changed(spark, ctx):
    df = spark.createDataFrame([(1, 2)]).toDF("order id", "NAME")

    sanitize_column_names(df, ctx)

    logged = ctx.logger.info.call_args
    assert logged.kwargs["name"] == "columns_sanitized"
    assert logged.kwargs["total"] == 1
    assert json.loads(logged.kwargs["metadata"]) == {"order id": "ORDER_ID"}


def test_sanitize_logs_nothing_when_names_are_already_clean(spark, ctx):
    """Silence in the rename log must mean 'no renames', not 'not logged'."""
    df = spark.createDataFrame([(1, 2)]).toDF("ID", "NAME")

    result = sanitize_column_names(df, ctx)

    assert result.columns == ["ID", "NAME"]
    ctx.logger.info.assert_not_called()


def test_provenance_survives_sanitization_as_the_metadata_contract(spark, tmp_path, ctx):
    df = _read_csv(spark, tmp_path, "AGENTS_20240115103000.csv")

    result = sanitize_column_names(add_provenance(df, ctx), ctx)

    # No write time: that is each table's own __LAST_MODIFIED_DT, set by the writer.
    assert [c for c in result.columns if c.startswith("__")] == ["__SOURCE", "__EXPORT_DATE"]


def test_rename_patterns_strip_a_suffix(spark):
    df = spark.createDataFrame([(1, 2)]).toDF("AGENT__V", "NAME")

    assert apply_rename_patterns(df, ["__[Vv]$="]).columns == ["AGENT", "NAME"]


def test_rename_patterns_are_applied_in_order(spark):
    df = spark.createDataFrame([(1,)]).toDF("PREFIX_AGENT__V")

    result = apply_rename_patterns(df, ["__[Vv]$=", "^PREFIX_="])

    assert result.columns == ["AGENT"]


def test_rename_patterns_can_substitute_not_only_delete(spark):
    df = spark.createDataFrame([(1,)]).toDF("AGENT_OLD")

    assert apply_rename_patterns(df, ["_OLD$=_CURRENT"]).columns == ["AGENT_CURRENT"]


def test_no_patterns_returns_the_input_untouched(spark):
    df = spark.createDataFrame([(1,)]).toDF("AGENT")

    assert apply_rename_patterns(df, []) is df


def test_rename_patterns_never_rename_a_framework_column(spark):
    """Provenance and the hash carry find these by name; a broad pattern must not move them."""
    df = spark.createDataFrame([(1, "f", "h")]).toDF("AGENT_ID", "__SOURCE", "__RECORD_HASH")

    result = apply_rename_patterns(df, ["_="])

    assert result.columns == ["AGENTID", "__SOURCE", "__RECORD_HASH"]


# --- __ANCHOR_DT ----------------------------------------------------------------


def _anchored(ctx, column="MODIFIED", format=None, origin=Origin.CSV):
    ctx.config.source.origin = origin
    ctx.config.source.anchor_dt = EventTimeConfig(column=column, format=format)
    return ctx


def test_no_anchor_configured_leaves_the_batch_alone(spark, ctx):
    ctx.config.source.anchor_dt = None
    df = spark.createDataFrame([("1", "2024-01-01")], "ID string, MODIFIED string")

    assert stamp_anchor(df, ctx).columns == ["ID", "MODIFIED"]


def test_a_table_origin_is_left_to_the_delta_read_that_stamped_it(spark, ctx):
    """By now sanitization may have renamed the source column the read followed."""
    df = spark.createDataFrame([("1", datetime(2024, 1, 15))], "ID string, MODIFIED timestamp")

    assert stamp_anchor(df, _anchored(ctx, origin=Origin.TABLE)).columns == ["ID", "MODIFIED"]


def test_a_string_anchor_is_parsed_with_its_format(spark, ctx):
    """'15/01/2024' sorts after '01/03/2024' as text; as a timestamp it comes first."""
    df = spark.createDataFrame(
        [("1", "15/01/2024"), ("2", "01/03/2024")], "ID string, MODIFIED string"
    )

    rows = stamp_anchor(df, _anchored(ctx, format="dd/MM/yyyy")).orderBy("__ANCHOR_DT").collect()

    assert [row["ID"] for row in rows] == ["1", "2"]
    assert rows[0]["__ANCHOR_DT"] == datetime(2024, 1, 15)
    assert rows[0]["MODIFIED"] == "15/01/2024"  # the original is left untouched


def test_a_typed_anchor_is_copied(spark, ctx):
    df = spark.createDataFrame([("1", datetime(2024, 1, 15, 9))], "ID string, MODIFIED timestamp")

    row = stamp_anchor(df, _anchored(ctx)).collect()[0]

    assert row["__ANCHOR_DT"] == datetime(2024, 1, 15, 9)


def test_an_unparseable_anchor_fails_the_batch(spark, ctx):
    df = spark.createDataFrame([("1", "2024-01-15"), ("2", "soon")], "ID string, MODIFIED string")

    with pytest.raises(PlatformPolicyViolation, match="e.g. soon"):
        stamp_anchor(df, _anchored(ctx, format="yyyy-MM-dd"))
    assert ctx.logger.error.call_args.kwargs["name"] == "anchor_unparseable"


def test_a_missing_anchor_is_logged_not_failed(spark, ctx):
    """Blank counts as missing, not unparseable, as it does for casts."""
    df = spark.createDataFrame(
        [("1", "2024-01-15"), ("2", None), ("3", " ")], "ID string, MODIFIED string"
    )

    result = stamp_anchor(df, _anchored(ctx, format="yyyy-MM-dd"))

    assert result.filter("__ANCHOR_DT IS NULL").count() == 2
    logged = ctx.logger.warning.call_args.kwargs
    assert logged["name"] == "anchor_missing"
    assert logged["total"] == 2


def test_an_anchor_column_not_in_the_batch_fails(spark, ctx):
    df = spark.createDataFrame([("1",)], "ID string")

    with pytest.raises(PlatformPolicyViolation, match="MODIFIED is not in the batch"):
        stamp_anchor(df, _anchored(ctx))


# --- __RECORD_HASH --------------------------------------------------------------


def _from(origin, ctx):
    ctx.config.source.origin = origin
    return ctx


def _hashes(spark, ctx, rows, schema):
    df = spark.createDataFrame(rows, schema)
    return [
        row["__RECORD_HASH"] for row in stamp_record_hash(df, _from(Origin.CSV, ctx)).collect()
    ]


def test_the_hash_is_sha256_hex(spark, ctx):
    (found,) = _hashes(spark, ctx, [("1", "alice")], "ID string, NAME string")

    assert len(found) == 64 and int(found, 16) >= 0


def test_the_same_content_gives_the_same_hash_whatever_the_column_order(spark, ctx):
    one = _hashes(spark, ctx, [("1", "alice")], "ID string, NAME string")
    other = _hashes(spark, ctx, [("alice", "1")], "NAME string, ID string")

    assert one == other


@pytest.mark.parametrize("changed", [("2", "alice", "x"), ("1", "bob", "x"), ("1", "alice", "y")])
def test_a_change_in_any_source_column_changes_the_hash(spark, ctx, changed):
    schema = "ID string, NAME string, NOTE string"

    hashes = _hashes(spark, ctx, [("1", "alice", "x"), changed], schema)

    assert hashes[0] != hashes[1]


def test_a_null_and_an_empty_string_hash_differently(spark, ctx):
    hashes = _hashes(spark, ctx, [("1", None), ("1", "")], "ID string, NOTE string")

    assert hashes[0] != hashes[1]


def test_a_value_cannot_shift_into_its_neighbour(spark, ctx):
    """Without a length prefix both rows would join to the same text, "abc"."""
    hashes = _hashes(spark, ctx, [("ab", "c"), ("a", "bc")], "A string, B string")

    assert hashes[0] != hashes[1]


def test_column_names_are_not_hashed(spark, ctx):
    one = _hashes(spark, ctx, [("1", "alice")], "A string, B string")
    other = _hashes(spark, ctx, [("1", "alice")], "X string, Y string")

    assert one == other


def test_framework_columns_are_left_out_of_the_hash(spark, ctx):
    """The same record in two exports carries a new __EXPORT_DATE, not new content."""
    schema = "ID string, __SOURCE string, __EXPORT_DATE timestamp"
    rows = [
        ("1", "FILE:/a_1.csv", datetime(2026, 1, 1)),
        ("1", "FILE:/a_2.csv", datetime(2026, 1, 2)),
    ]

    hashes = _hashes(spark, ctx, rows, schema)

    assert hashes[0] == hashes[1]


def test_a_widened_integer_keeps_its_hash(spark, ctx):
    narrow = _hashes(spark, ctx, [(1,)], "ID int")
    wide = _hashes(spark, ctx, [(1,)], "ID bigint")

    assert narrow == wide


def test_a_timestamp_hashes_the_same_in_every_session_time_zone(spark, ctx):
    zone = "spark.sql.session.timeZone"
    previous = spark.conf.get(zone)
    rows, schema = [(datetime(2026, 1, 5, 10, 30),)], "AT timestamp"
    try:
        spark.conf.set(zone, "UTC")
        in_utc = _hashes(spark, ctx, rows, schema)
        spark.conf.set(zone, "America/New_York")
        in_new_york = _hashes(spark, ctx, rows, schema)
    finally:
        spark.conf.set(zone, previous)

    assert in_utc == in_new_york


def test_a_field_added_to_a_struct_changes_the_hash_even_when_null(spark, ctx):
    """A new schema is a new version of every record, nested fields included."""
    before = _hashes(spark, ctx, [("1", (1,))], "ID string, S struct<A: int>")
    after = _hashes(spark, ctx, [("1", (1, None))], "ID string, S struct<A: int, B: int>")

    assert before != after


def test_a_table_we_stamped_keeps_the_hash_it_arrived_with(spark, ctx):
    df = spark.createDataFrame([("1", "upstream")], "ID string, __RECORD_HASH string")

    result = stamp_record_hash(df, _from(Origin.TABLE, ctx))

    assert result.collect()[0]["__RECORD_HASH"] == "upstream"


def test_a_table_we_did_not_create_is_hashed_where_it_is_read(spark, ctx):
    df = spark.createDataFrame([("1",)], "ID string")

    result = stamp_record_hash(df, _from(Origin.TABLE, ctx))

    assert len(result.collect()[0]["__RECORD_HASH"]) == 64


def test_a_file_is_always_hashed_here(spark, ctx):
    """A column of that name in a file is the vendor's, not a hash we stamped."""
    df = spark.createDataFrame([("1", "forged")], "ID string, __RECORD_HASH string")

    result = stamp_record_hash(df, _from(Origin.CSV, ctx))

    assert result.collect()[0]["__RECORD_HASH"] != "forged"
