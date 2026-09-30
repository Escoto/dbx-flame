"""Tests for pipelines.enrichment — provenance, sanitization, rename patterns."""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from pyspark.sql import functions as F

from dbx_flame.context.config import EventTimeConfig, SnapshotTimePattern
from dbx_flame.pipelines.enrichment import (
    AnchorException,
    UnstampedFileError,
    _export_date,
    add_provenance,
    apply_rename_patterns,
    reject_unstamped,
    sanitize_column_names,
    stamp_anchor,
)


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

    assert row["__filePath"].endswith("AGENTS_20240115103000.csv")
    assert row["__EXPORT_DATE"] == datetime(2024, 1, 15, 10, 30, 0)
    assert row["__bronze_last_modified_dt"] is not None


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


def test_an_unstamped_file_is_refused_before_anything_is_written(spark, tmp_path, ctx):
    df = add_provenance(_read_csv(spark, tmp_path, "AGENTS.csv"), ctx)

    with pytest.raises(UnstampedFileError, match="AGENTS.csv doesn't carry"):
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

    assert "__FILEPATH" in result.columns
    assert "__BRONZE_LAST_MODIFIED_DT" in result.columns
    assert "__EXPORT_DATE" in result.columns


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


# --- __ANCHOR_DT ----------------------------------------------------------------


def _anchored(ctx, column="MODIFIED", format=None):
    ctx.config.source.anchor_dt = EventTimeConfig(column=column, format=format)
    return ctx


def test_no_anchor_configured_leaves_the_batch_alone(spark, ctx):
    ctx.config.source.anchor_dt = None
    df = spark.createDataFrame([("1", "2024-01-01")], "ID string, MODIFIED string")

    assert stamp_anchor(df, ctx).columns == ["ID", "MODIFIED"]


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

    with pytest.raises(AnchorException, match="e.g. soon"):
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

    with pytest.raises(AnchorException, match="MODIFIED is not in the batch"):
        stamp_anchor(df, _anchored(ctx))
