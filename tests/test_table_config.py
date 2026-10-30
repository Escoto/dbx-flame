"""Tests for output.table_config — the configuration kept on every Delta table."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dbx_flame.context.table_config import TableConfig
from dbx_flame.output.mechanics import EXPORT_DATE
from dbx_flame.output.table_config import STATS_COLUMNS, DeltaTableConfig, stats_columns
from dbx_flame.output.table_metadata import FRAMEWORK_COMMENTS


def _schema(*names: str) -> StructType:
    return StructType([StructField(name, StringType()) for name in names])


def test_a_narrow_table_lists_every_column():
    """What Delta's default would index anyway, now written down."""
    schema = _schema("ID", "NAME", "__EXPORT_DATE")

    assert stats_columns(schema) == "ID,NAME,__EXPORT_DATE"


def test_a_wide_table_keeps_the_first_32_and_adds_the_framework_columns():
    business = [f"C{n}" for n in range(40)]
    schema = _schema(*business, "__EXPORT_DATE", "__ANCHOR_DT")

    assert stats_columns(schema).split(",") == business[:32] + ["__EXPORT_DATE", "__ANCHOR_DT"]


def test_a_framework_column_already_in_the_first_32_is_not_repeated():
    schema = _schema("__EXPORT_DATE", *[f"C{n}" for n in range(40)])

    listed = stats_columns(schema).split(",")

    assert len(listed) == 32
    assert listed.count("__EXPORT_DATE") == 1


def test_structs_are_listed_by_leaf_and_arrays_and_maps_are_left_out():
    """Delta rejects arrays and maps in the list; struct fields count one by one."""
    schema = StructType(
        [
            StructField("ID", IntegerType()),
            StructField("TAGS", ArrayType(StringType())),
            StructField("ATTRS", MapType(StringType(), StringType())),
            StructField("ADDRESS", StructType([StructField("CITY", StringType())])),
            StructField("__EXPORT_DATE", TimestampType()),
        ]
    )

    assert stats_columns(schema) == "ID,ADDRESS.CITY,__EXPORT_DATE"


@pytest.fixture
def table(spark):
    database = f"test_table_config_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE `{database}`")
    yield f"`{database}`.`TARGET`"
    spark.sql(f"DROP DATABASE IF EXISTS `{database}` CASCADE")


def _ctx(spark, table, table_config: TableConfig | None = None) -> MagicMock:
    ctx = MagicMock()
    ctx.spark = spark
    ctx.target_table = table
    ctx.table_config = table_config
    return ctx


def _property(spark, table) -> str | None:
    rows = spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
    return {row.key: row.value for row in rows}.get(STATS_COLUMNS)


def _comment(spark, table, column: str) -> str | None:
    field = next(f for f in spark.table(table).schema.fields if f.name == column)
    return field.metadata.get("comment")


def test_the_list_is_set_when_the_write_creates_the_table(spark, table):
    ctx = _ctx(spark, table)
    df = spark.createDataFrame([("1", "a")], "ID string, __EXPORT_DATE string")

    df.write.format("delta").options(**DeltaTableConfig(ctx).write_options(df)).saveAsTable(table)

    assert _property(spark, table) == "ID,__EXPORT_DATE"


def test_apply_follows_a_column_schema_evolution_added(spark, table):
    """An explicit list doesn't pick up a new column the way Delta's default would."""
    ctx = _ctx(spark, table)
    spark.createDataFrame([("1",)], "ID string").write.format("delta").saveAsTable(table)
    DeltaTableConfig(ctx).apply()

    evolved = spark.createDataFrame([("2", "x")], "ID string, REGION string")
    evolved.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)
    DeltaTableConfig(ctx).apply()

    assert _property(spark, table) == "ID,REGION"
    assert ctx.logger.info.call_args.kwargs["name"] == "table_configured"


def test_apply_leaves_a_table_that_is_already_current_alone(spark, table):
    ctx = _ctx(spark, table)
    df = spark.createDataFrame([("1",)], "ID string")
    df.write.format("delta").options(**DeltaTableConfig(ctx).write_options(df)).saveAsTable(table)

    DeltaTableConfig(ctx).apply()

    ctx.logger.info.assert_not_called()


def test_apply_does_nothing_before_the_table_exists(spark, table):
    ctx = _ctx(spark, table)

    DeltaTableConfig(ctx).apply()

    ctx.logger.info.assert_not_called()
    ctx.logger.warning.assert_not_called()


def test_a_table_config_that_cannot_be_applied_yet_is_warned_about(spark, table):
    """Not skipped silently: the file names a table that isn't there."""
    ctx = _ctx(spark, table, TableConfig(comment="Agents."))

    DeltaTableConfig(ctx).apply()

    assert ctx.logger.warning.call_args.kwargs["name"] == "table_metadata_skipped"


def test_apply_also_applies_the_comments_and_tags(spark, table, monkeypatch):
    applied = MagicMock()
    monkeypatch.setattr("dbx_flame.output.table_config.apply_metadata", applied)
    ctx = _ctx(spark, table)
    spark.createDataFrame([("1",)], "ID string").write.format("delta").saveAsTable(table)

    DeltaTableConfig(ctx).apply()

    applied.assert_called_once_with(ctx, spark.table(table).schema)


def test_a_framework_column_is_commented_once_on_a_real_table(spark, table):
    ctx = _ctx(spark, table)
    df = spark.createDataFrame([("1", "x")], f"ID string, {EXPORT_DATE} string")
    df.write.format("delta").options(**DeltaTableConfig(ctx).write_options(df)).saveAsTable(table)

    DeltaTableConfig(ctx).apply()
    assert _comment(spark, table, EXPORT_DATE) == FRAMEWORK_COMMENTS[EXPORT_DATE]

    ctx.logger.reset_mock()
    DeltaTableConfig(ctx).apply()
    ctx.logger.info.assert_not_called()


def test_a_column_comment_reaches_a_real_table_as_written(spark, table):
    """An apostrophe and a backslash survive the trip through the ALTER statement."""
    text = "The site's code, e.g. A\\B."
    ctx = _ctx(spark, table, TableConfig(columns=[{"name": "ID", "comment": text}]))
    spark.createDataFrame([("1",)], "ID string").write.format("delta").saveAsTable(table)

    DeltaTableConfig(ctx).apply()

    assert _comment(spark, table, "ID") == text


def test_a_column_schema_evolution_adds_gets_its_comment_on_the_same_run(spark, table):
    ctx = _ctx(spark, table, TableConfig(columns=[{"name": "REGION", "comment": "Region."}]))
    spark.createDataFrame([("1",)], "ID string").write.format("delta").saveAsTable(table)

    evolved = spark.createDataFrame([("2", "x")], "ID string, REGION string")
    evolved.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)
    DeltaTableConfig(ctx).apply()

    assert _comment(spark, table, "REGION") == "Region."
