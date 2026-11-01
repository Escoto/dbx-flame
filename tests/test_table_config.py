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
from dbx_flame.output.base import Layout
from dbx_flame.output.mechanics import EXPORT_DATE
from dbx_flame.output.table_config import (
    STATS_COLUMNS,
    DeltaTableConfig,
    clustering_columns,
    stats_columns,
)
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


def test_a_clustering_column_past_the_first_32_keeps_its_statistics():
    """Delta refuses to cluster by a column it keeps no statistics on."""
    business = [f"C{n}" for n in range(40)]
    schema = _schema(*business, "__CURRENT_FLAG")

    listed = stats_columns(schema, ["C39", "__CURRENT_FLAG"]).split(",")

    assert listed == business[:32] + ["C39", "__CURRENT_FLAG"]


def test_a_keyed_table_clusters_by_its_keys():
    schema = _schema("ID", "SITE", "NAME")

    assert clustering_columns(Layout.KEYS, ["ID", "SITE"], schema) == (["ID", "SITE"], [])


def test_a_history_table_also_clusters_by_its_current_flag():
    schema = _schema("ID", "NAME", "__CURRENT_FLAG")

    assert clustering_columns(Layout.KEYS, ["ID"], schema) == (["ID", "__CURRENT_FLAG"], [])


def test_an_append_table_clusters_by_its_export_date_and_anchor():
    with_anchor = _schema("ID", "__EXPORT_DATE", "__ANCHOR_DT")
    without = _schema("ID", "__EXPORT_DATE")

    assert clustering_columns(Layout.EXPORTS, [], with_anchor) == (
        ["__EXPORT_DATE", "__ANCHOR_DT"],
        [],
    )
    assert clustering_columns(Layout.EXPORTS, [], without) == (["__EXPORT_DATE"], [])


def test_a_full_table_is_not_clustered():
    schema = _schema("ID", "__EXPORT_DATE")

    assert clustering_columns(Layout.NONE, ["ID"], schema) == ([], [])


def test_a_history_table_with_more_keys_than_fit_keeps_its_current_flag():
    """The flag filters every history merge; the leading keys still skip most files."""
    schema = _schema("A", "B", "C", "D", "E", "__CURRENT_FLAG")

    clustered, left_out = clustering_columns(Layout.KEYS, ["A", "B", "C", "D", "E"], schema)

    assert clustered == ["A", "B", "C", "__CURRENT_FLAG"]
    assert left_out == ["D", "E"]


def test_an_upsert_table_with_more_keys_than_fit_clusters_by_the_first_four():
    schema = _schema("A", "B", "C", "D", "E")

    clustered, left_out = clustering_columns(Layout.KEYS, ["A", "B", "C", "D", "E"], schema)

    assert clustered == ["A", "B", "C", "D"]
    assert left_out == ["E"]


def test_a_key_takes_the_tables_own_spelling():
    """A key works in any case in a merge; the statistics list needs the column's own name."""
    schema = _schema("ID", "NAME")

    assert clustering_columns(Layout.KEYS, ["id"], schema) == (["ID"], [])


@pytest.fixture
def table(spark):
    database = f"test_table_config_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE `{database}`")
    yield f"`{database}`.`TARGET`"
    spark.sql(f"DROP DATABASE IF EXISTS `{database}` CASCADE")


def _ctx(spark, table, table_config: TableConfig | None = None, keys=()) -> MagicMock:
    ctx = MagicMock()
    ctx.spark = spark
    ctx.target_table = table
    ctx.table_config = table_config
    ctx.config.output.keys = list(keys)
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

    df.write.format("delta").options(
        **DeltaTableConfig(ctx, Layout.NONE).write_options(df)
    ).saveAsTable(table)

    assert _property(spark, table) == "ID,__EXPORT_DATE"


def test_apply_follows_a_column_schema_evolution_added(spark, table):
    """An explicit list doesn't pick up a new column the way Delta's default would."""
    ctx = _ctx(spark, table)
    spark.createDataFrame([("1",)], "ID string").write.format("delta").saveAsTable(table)
    DeltaTableConfig(ctx, Layout.NONE).apply()

    evolved = spark.createDataFrame([("2", "x")], "ID string, REGION string")
    evolved.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)
    DeltaTableConfig(ctx, Layout.NONE).apply()

    assert _property(spark, table) == "ID,REGION"
    assert ctx.logger.info.call_args.kwargs["name"] == "table_configured"


def test_apply_leaves_a_table_that_is_already_current_alone(spark, table):
    ctx = _ctx(spark, table)
    df = spark.createDataFrame([("1",)], "ID string")
    df.write.format("delta").options(
        **DeltaTableConfig(ctx, Layout.NONE).write_options(df)
    ).saveAsTable(table)

    DeltaTableConfig(ctx, Layout.NONE).apply()

    ctx.logger.info.assert_not_called()


def test_apply_does_nothing_before_the_table_exists(spark, table):
    ctx = _ctx(spark, table)

    DeltaTableConfig(ctx, Layout.NONE).apply()

    ctx.logger.info.assert_not_called()
    ctx.logger.warning.assert_not_called()


def test_a_table_config_that_cannot_be_applied_yet_is_warned_about(spark, table):
    """Not skipped silently: the file names a table that isn't there."""
    ctx = _ctx(spark, table, TableConfig(comment="Agents."))

    DeltaTableConfig(ctx, Layout.NONE).apply()

    assert ctx.logger.warning.call_args.kwargs["name"] == "table_metadata_skipped"


def test_apply_also_applies_the_comments_and_tags(spark, table, monkeypatch):
    applied = MagicMock()
    monkeypatch.setattr("dbx_flame.output.table_config.apply_metadata", applied)
    ctx = _ctx(spark, table)
    spark.createDataFrame([("1",)], "ID string").write.format("delta").saveAsTable(table)

    DeltaTableConfig(ctx, Layout.NONE).apply()

    applied.assert_called_once_with(ctx, spark.table(table).schema)


def test_a_framework_column_is_commented_once_on_a_real_table(spark, table):
    ctx = _ctx(spark, table)
    df = spark.createDataFrame([("1", "x")], f"ID string, {EXPORT_DATE} string")
    df.write.format("delta").options(
        **DeltaTableConfig(ctx, Layout.NONE).write_options(df)
    ).saveAsTable(table)

    DeltaTableConfig(ctx, Layout.NONE).apply()
    assert _comment(spark, table, EXPORT_DATE) == FRAMEWORK_COMMENTS[EXPORT_DATE]

    ctx.logger.reset_mock()
    DeltaTableConfig(ctx, Layout.NONE).apply()
    ctx.logger.info.assert_not_called()


def test_a_column_comment_reaches_a_real_table_as_written(spark, table):
    """An apostrophe and a backslash survive the trip through the ALTER statement."""
    text = "The site's code, e.g. A\\B."
    ctx = _ctx(spark, table, TableConfig(columns=[{"name": "ID", "comment": text}]))
    spark.createDataFrame([("1",)], "ID string").write.format("delta").saveAsTable(table)

    DeltaTableConfig(ctx, Layout.NONE).apply()

    assert _comment(spark, table, "ID") == text


def test_a_column_schema_evolution_adds_gets_its_comment_on_the_same_run(spark, table):
    ctx = _ctx(spark, table, TableConfig(columns=[{"name": "REGION", "comment": "Region."}]))
    spark.createDataFrame([("1",)], "ID string").write.format("delta").saveAsTable(table)

    evolved = spark.createDataFrame([("2", "x")], "ID string, REGION string")
    evolved.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)
    DeltaTableConfig(ctx, Layout.NONE).apply()

    assert _comment(spark, table, "REGION") == "Region."


def _clustering(spark, table) -> list[str]:
    return list(spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]["clusteringColumns"])


def _create(spark, table, ctx, layout, df):
    """As a writer creates its target: the write, then the apply the pipeline runs after it."""
    config = DeltaTableConfig(ctx, layout)
    df.write.format("delta").options(**config.write_options(df)).saveAsTable(table)
    config.apply()


def test_a_history_target_is_clustered_after_the_write_that_created_it(spark, table):
    ctx = _ctx(spark, table, keys=["ID"])
    df = spark.createDataFrame([("1", "a", "Y")], "ID string, NAME string, __CURRENT_FLAG string")

    _create(spark, table, ctx, Layout.KEYS, df)

    assert _clustering(spark, table) == ["ID", "__CURRENT_FLAG"]
    assert ctx.logger.info.call_args.kwargs["name"] == "table_clustered"


def test_an_append_target_is_clustered_by_export_date_and_anchor(spark, table):
    ctx = _ctx(spark, table)
    df = spark.createDataFrame(
        [("1", "2026-01-01 00:00:00", "2026-01-01 00:00:00")],
        "ID string, __EXPORT_DATE string, __ANCHOR_DT string",
    )

    _create(spark, table, ctx, Layout.EXPORTS, df)

    assert _clustering(spark, table) == ["__EXPORT_DATE", "__ANCHOR_DT"]


def test_a_full_target_is_left_unclustered(spark, table):
    ctx = _ctx(spark, table, keys=["ID"])
    df = spark.createDataFrame([("1", "2026-01-01 00:00:00")], "ID string, __EXPORT_DATE string")

    _create(spark, table, ctx, Layout.NONE, df)

    assert _clustering(spark, table) == []


def test_an_existing_target_is_brought_in_line_with_its_keys(spark, table):
    df = spark.createDataFrame([("1", "a")], "ID string, NAME string")
    _create(spark, table, _ctx(spark, table, keys=["ID"]), Layout.KEYS, df)

    DeltaTableConfig(_ctx(spark, table, keys=["ID", "NAME"]), Layout.KEYS).apply()

    assert _clustering(spark, table) == ["ID", "NAME"]


def test_a_target_already_clustered_by_its_keys_is_left_alone(spark, table):
    ctx = _ctx(spark, table, keys=["ID"])
    _create(spark, table, ctx, Layout.KEYS, spark.createDataFrame([("1",)], "ID string"))

    ctx.logger.reset_mock()
    DeltaTableConfig(ctx, Layout.KEYS).apply()

    ctx.logger.info.assert_not_called()


def test_a_wide_target_written_before_clustering_gets_statistics_on_its_key_first(spark, table):
    """Its key sits past the first 32, so clustering it would fail without its statistics."""
    business = [f"C{n}" for n in range(40)]
    df = spark.createDataFrame(
        [tuple("x" for _ in business)], ", ".join(f"{c} string" for c in business)
    )
    unclustered = DeltaTableConfig(_ctx(spark, table), Layout.NONE)
    df.write.format("delta").options(**unclustered.write_options(df)).saveAsTable(table)

    DeltaTableConfig(_ctx(spark, table, keys=["C39"]), Layout.KEYS).apply()

    assert "C39" in _property(spark, table).split(",")
    assert _clustering(spark, table) == ["C39"]


def test_the_keys_left_out_of_the_clustering_are_named_in_the_log(spark, table):
    ctx = _ctx(spark, table, keys=["A", "B", "C", "D", "E"])
    df = spark.createDataFrame(
        [("a", "b", "c", "d", "e")], "A string, B string, C string, D string, E string"
    )

    _create(spark, table, ctx, Layout.KEYS, df)

    logged = ctx.logger.info.call_args.kwargs
    assert logged["name"] == "table_clustered"
    assert logged["description"].endswith("not clustered: E")
