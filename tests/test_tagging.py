"""Tests for Unity Catalog table tagging — mock-based (no live catalog)."""

from __future__ import annotations

from unittest.mock import MagicMock

from dbx_flame.observability.tagging import apply_tags


def _mock_tag_rows(tags: dict[str, str]) -> list[MagicMock]:
    rows = []
    for k, v in tags.items():
        row = MagicMock()
        row.tag_name = k
        row.tag_value = v
        rows.append(row)
    return rows


def _make_spark(*, table_exists: bool = True, existing_tags: dict[str, str] | None = None):
    spark = MagicMock()
    spark.catalog.tableExists.return_value = table_exists

    rows = _mock_tag_rows(existing_tags or {})
    spark.sql.return_value.collect.return_value = rows
    return spark


class TestApplyTags:
    def test_noop_when_table_missing(self):
        spark = _make_spark(table_exists=False)
        apply_tags(spark, "cat.schema.tbl", {"env": "dev"})

        spark.catalog.tableExists.assert_called_once_with("cat.schema.tbl")
        spark.sql.assert_not_called()

    def test_upsert_new_tags(self):
        spark = _make_spark(table_exists=True, existing_tags={})
        apply_tags(spark, "cat.schema.tbl", {"env": "dev"})

        calls = [str(c) for c in spark.sql.call_args_list]
        assert any("ALTER TABLE" in c and "'env' = 'dev'" in c for c in calls)

    def test_no_write_when_unchanged(self):
        spark = _make_spark(table_exists=True, existing_tags={"env": "dev"})
        apply_tags(spark, "cat.schema.tbl", {"env": "dev"})

        calls = [str(c) for c in spark.sql.call_args_list]
        assert not any("ALTER TABLE" in c for c in calls)

    def test_write_when_key_changes_same_value(self):
        """Comparing only set(values) would make {env: dev} and {region: dev}
        would look identical. The new code compares key-value pairs."""
        spark = _make_spark(table_exists=True, existing_tags={"env": "dev"})
        apply_tags(spark, "cat.schema.tbl", {"region": "dev"})

        calls = [str(c) for c in spark.sql.call_args_list]
        assert any("ALTER TABLE" in c for c in calls)
        alter_call = [c for c in calls if "ALTER TABLE" in c][0]
        assert "'env' = 'dev'" in alter_call
        assert "'region' = 'dev'" in alter_call

    def test_special_characters_escaped(self):
        spark = _make_spark(table_exists=True, existing_tags={})
        apply_tags(spark, "cat.schema.tbl", {"note": "it's a test"})

        calls = [str(c) for c in spark.sql.call_args_list]
        alter_call = [c for c in calls if "ALTER TABLE" in c][0]
        assert "'it''s a test'" in alter_call

    def test_backticked_table_name(self):
        spark = _make_spark(table_exists=True, existing_tags={})
        apply_tags(spark, "`my_cat`.`my_schema`.`my_table`", {"env": "dev"})

        select_call = spark.sql.call_args_list[0][0][0]
        assert "`my_cat`.information_schema.table_tags" in select_call
        assert "schema_name = 'my_schema'" in select_call
        assert "table_name = 'my_table'" in select_call
