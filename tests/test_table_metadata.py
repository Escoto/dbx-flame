"""Tests for output.table_metadata — comments and tags on the target, mock-based where
they need Unity Catalog's information_schema."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from dbx_flame.context.table_config import TableConfig
from dbx_flame.output.mechanics import EXPORT_DATE
from dbx_flame.output.table_metadata import FRAMEWORK_COMMENTS, _literal, apply_metadata
from dbx_flame.pipelines.enrichment import SOURCE

# The lookups build Column expressions, which need an active SparkContext.
pytestmark = pytest.mark.usefixtures("spark")

TABLE = "`cat`.`schema`.`TBL`"


def _row(**fields: Any) -> MagicMock:
    row = MagicMock()
    for name, value in fields.items():
        setattr(row, name, value)
    return row


def _ctx(
    config: dict[str, Any] | None = None,
    *,
    table_comment: str | None = None,
    table_tags: dict[str, str] | None = None,
    column_tags: dict[str, dict[str, str]] | None = None,
) -> MagicMock:
    """A target whose information_schema views hold what is already set on it."""
    ctx = MagicMock()
    ctx.target_table = TABLE
    ctx.table_config = TableConfig(**config) if config else None
    views = {
        "tables": [_row(comment=table_comment)],
        "table_tags": [_row(tag_name=k, tag_value=v) for k, v in (table_tags or {}).items()],
        "column_tags": [
            _row(column_name=column, tag_name=k, tag_value=v)
            for column, tags in (column_tags or {}).items()
            for k, v in tags.items()
        ],
    }

    def table(name: str) -> MagicMock:
        frame = MagicMock()
        frame.where.return_value.collect.return_value = views[name.rsplit(".", 1)[1]]
        return frame

    ctx.spark.table.side_effect = table
    return ctx


def _schema(*names: str, comments: dict[str, str] | None = None) -> StructType:
    """The target's columns, with the comments already on them."""
    comments = comments or {}
    return StructType(
        [
            (
                StructField(name, StringType(), metadata={"comment": comments[name]})
                if name in comments
                else StructField(name, StringType())
            )
            for name in names
        ]
    )


def _statements(ctx: MagicMock) -> list[str]:
    return [call.args[0] for call in ctx.spark.sql.call_args_list]


def _views_read(ctx: MagicMock) -> list[str]:
    return [call.args[0] for call in ctx.spark.table.call_args_list]


def test_a_table_with_nothing_to_set_is_left_alone():
    ctx = _ctx()

    apply_metadata(ctx, _schema("ID"))

    ctx.spark.sql.assert_not_called()
    ctx.spark.table.assert_not_called()
    ctx.logger.info.assert_not_called()


def test_the_framework_columns_the_table_carries_get_their_comments():
    """No file needed: the framework knows what its own columns mean."""
    ctx = _ctx()

    apply_metadata(ctx, _schema("ID", EXPORT_DATE, SOURCE))

    assert _statements(ctx) == [
        f"ALTER TABLE {TABLE} ALTER COLUMN `{column}` COMMENT "
        f"{_literal(FRAMEWORK_COMMENTS[column])}"
        for column in (EXPORT_DATE, SOURCE)
    ]


def test_a_framework_comment_already_in_place_is_not_rewritten():
    ctx = _ctx()
    schema = _schema("ID", EXPORT_DATE, comments={EXPORT_DATE: FRAMEWORK_COMMENTS[EXPORT_DATE]})

    apply_metadata(ctx, schema)

    ctx.spark.sql.assert_not_called()
    ctx.logger.info.assert_not_called()


def test_the_table_comment_is_set_when_it_differs():
    ctx = _ctx({"comment": "Study milestones."}, table_comment="Old text.")

    apply_metadata(ctx, _schema("ID"))

    assert _statements(ctx) == [f"COMMENT ON TABLE {TABLE} IS 'Study milestones.'"]
    assert _views_read(ctx) == ["`cat`.information_schema.tables"]


def test_a_table_comment_already_in_place_is_not_rewritten():
    ctx = _ctx({"comment": "Study milestones."}, table_comment="Study milestones.")

    apply_metadata(ctx, _schema("ID"))

    ctx.spark.sql.assert_not_called()


def test_only_the_changed_table_tags_are_set():
    """SET TAGS leaves unnamed tags alone, so a tag set outside the framework survives."""
    ctx = _ctx(
        {"tags": {"env": "dev", "project": "dbx-flame"}},
        table_tags={"env": "dev", "owner": "someone"},
    )

    apply_metadata(ctx, _schema("ID"))

    assert _statements(ctx) == [f"ALTER TABLE {TABLE} SET TAGS ('project' = 'dbx-flame')"]


def test_table_tags_already_in_place_are_not_rewritten():
    ctx = _ctx({"tags": {"env": "dev"}}, table_tags={"env": "dev"})

    apply_metadata(ctx, _schema("ID"))

    ctx.spark.sql.assert_not_called()


def test_a_column_comment_from_the_file_is_set_and_others_are_left_alone():
    """NAME's comment was set by someone else, and the file doesn't name NAME."""
    ctx = _ctx({"columns": [{"name": "ID", "comment": "The site's ID."}]})
    schema = _schema("ID", "NAME", comments={"NAME": "Set by a data steward."})

    apply_metadata(ctx, schema)

    comment = _literal("The site's ID.")
    assert _statements(ctx) == [f"ALTER TABLE {TABLE} ALTER COLUMN `ID` COMMENT {comment}"]


def test_a_column_comment_already_in_place_is_not_rewritten():
    ctx = _ctx({"columns": [{"name": "ID", "comment": "The ID."}]})

    apply_metadata(ctx, _schema("ID", comments={"ID": "The ID."}))

    ctx.spark.sql.assert_not_called()


def test_only_the_changed_column_tags_are_set():
    """Unity Catalog lists column names in lowercase; the file names them as the table does."""
    ctx = _ctx(
        {"columns": [{"name": "ID", "tags": {"PII": "no", "category": "key"}}]},
        column_tags={"id": {"PII": "no", "owner": "someone"}},
    )

    apply_metadata(ctx, _schema("ID"))

    assert _statements(ctx) == [
        f"ALTER TABLE {TABLE} ALTER COLUMN `ID` SET TAGS ('category' = 'key')"
    ]
    assert _views_read(ctx) == ["`cat`.information_schema.column_tags"]


def test_unity_catalog_is_read_only_for_what_the_file_sets():
    """Column comments come from the schema; no information_schema view is needed."""
    ctx = _ctx({"columns": [{"name": "ID", "comment": "The ID.", "cast": {"target_type": "int"}}]})

    apply_metadata(ctx, _schema("ID"))

    ctx.spark.table.assert_not_called()


def test_every_change_is_logged_in_one_audit_row():
    ctx = _ctx(
        {
            "comment": "Study milestones.",
            "tags": {"domain": "clinical"},
            "columns": [{"name": "ID", "comment": "The ID.", "tags": {"PII": "no"}}],
        }
    )

    apply_metadata(ctx, _schema("ID", EXPORT_DATE))

    assert len(_statements(ctx)) == 5
    ctx.logger.info.assert_called_once()
    logged = ctx.logger.info.call_args.kwargs
    assert logged["name"] == "table_metadata_applied"
    assert logged["total"] == 5
    assert "comment on ID" in logged["description"]


@pytest.mark.parametrize(
    "value", ["plain", "it's", "a \\ backslash", "both ' and \\'", "trailing \\"]
)
def test_a_literal_reads_back_as_the_same_text(spark, value):
    """Run through real Spark SQL, the way the ALTER statements are."""
    assert spark.sql(f"SELECT {_literal(value)} AS v").collect()[0].v == value


def test_doubling_a_quote_would_silently_drop_it(spark):
    """Why the literal escapes with a backslash: Spark joins adjacent literals."""
    assert spark.sql("SELECT 'it''s' AS v").collect()[0].v == "its"
