"""Tests for Unity Catalog table tagging — mock-based (no live catalog)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dbx_flame.observability.tagging import apply_tags

# The lookup builds Column expressions, which need an active SparkContext.
pytestmark = pytest.mark.usefixtures("spark")

TABLE = "`cat`.`schema`.`TBL`"


def _mock_tag_rows(tags: dict[str, str]) -> list[MagicMock]:
    rows = []
    for k, v in tags.items():
        row = MagicMock()
        row.tag_name = k
        row.tag_value = v
        rows.append(row)
    return rows


def _ctx(
    tags: dict[str, str], *, table_exists: bool = True, existing: dict[str, str] | None = None
) -> MagicMock:
    ctx = MagicMock()
    ctx.target_table = TABLE
    ctx.config.output.tags = tags
    ctx.spark.catalog.tableExists.return_value = table_exists
    lookup = ctx.spark.table.return_value.where.return_value.select.return_value
    lookup.collect.return_value = _mock_tag_rows(existing or {})
    return ctx


def _alter(ctx: MagicMock) -> str:
    return ctx.spark.sql.call_args.args[0]


def test_no_tags_configured_touches_nothing():
    ctx = _ctx({})

    apply_tags(ctx)

    ctx.spark.catalog.tableExists.assert_not_called()
    ctx.spark.sql.assert_not_called()


def test_a_missing_table_is_warned_about_not_skipped_silently():
    ctx = _ctx({"env": "dev"}, table_exists=False)

    apply_tags(ctx)

    ctx.spark.sql.assert_not_called()
    assert ctx.logger.warning.call_args.kwargs["name"] == "table_not_tagged"


def test_new_tags_are_set_and_logged():
    ctx = _ctx({"project": "dbx-flame", "environment": "dev"})

    apply_tags(ctx)

    assert _alter(ctx) == (
        f"ALTER TABLE {TABLE} SET TAGS ('project' = 'dbx-flame', 'environment' = 'dev')"
    )
    assert ctx.logger.info.call_args.kwargs["name"] == "table_tagged"


def test_tags_already_in_place_are_not_rewritten():
    ctx = _ctx({"env": "dev"}, existing={"env": "dev", "owner": "someone"})

    apply_tags(ctx)

    ctx.spark.sql.assert_not_called()
    ctx.logger.info.assert_not_called()


def test_only_the_changed_tags_are_set():
    """SET TAGS leaves unnamed tags alone, so a tag set outside the framework survives."""
    ctx = _ctx({"env": "dev", "project": "dbx-flame"}, existing={"env": "dev", "owner": "x"})

    apply_tags(ctx)

    assert _alter(ctx) == f"ALTER TABLE {TABLE} SET TAGS ('project' = 'dbx-flame')"


def test_a_tag_moved_to_another_key_with_the_same_value_is_set():
    ctx = _ctx({"region": "dev"}, existing={"env": "dev"})

    apply_tags(ctx)

    assert "'region' = 'dev'" in _alter(ctx)


def test_quotes_in_tag_values_are_escaped():
    ctx = _ctx({"note": "it's a test"})

    apply_tags(ctx)

    assert "'note' = 'it''s a test'" in _alter(ctx)


def test_tags_are_looked_up_in_the_target_catalog():
    ctx = _ctx({"env": "dev"})

    apply_tags(ctx)

    ctx.spark.table.assert_called_once_with("`cat`.information_schema.table_tags")
