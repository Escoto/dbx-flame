"""Unity Catalog table tagging."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from dbx_flame.context.context import Context

_SOURCE = "TableTagging"


def _parse_table_parts(table: str) -> tuple[str, str, str]:
    """Split a three-part table name into (catalog, schema, table), stripping backticks."""
    parts = [p.strip().strip("`") for p in table.split(".")]
    if len(parts) != 3:
        raise ValueError(f"Expected catalog.schema.table, got: {table}")
    return parts[0], parts[1], parts[2]


def _escape(value: str) -> str:
    return value.replace("'", "''")


def _current_tags(ctx: Context) -> dict[str, str]:
    catalog, schema, table_name = _parse_table_parts(ctx.target_table)
    # Unity Catalog lists names in lowercase, and our tables are UPPERCASE.
    rows = (
        ctx.spark.table(f"`{catalog}`.information_schema.table_tags")
        .where(
            (F.lower("schema_name") == schema.lower())
            & (F.lower("table_name") == table_name.lower())
        )
        .select("tag_name", "tag_value")
        .collect()
    )
    return {row.tag_name: row.tag_value for row in rows}


def apply_tags(ctx: Context) -> None:
    """Upsert the task's output.tags on its target. Only writes when a value changes.

    SET TAGS leaves tags it doesn't name alone, so tags set outside the framework survive.
    """
    tags = ctx.config.output.tags
    if not tags:
        return

    table = ctx.target_table
    if not ctx.spark.catalog.tableExists(table):
        ctx.logger.warning(
            name="table_not_tagged",
            source=_SOURCE,
            description=f"{table} does not exist, so its tags were not applied",
        )
        return

    current = _current_tags(ctx)
    changed = {k: v for k, v in tags.items() if current.get(k) != v}
    if not changed:
        return

    # SET TAGS has no DataFrame equivalent, hence the escaped literals.
    pairs = ", ".join(f"'{_escape(k)}' = '{_escape(v)}'" for k, v in changed.items())
    ctx.spark.sql(f"ALTER TABLE {table} SET TAGS ({pairs})")
    ctx.logger.info(
        name="table_tagged",
        source=_SOURCE,
        description=f"{table} tagged with {changed}",
    )
