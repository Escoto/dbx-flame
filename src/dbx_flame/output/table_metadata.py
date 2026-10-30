"""Unity Catalog comments and tags on the target: the table_config's, and the comments
the framework keeps on its own __ columns."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from dbx_flame.output.mechanics import (
    CURRENT_FLAG,
    DELETED_FLAG,
    END_DATE,
    EXPORT_DATE,
    LAST_MODIFIED,
    START_DATE,
)
from dbx_flame.pipelines.enrichment import ANCHOR_DT, SOURCE

if TYPE_CHECKING:
    from pyspark.sql import Row
    from pyspark.sql.types import StructType

    from dbx_flame.context.context import Context

_SOURCE = "TableMetadata"

# What each framework column means, for a reader who sees only the table: Genie, Catalog
# search, an auditor. Kept on every table that carries the column.
FRAMEWORK_COMMENTS: dict[str, str] = {
    EXPORT_DATE: (
        "When the source exported this record: the time in its file name, or the read time "
        "of a table. The only source of truth for time. Set once, never overwritten."
    ),
    SOURCE: (
        "Where this record was read: FILE:<path>, or TABLE:<catalog>.<schema>.<table>"
        "@<read time>. Set once, never overwritten."
    ),
    ANCHOR_DT: "The source's own date for this record (source.anchor_dt), as a timestamp.",
    LAST_MODIFIED: (
        "When the framework last wrote this row in this table. Not a business date; "
        f"use {EXPORT_DATE}."
    ),
    START_DATE: "When this version of the record became valid.",
    END_DATE: "When this version of the record stopped being valid; NULL while it is current.",
    CURRENT_FLAG: "'Y' for the current version of the record, 'N' for a past one.",
    DELETED_FLAG: "'Y' when the source deleted the record; the row is kept as history.",
}


def _literal(value: str) -> str:
    """A SQL string literal.

    Spark reads a backslash as an escape and joins 'a''b' into 'ab', so a doubled quote
    would silently drop the apostrophe. Both are escaped with a backslash instead.
    """
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _tag_pairs(tags: dict[str, str]) -> str:
    return ", ".join(f"{_literal(name)} = {_literal(value)}" for name, value in tags.items())


def _changed(wanted: dict, current: dict) -> dict:
    return {key: value for key, value in wanted.items() if current.get(key) != value}


def _table_parts(table: str) -> tuple[str, str, str]:
    """Split a three-part table name into (catalog, schema, table), stripping backticks."""
    parts = [p.strip().strip("`") for p in table.split(".")]
    if len(parts) != 3:
        raise ValueError(f"Expected catalog.schema.table, got: {table}")
    return parts[0], parts[1], parts[2]


def _information(ctx: Context, view: str, schema_column: str = "schema_name") -> list[Row]:
    """The target's rows in one of its catalog's information_schema views."""
    catalog, schema, table = _table_parts(ctx.target_table)
    # Unity Catalog lists names in lowercase, and our tables are UPPERCASE.
    return (
        ctx.spark.table(f"`{catalog}`.information_schema.{view}")
        .where(
            (F.lower(schema_column) == schema.lower()) & (F.lower("table_name") == table.lower())
        )
        .collect()
    )


def _table_comment(ctx: Context) -> str | None:
    rows = _information(ctx, "tables", schema_column="table_schema")
    return rows[0].comment if rows else None


def _table_tags(ctx: Context) -> dict[str, str]:
    return {row.tag_name: row.tag_value for row in _information(ctx, "table_tags")}


def _column_tags(ctx: Context) -> dict[str, dict[str, str]]:
    """Tags per column, keyed by the lowercase column name Unity Catalog lists."""
    tags: dict[str, dict[str, str]] = {}
    for row in _information(ctx, "column_tags"):
        tags.setdefault(row.column_name.lower(), {})[row.tag_name] = row.tag_value
    return tags


def apply_metadata(ctx: Context, schema: StructType) -> None:
    """Keep the comments and tags on the existing target, writing only what changed.

    Comments and tags this doesn't name are left alone, so whatever a data steward sets
    elsewhere survives. The information_schema views are read only for what the
    table_config actually sets.
    """
    config = ctx.table_config
    table = ctx.target_table
    changes: list[str] = []

    if config and config.comment and _table_comment(ctx) != config.comment:
        ctx.spark.sql(f"COMMENT ON TABLE {table} IS {_literal(config.comment)}")
        changes.append("table comment")

    if config and config.tags:
        tags = _changed(config.tags, _table_tags(ctx))
        if tags:
            # SET TAGS has no DataFrame equivalent, hence the escaped literals.
            ctx.spark.sql(f"ALTER TABLE {table} SET TAGS ({_tag_pairs(tags)})")
            changes.append(f"table tags {sorted(tags)}")

    current = {field.name: field.metadata.get("comment") for field in schema.fields}
    comments = {name: text for name, text in FRAMEWORK_COMMENTS.items() if name in current}
    if config:
        comments |= {column.name: column.comment for column in config.columns if column.comment}
    for column, text in _changed(comments, current).items():
        ctx.spark.sql(f"ALTER TABLE {table} ALTER COLUMN `{column}` COMMENT {_literal(text)}")
        changes.append(f"comment on {column}")

    if config and any(column.tags for column in config.columns):
        current_tags = _column_tags(ctx)
        for column in config.columns:
            tags = _changed(column.tags, current_tags.get(column.name.lower(), {}))
            if tags:
                ctx.spark.sql(
                    f"ALTER TABLE {table} ALTER COLUMN `{column.name}` SET TAGS ({_tag_pairs(tags)})"
                )
                changes.append(f"tags on {column.name} {sorted(tags)}")

    if changes:
        ctx.logger.info(
            name="table_metadata_applied",
            source=_SOURCE,
            description=f"{table}: " + "; ".join(changes),
            total=len(changes),
        )
