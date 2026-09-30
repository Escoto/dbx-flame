"""Unity Catalog table tagging."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def _parse_table_parts(table: str) -> tuple[str, str, str]:
    """Split a three-part table name into (catalog, schema, table), stripping backticks."""
    parts = [p.strip().strip("`") for p in table.split(".")]
    if len(parts) != 3:
        raise ValueError(f"Expected catalog.schema.table, got: {table}")
    return parts[0], parts[1], parts[2]


def _escape(value: str) -> str:
    return value.replace("'", "''")


def apply_tags(spark: SparkSession, table: str, tags: dict[str, str]) -> None:
    """Upsert Unity Catalog tags on a table. Only writes when new tag values appear."""
    if not tags:
        return

    if not spark.catalog.tableExists(table):
        return

    catalog, schema, table_name = _parse_table_parts(table)

    query = (
        f"SELECT tag_name, tag_value "
        f"FROM `{catalog}`.information_schema.table_tags "
        f"WHERE schema_name = '{_escape(schema)}' "
        f"AND table_name = '{_escape(table_name)}'"
    )
    current_tags = {row.tag_name: row.tag_value for row in spark.sql(query).collect()}

    needs_update = any(tags.get(k) != current_tags.get(k) for k in tags)
    if not needs_update:
        return

    merged = {**current_tags, **tags}
    tag_pairs = ", ".join(f"'{_escape(k)}' = '{_escape(v)}'" for k, v in merged.items())
    spark.sql(f"ALTER TABLE {table} SET TAGS ({tag_pairs})")
