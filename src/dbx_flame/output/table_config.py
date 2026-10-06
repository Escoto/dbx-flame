"""Table-level configuration the framework keeps on every Delta table it writes."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql.types import ArrayType, MapType, StructType

from dbx_flame.observability.tagging import apply_tags
from dbx_flame.output.mechanics import EXPORT_DATE
from dbx_flame.pipelines.enrichment import ANCHOR_DT

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructField

    from dbx_flame.context.context import Context

STATS_COLUMNS = "delta.dataSkippingStatsColumns"

# Delta's own default is statistics on the first 32 leaf columns. Setting the property
# replaces that default, so the list starts from the same 32.
_DEFAULT_INDEXED = 32

# The columns the framework itself filters on, wherever they sit in the table.
_ALWAYS_INDEXED = (EXPORT_DATE, ANCHOR_DT)

_SOURCE = "DeltaTableConfig"


def _leaves(fields: list[StructField], prefix: str = "") -> Iterator[str]:
    """Leaf columns in table order. Delta rejects arrays and maps in the list."""
    for field in fields:
        name = f"{prefix}{field.name}"
        if isinstance(field.dataType, StructType):
            yield from _leaves(field.dataType.fields, f"{name}.")
        elif not isinstance(field.dataType, (ArrayType, MapType)):
            yield name


def stats_columns(schema: StructType) -> str:
    """The first 32 leaf columns, plus the framework's own filter columns when present."""
    leaves = list(_leaves(schema.fields))
    chosen = leaves[:_DEFAULT_INDEXED]
    chosen += [name for name in _ALWAYS_INDEXED if name in leaves and name not in chosen]
    return ",".join(chosen)


class DeltaTableConfig:
    """Everything the framework configures on a Delta table, in one place.

    Today that is which columns carry data-skipping statistics, and the task's Unity
    Catalog tags. Other table-level settings (Z-ordering, vacuum retention, ...) belong
    here too.
    """

    def __init__(self, ctx: Context) -> None:
        self._ctx = ctx

    def write_options(self, df: DataFrame) -> dict[str, str]:
        """Options for a write that may create the table.

        Delta turns delta.* options into table properties only when the write creates
        the table; on an existing one they are ignored, and apply() keeps it current.
        """
        return {STATS_COLUMNS: stats_columns(df.schema)}

    def apply(self) -> None:
        """Bring an existing table's configuration in line with its schema and tags.

        An explicit statistics list doesn't pick up a column schema evolution adds, the
        way Delta's default would. The ALTER is a metadata-only commit, and only runs
        when the list actually changes.
        """
        ctx = self._ctx
        apply_tags(ctx)

        table = ctx.target_table
        if not ctx.spark.catalog.tableExists(table):
            return

        wanted = stats_columns(ctx.spark.table(table).schema)
        properties = {
            row.key: row.value for row in ctx.spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
        }
        if properties.get(STATS_COLUMNS) == wanted:
            return

        ctx.spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('{STATS_COLUMNS}' = '{wanted}')")
        ctx.logger.info(
            name="table_configured",
            source=_SOURCE,
            description=f"{STATS_COLUMNS} on {table} set to {wanted}",
        )
