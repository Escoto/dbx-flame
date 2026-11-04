"""Table-level configuration the framework keeps on every Delta table it writes."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql.types import ArrayType, MapType, StructType

from dbx_flame.output.base import Layout
from dbx_flame.output.mechanics import CURRENT_FLAG, EXPORT_DATE
from dbx_flame.output.table_metadata import apply_metadata
from dbx_flame.pipelines.enrichment import ANCHOR_DT

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructField

    from dbx_flame.context.context import Context

STATS_COLUMNS = "delta.dataSkippingStatsColumns"

# Delta's own default is statistics on the first 32 leaf columns. Setting the property
# replaces that default, so the list starts from the same 32.
_DEFAULT_INDEXED = 32

# The columns the framework itself filters on, wherever they sit in the table.
_ALWAYS_INDEXED = (EXPORT_DATE, ANCHOR_DT)

# Liquid clustering takes at most this many columns.
_MAX_CLUSTERED = 4

_SOURCE = "DeltaTableConfig"


def _leaves(fields: list[StructField], prefix: str = "") -> Iterator[str]:
    """Leaf columns in table order. Delta rejects arrays and maps in the list."""
    for field in fields:
        name = f"{prefix}{field.name}"
        if isinstance(field.dataType, StructType):
            yield from _leaves(field.dataType.fields, f"{name}.")
        elif not isinstance(field.dataType, (ArrayType, MapType)):
            yield name


def stats_columns(schema: StructType, clustered: Sequence[str] = ()) -> str:
    """The first 32 leaf columns, plus the framework's filter columns and the clustering ones.

    Delta refuses to cluster by a column it keeps no statistics on, so a clustering column
    past the first 32 is listed too.
    """
    leaves = list(_leaves(schema.fields))
    chosen = leaves[:_DEFAULT_INDEXED]
    for name in (*_ALWAYS_INDEXED, *clustered):
        if name in leaves and name not in chosen:
            chosen.append(name)
    return ",".join(chosen)


def clustering_columns(
    layout: Layout, keys: list[str], schema: StructType
) -> tuple[list[str], list[str]]:
    """The columns to cluster by, and the keys left out because clustering takes only four.

    A framework column goes in only where the table has it, and keeps its place when keys
    must be cut: a history merge filters on __CURRENT_FLAG whatever the keys. Keys take the
    table's own spelling, since a key can be configured in another case than the column.
    """
    names = {field.name.upper(): field.name for field in schema.fields}
    if layout == Layout.EXPORTS:
        return [names[name] for name in (EXPORT_DATE, ANCHOR_DT) if name in names], []
    if layout != Layout.KEYS:
        return [], []

    framework = [names[CURRENT_FLAG]] if CURRENT_FLAG in names else []
    resolved = [names.get(key.upper(), key) for key in keys]
    room = _MAX_CLUSTERED - len(framework)
    return resolved[:room] + framework, resolved[room:]


class DeltaTableConfig:
    """Everything the framework configures on a Delta table, in one place.

    Today that is which columns carry data-skipping statistics, liquid clustering on the
    columns the verb's own reads and merges filter on, and the comments and Unity Catalog
    tags from the task's table_config. Other table-level settings (vacuum retention, ...)
    belong here too.
    """

    def __init__(self, ctx: Context, layout: Layout) -> None:
        self._ctx = ctx
        self._layout = layout

    def write_options(self, df: DataFrame) -> dict[str, str]:
        """Options for a write that may create the table.

        Delta turns delta.* options into table properties only when the write creates
        the table; on an existing one they are ignored, and apply() keeps it current.
        """
        clustered, _ = self._clustering(df.schema)
        return {STATS_COLUMNS: stats_columns(df.schema, clustered)}

    def apply(self) -> None:
        """Bring an existing table's configuration in line with its schema and table_config.

        Runs after every write, so a column schema evolution just added gets its comment,
        tags and statistics on the same run.
        """
        ctx = self._ctx
        table = ctx.target_table
        if not ctx.spark.catalog.tableExists(table):
            if ctx.table_config:
                ctx.logger.warning(
                    name="table_metadata_skipped",
                    source=_SOURCE,
                    description=f"{table} does not exist, so its table_config was not applied",
                )
            return

        schema = ctx.spark.table(table).schema
        apply_metadata(ctx, schema)
        clustered, left_out = self._clustering(schema)
        # Statistics first: Delta refuses to cluster by a column it has none on.
        self._keep_stats_columns(schema, clustered)
        self._keep_clustering(clustered, left_out)

    def _clustering(self, schema: StructType) -> tuple[list[str], list[str]]:
        return clustering_columns(self._layout, self._ctx.config.output.keys, schema)

    def _keep_stats_columns(self, schema: StructType, clustered: list[str]) -> None:
        """An explicit statistics list doesn't pick up a column schema evolution adds, the
        way Delta's default would. The ALTER is a metadata-only commit, and only runs
        when the list actually changes.
        """
        ctx = self._ctx
        table = ctx.target_table
        wanted = stats_columns(schema, clustered)
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

    def _keep_clustering(self, clustered: list[str], left_out: list[str]) -> None:
        """Cluster the table by what its verb filters on; a FULL table is left as it is.

        The ALTER is a metadata-only commit, and only runs when the columns change. Spark
        3.5 can't cluster the write that creates a table, so the next OPTIMIZE clusters the
        files that write left.
        """
        if not clustered:
            return

        ctx = self._ctx
        table = ctx.target_table
        detail = ctx.spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
        if list(detail["clusteringColumns"] or []) == clustered:
            return

        columns = ", ".join(f"`{column}`" for column in clustered)
        ctx.spark.sql(f"ALTER TABLE {table} CLUSTER BY ({columns})")
        description = f"{table} clustered by {', '.join(clustered)}"
        if left_out:
            description += (
                f"; clustering takes at most {_MAX_CLUSTERED} columns, so these keys are "
                f"not clustered: {', '.join(left_out)}"
            )
        ctx.logger.info(name="table_clustered", source=_SOURCE, description=description)
