"""CSV/TXT origin — Auto Loader with cloudFiles format."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dbx_flame.pipelines.base import cloud_files_options, flag

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from dbx_flame.context.context import Context

# A txt source is still read by the csv reader; only the glob's extension differs.
_FORMAT = "csv"

_SOURCE = "CsvSource"


def reader_options(ctx: Context) -> dict[str, str]:
    """Every Auto Loader option for this read, as the strings Spark expects.

    Separate from the read itself on purpose: cloudFiles only exists on Databricks,
    so this is the half that can be verified locally.
    """
    options = ctx.config.source.options
    return {
        **cloud_files_options(ctx, _FORMAT),
        "header": flag(options.header),
        "delimiter": options.delimiter,
        "multiLine": flag(options.multiline),
        "quote": options.quote,
        "escape": options.escape,
    }


class CsvSource:
    """Read CSV (or TXT) files via Auto Loader streaming."""

    def read(self, ctx: Context) -> DataFrame:
        glob = ctx.inbound_glob
        assert glob  # guaranteed by SourceConfig for every file origin

        ctx.logger.info(
            name="source_read",
            source=_SOURCE,
            description=f"Auto Loader stream over {glob}",
        )
        return ctx.spark.readStream.format("cloudFiles").options(**reader_options(ctx)).load(glob)
