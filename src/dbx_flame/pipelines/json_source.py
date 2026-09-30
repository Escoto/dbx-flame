"""JSON origin — Auto Loader with cloudFiles format."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dbx_flame.pipelines.base import cloud_files_options, flag

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from dbx_flame.context.context import Context

_FORMAT = "json"

_SOURCE = "JsonSource"


def reader_options(ctx: Context) -> dict[str, str]:
    """Every Auto Loader option for this read, as the strings Spark expects.

    Separate from the read itself on purpose: cloudFiles only exists on Databricks,
    so this is the half that can be verified locally.
    """
    options = ctx.config.source.options
    resolved = {
        **cloud_files_options(ctx, _FORMAT),
        # An export is one JSON document spanning many lines, not one object per line.
        "multiLine": flag(options.multiline),
    }
    if options.schema_hints:
        resolved["cloudFiles.schemaHints"] = options.schema_hints
    return resolved


class JsonSource:
    """Read JSON files via Auto Loader streaming."""

    def read(self, ctx: Context) -> DataFrame:
        glob = ctx.inbound_glob
        assert glob  # guaranteed by SourceConfig for every file origin

        ctx.logger.info(
            name="source_read",
            source=_SOURCE,
            description=f"Auto Loader stream over {glob}",
        )
        return ctx.spark.readStream.format("cloudFiles").options(**reader_options(ctx)).load(glob)
