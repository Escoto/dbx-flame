"""SAS origin — Auto Loader binaryFile discovery + pandas.read_sas per file."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from dbx_flame.context.context import Context


class SasSource:
    """Read SAS7BDAT files via binaryFile discovery and pandas decode."""

    def read(self, ctx: Context) -> DataFrame:
        raise NotImplementedError("P6")
