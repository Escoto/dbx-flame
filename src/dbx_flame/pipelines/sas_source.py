"""SAS origin — Auto Loader binaryFile discovery + pandas.read_sas per file."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from dbx_flame.pipelines.base import FILE_STRATEGIES

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from dbx_flame.context.config import IncrementStrategy
    from dbx_flame.context.context import Context


class SasSource:
    """Read SAS7BDAT files via binaryFile discovery and pandas decode."""

    increment_strategies: ClassVar[tuple[IncrementStrategy, ...]] = FILE_STRATEGIES

    def read(self, ctx: Context) -> DataFrame:
        raise NotImplementedError("P6")
