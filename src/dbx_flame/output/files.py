"""FileWriter — interface only; implementation deferred."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from dbx_flame.context.config import Verb
from dbx_flame.output.base import Requirements

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from dbx_flame.context.context import Context


class FileWriter:
    """Write to Volume paths (export targets). Interface only — not implemented."""

    verb: ClassVar[Verb]
    requires: ClassVar[Requirements] = Requirements()

    def write(self, df: DataFrame, ctx: Context) -> None:
        raise NotImplementedError("FileWriter is an interface stub; implementation deferred.")
