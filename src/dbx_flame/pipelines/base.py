"""SourcePipeline protocol and the Auto Loader options every file origin shares."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Protocol, runtime_checkable

from dbx_flame.context.config import IncrementStrategy, SchemaEvolution

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from dbx_flame.context.context import Context

# Auto Loader tracks which files it has read in its checkpoint; there is no other way
# to take only what is new from a folder.
FILE_STRATEGIES = (IncrementStrategy.CHECKPOINT,)


@runtime_checkable
class SourcePipeline(Protocol):
    """Read from a configured origin and return a DataFrame (streaming or batch)."""

    # Which increment strategies this origin can read with. Start accepts a strategy only
    # when both the origin and the verb declare it.
    increment_strategies: ClassVar[tuple[IncrementStrategy, ...]]

    def read(self, ctx: Context) -> DataFrame: ...


# The one surfaced schema_evolution knob, spelled the way Auto Loader spells it.
EVOLUTION_MODES: dict[SchemaEvolution, str] = {
    SchemaEvolution.ADD_NEW_COLUMNS: "addNewColumns",
    SchemaEvolution.ADD_NEW_COLUMNS_WITH_TYPE_WIDENING: "addNewColumnsWithTypeWidening",
    SchemaEvolution.FAIL_ON_NEW_COLUMNS: "failOnNewColumns",
    SchemaEvolution.NONE: "none",
}


def flag(value: bool) -> str:
    return "true" if value else "false"


def cloud_files_options(ctx: Context, source_format: str) -> dict[str, str]:
    """The Auto Loader options that do not depend on the file format.

    Shared so a new file origin inherits the evolution mapping rather than restating it.
    """
    return {
        "cloudFiles.format": source_format,
        "cloudFiles.schemaLocation": ctx.schema_hints_location,
        "cloudFiles.schemaEvolutionMode": EVOLUTION_MODES[ctx.config.schema_evolution],
        # A file that vanishes between listing and read must not fail the batch.
        "ignoreMissingFiles": "true",
    }
