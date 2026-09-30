"""Writer protocol and verb requirements declaration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar, Protocol, runtime_checkable

from dbx_flame.context.config import IncrementStrategy, Origin, Verb

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from dbx_flame.context.context import Context


@dataclass(frozen=True)
class Requirements:
    """What a verb requires from the configuration to operate."""

    keys: bool = False
    event_time: bool = False
    supports_deletes: bool = False
    supports_snapshot_scope: bool = False
    origins: frozenset[Origin] = frozenset(Origin)

    # Which increment strategies this verb can run under; the first is the default
    # and the only one used unless source.increment_strategy picks another.
    increment_strategies: tuple[IncrementStrategy, ...] = (IncrementStrategy.CHECKPOINT,)

    @property
    def increment_strategy(self) -> IncrementStrategy:
        """The default strategy for this verb."""
        return self.increment_strategies[0]


@runtime_checkable
class Writer(Protocol):
    """Write a DataFrame to a target using verb-specific semantics."""

    verb: ClassVar[Verb]
    requires: ClassVar[Requirements]

    def write(self, df: DataFrame, ctx: Context) -> None: ...
