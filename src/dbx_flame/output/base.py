"""Writer protocol, verb requirements and table layout declarations."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum, unique
from typing import TYPE_CHECKING, ClassVar, Protocol, runtime_checkable

from dbx_flame.context.config import IncrementStrategy, Origin, Verb

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from dbx_flame.context.context import Context


@unique
class Layout(StrEnum):
    """What a verb's target is clustered by: the columns its own reads and merges filter on."""

    # Replaced by one export on every write, so there is nothing to skip.
    NONE = "none"
    # Read by export: a watermark read, or a snapshot cut on __EXPORT_DATE.
    EXPORTS = "exports"
    # Merged on output.keys, and a history table only against its current rows.
    KEYS = "keys"


@dataclass(frozen=True)
class Requirements:
    """What a verb requires from the configuration to operate."""

    keys: bool = False
    event_time: bool = False
    supports_deletes: bool = False
    supports_snapshot_scope: bool = False
    origins: frozenset[Origin] = frozenset(Origin)

    # The batch is a backlog of snapshots the verb replays one at a time, so dedup
    # and the policy gate judge each snapshot rather than the backlog as a whole.
    per_snapshot: bool = False

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
    layout: ClassVar[Layout]

    def write(self, df: DataFrame, ctx: Context) -> None: ...
