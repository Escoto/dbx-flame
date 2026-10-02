"""Platform policies: the contracts the framework itself depends on.

Unlike the DQX ruleset (user policies), these aren't configurable: each protects
something a watermark, merge or history relies on, so a task can't switch them off.
"""

from __future__ import annotations

from enum import StrEnum, unique
from typing import TYPE_CHECKING, NoReturn

if TYPE_CHECKING:
    from dbx_flame.context.context import Context


@unique  # two policies sharing an audit name would be indistinguishable
class PlatformPolicy(StrEnum):
    """Every platform policy, named as the audit row a violation writes."""

    UNSTAMPED_FILE = "unstamped_file"  # a file name carries the agreed export stamp
    ANCHOR_COLUMN_MISSING = "anchor_column_missing"  # source.anchor_dt names a real column
    ANCHOR_UNPARSEABLE = "anchor_unparseable"  # source.anchor_dt parses as a timestamp
    ANCHOR_NOT_STAMPED = "anchor_not_stamped"  # an anchored read finds __ANCHOR_DT
    CAST_SILENT_NULL = "cast_silent_null"  # a cast never turns a value into NULL
    EMPTY_SOURCE_SCHEMA = "empty_source_schema"  # a new target has columns to build from
    UNEXPECTED_COLUMNS = "unexpected_columns"  # new columns only under add_new_columns*


class PlatformPolicyViolation(Exception):
    """Raised when a batch breaks a platform policy; nothing from it is written."""

    def __init__(self, policy: PlatformPolicy, message: str):
        self.policy = policy
        super().__init__(f"{policy.value}: {message}")


def violate(
    ctx: Context,
    policy: PlatformPolicy,
    source: str,
    message: str,
    total: int = 0,
    metadata: str | None = None,
) -> NoReturn:
    """Log the violation, commit it to the audit table, then fail.

    Flushed before raising so the reason survives even if the run dies before its own
    final flush.
    """
    ctx.logger.error(
        name=policy.value, source=source, description=message, total=total, metadata=metadata
    )
    ctx.logger.flush()
    raise PlatformPolicyViolation(policy, message)
