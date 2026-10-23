"""Framework columns: the rule that tells them apart from a source's own."""

from __future__ import annotations

FRAMEWORK_PREFIX = "__"


def is_framework_column(name: str) -> bool:
    """Whether the framework owns this column (__EXPORT_DATE, __SOURCE, ...), not the source."""
    return name.startswith(FRAMEWORK_PREFIX)
