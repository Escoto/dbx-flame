"""KPI event names: the names dashboards query, kept stable."""

from __future__ import annotations

from enum import StrEnum, unique


@unique
class Kpi(StrEnum):
    """One event per verb, named after what it did rather than the layer it wrote.

    A verb can write any layer (FULL and SCD2 build Bronze from a federated source as
    readily as Silver), and the audit row's catalog, schema and table already say which.
    """

    # APPEND: rows added to the target as they arrived.
    ROWS_APPENDED = "rows_appended"
    # FULL: rows of the newest export that replaced the target.
    ROWS_OVERWRITTEN = "rows_overwritten"
    # UPSERT: rows inserted or refreshed in place, one per key.
    ROWS_UPSERTED = "rows_upserted"
    # SCD2 and COMPLETE_DELTA: new history versions opened.
    ROWS_HISTORIZED = "rows_historized"
    # COMPLETE_DELTA deletes feed: keys soft-deleted and their open version closed.
    ROWS_RETIRED = "rows_retired"
