"""Which exports a target already took: the record each step leaves, and the check a
watermark read makes against it (gh #84).

An export that changes nothing leaves no row behind, so the target's own watermark can't
pass it. Its export_processed record can.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from dbx_flame.pipelines.enrichment import EXPORT_DATE, SOURCE

if TYPE_CHECKING:
    from datetime import datetime

    from pyspark.sql import Column, DataFrame

    from dbx_flame.context.context import Context

EXPORT_PROCESSED = "export_processed"
EXPORT_SKIPPED = "export_skipped"


def exports_in(*frames: DataFrame | None) -> dict[datetime, list[str]]:
    """Each export the frames carry, with the sources it came from."""
    found: dict[datetime, set[str]] = {}
    for df in frames:
        if df is None:
            continue
        assert EXPORT_DATE in df.columns and SOURCE in df.columns  # stamped where data enters
        for export, source in df.select(EXPORT_DATE, SOURCE).distinct().collect():
            found.setdefault(export, set()).add(source)
    return {export: sorted(sources) for export, sources in found.items()}


def log_processed(
    ctx: Context, writer: str, version: int, exports: dict[datetime, list[str]]
) -> None:
    """Log one export_processed record per export a step took, changed rows or not.

    The record names the target by its Delta table id, so a dropped and recreated target
    starts with none, and carries the version after the step, so a restore can undo it.
    """
    if not exports:
        return

    table_id = _table_id(ctx)
    for export, sources in sorted(exports.items()):
        ctx.logger.info(
            name=EXPORT_PROCESSED,
            source=writer,
            total=version,
            description=f"export {export} processed into {ctx.target_table} (version {version})",
            metadata=json.dumps(
                {
                    "export_date": export.isoformat(),
                    "version": version,
                    "table_id": table_id,
                    "sources": sources,
                }
            ),
        )


def newest_promoted(ctx: Context, candidates: list[datetime]) -> datetime | None:
    """The newest candidate export the target already took, by its export_processed records.

    One audit query, filtered to the target and the candidates. A record that a restore
    has since undone doesn't count, so a rolled-back export is promoted again.
    """
    if not candidates or not ctx.spark.catalog.tableExists(ctx.target_table):
        return None

    by_stamp = {export.isoformat(): export for export in candidates}
    records = (
        ctx.logger.recorded(EXPORT_PROCESSED)
        .filter(
            (_field("table_id") == _table_id(ctx)) & _field("export_date").isin(list(by_stamp))
        )
        .select(_field("export_date"), _field("version").cast("long"))
        .collect()
    )
    restores = _restores(ctx)
    taken = [
        by_stamp[export]
        for export, version in records
        if not any(restored_to < version < at for restored_to, at in restores)
    ]
    return max(taken, default=None)


def _field(key: str) -> Column:
    return F.get_json_object(F.col("metadata"), f"$.{key}")


def _table_id(ctx: Context) -> str:
    return ctx.spark.sql(f"DESCRIBE DETAIL {ctx.target_table}").collect()[0]["id"]


def _restores(ctx: Context) -> list[tuple[int, int]]:
    """(version restored to, version of the restore) for every RESTORE of the target.

    A restore by timestamp names no version, so it counts as one to before the first
    commit: every record older than it is distrusted, and its export is promoted again,
    which the record hash makes a no-op.
    """
    commits = (
        ctx.spark.sql(f"DESCRIBE HISTORY {ctx.target_table}")
        .filter(F.col("operation") == "RESTORE")
        .select("version", "operationParameters")
        .collect()
    )
    restores = []
    for at, parameters in commits:
        restored_to = (parameters or {}).get("version")
        restores.append((int(restored_to) if restored_to is not None else -1, at))
    return restores
