"""Export entry point: a schema's tables and views → one CSV each in the preparation path."""

from __future__ import annotations

import json
import os
import shutil
import sys
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from pyspark.sql import SparkSession

from dbx_flame.context.config import ExportTaskConfig
from dbx_flame.context.loader import load_config
from dbx_flame.entrypoints.run import LOCAL, _parse_argv, _run_identity
from dbx_flame.observability.audit_logger import AuditLogger
from dbx_flame.observability.kpi import Kpi

if TYPE_CHECKING:
    from collections.abc import Callable

    from dbx_flame.context.config import ExportConfig

_SOURCE = "export"

# RFC 4180: a quote inside a value is doubled. Spark's writer trims surrounding whitespace
# by default, which would alter the data, so both trims are switched off.
_CSV_OPTIONS = {
    "header": True,
    "escape": '"',
    "ignoreLeadingWhiteSpace": False,
    "ignoreTrailingWhiteSpace": False,
}


def _objects(spark: SparkSession, schema: str, requested: list[str]) -> list[str]:
    """The schema's tables and views, or the requested ones, failing on any it lacks.

    Unity Catalog stores names in lowercase, so names match in any case.
    """
    listed = [
        row.tableName
        for row in spark.sql(f"SHOW TABLES IN {schema}").collect()
        if not row.isTemporary  # a session's temp views are not part of the schema
    ]
    if not listed:
        raise ValueError(f"{schema} has no tables or views to export")
    if not requested:
        return listed

    by_name = {name.lower(): name for name in listed}
    missing = [name for name in requested if name.lower() not in by_name]
    if missing:
        raise ValueError(f"export.tables names objects not in {schema}: {', '.join(missing)}")
    return [by_name[name.lower()] for name in requested]


def _write_csv(spark: SparkSession, source: str, path: str) -> int:
    """Write one object to a single CSV at path; return the rows the file holds."""
    staging = f"{path}_staging"
    # repartition, not coalesce: coalesce(1) would also run the whole read in one task.
    spark.table(source).repartition(1).write.options(**_CSV_OPTIONS).csv(staging)
    [part] = [name for name in os.listdir(staging) if name.startswith("part-")]
    shutil.move(os.path.join(staging, part), path)
    shutil.rmtree(staging)
    # Counted from the file, so the KPI reports what was exported, not a second read.
    return spark.read.options(**_CSV_OPTIONS).csv(path, multiLine=True).count()


def export_objects(
    spark: SparkSession,
    schema: str,
    config: ExportConfig,
    stamp: str,
    logger_for: Callable[[str], AuditLogger],
) -> None:
    """Export each object to {preparation_path}/{schema_name}/{object}_{stamp}.csv.

    Every object is attempted; the run fails afterwards if any of them did.
    """
    objects = _objects(spark, schema, config.tables)

    # Only this schema's folder: the preparation path may hold other schemas' exports.
    folder = os.path.join(config.preparation_path, config.schema_name)
    if os.path.exists(folder):
        shutil.rmtree(folder)
    os.makedirs(folder)

    failed: list[str] = []
    for name in objects:
        source = f"{schema}.`{name}`"
        logger = logger_for(source)
        try:
            rows = _write_csv(spark, source, os.path.join(folder, f"{name}_{stamp}.csv"))
            logger.kpi(Kpi.ROWS_EXPORTED, rows, description=f"{source} exported to {folder}")
        except Exception as exc:
            logger.error(name="object_export_failure", source=_SOURCE, description=str(exc))
            failed.append(name)
        finally:
            logger.flush()

    if failed:
        raise RuntimeError(f"Export failed for {len(failed)} object(s): {', '.join(failed)}")


def main() -> None:
    """Entry point for python_wheel_task. Reads named parameters and runs the export."""
    params = _parse_argv(sys.argv[1:])
    run, defaulted = _run_identity(params)
    config = load_config(params, ExportTaskConfig)

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    schema = f"`{config.catalog}_{config.env}`.`{config.export.schema_name}`"
    logger = AuditLogger(spark, config.env, schema, run)

    if defaulted:
        logger.warning(
            name="run_identity_missing",
            source=_SOURCE,
            description=(
                f"Run identity not supplied; defaulted to '{LOCAL}': " + ", ".join(defaulted)
            ),
        )

    # Taken once, in UTC, so every file of the run carries the same stamp.
    stamp = datetime.now(timezone.utc).strftime(config.export.stamp_format)

    # Unguarded, as in run.py: the first audit write proves the audit path before any work.
    logger.info(
        name="export_start",
        source=_SOURCE,
        description=f"Exporting {schema} with stamp {stamp}",
        metadata=json.dumps(params, sort_keys=True),
    )
    logger.flush()

    try:
        export_objects(
            spark,
            schema,
            config.export,
            stamp,
            lambda source: AuditLogger(spark, config.env, source, run),
        )
        logger.info(name="export_complete", source=_SOURCE, description=f"{schema} exported")
    except Exception as exc:
        logger.error(name="export_failure", source=_SOURCE, description=str(exc))
        raise
    finally:
        # in finally: a cancelled run raises BaseException, which skips `except Exception`.
        logger.flush()


if __name__ == "__main__":
    sys.exit(main() or 0)
