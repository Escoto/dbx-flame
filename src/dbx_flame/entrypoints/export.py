"""Export entry point: a schema's tables and views → one CSV each in the preparation path."""

from __future__ import annotations

import json
import os
import shutil
import sys
from collections import Counter
from datetime import datetime
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from dbx_flame.context.config import ExportTaskConfig
from dbx_flame.context.loader import load_config
from dbx_flame.entrypoints.run import LOCAL, _parse_argv, _run_identity
from dbx_flame.mechanics.columns import is_framework_column
from dbx_flame.observability.audit_logger import AuditLogger
from dbx_flame.observability.kpi import Kpi

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.sql import DataFrame

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


def _listed(spark: SparkSession, schema: str) -> list[str]:
    """The schema's tables and views, as the catalog names them."""
    listed = [
        row.tableName
        for row in spark.sql(f"SHOW TABLES IN {schema}").collect()
        if not row.isTemporary  # a session's temp views are not part of the schema
    ]
    if not listed:
        raise ValueError(f"{schema} has no tables or views to export")
    return listed


def _resolve(listed: list[str], names: list[str]) -> tuple[list[str], list[str]]:
    """names as the catalog lists them, and the ones it doesn't list.

    Unity Catalog stores names in lowercase, so names match in any case.
    """
    by_name = {name.lower(): name for name in listed}
    found = [by_name[name.lower()] for name in names if name.lower() in by_name]
    return found, [name for name in names if name.lower() not in by_name]


def _shape(
    name: str, columns: list[str], config: ExportConfig
) -> tuple[dict[str, str], list[str]]:
    """Each column the file keeps → its name there, and what the settings got wrong.

    Excludes apply before renames.
    """
    excludes = next((v for k, v in config.excludes.items() if k.lower() == name.lower()), [])
    renames = next((v for k, v in config.renames.items() if k.lower() == name.lower()), {})
    kept = {
        column: renames.get(column, column)
        for column in columns
        if column not in excludes and not (config.exclude_metadata and is_framework_column(column))
    }

    problems: list[str] = []
    unknown = [column for column in [*excludes, *renames] if column not in columns]
    if unknown:
        problems.append(f"{name} has no column(s): {', '.join(unknown)}")
    if not kept:
        problems.append(f"{name} would be exported without any column")
    clashing = sorted(new for new, count in Counter(kept.values()).items() if count > 1)
    if clashing:
        problems.append(f"{name} would hold more than one column named: {', '.join(clashing)}")
    return kept, problems


def _source(schema: str, name: str) -> str:
    return f"{schema}.`{name}`"


def _log_failure(logger: AuditLogger, exc: Exception) -> None:
    logger.error(name="object_export_failure", source=_SOURCE, description=str(exc))


def _plan(
    spark: SparkSession,
    schema: str,
    config: ExportConfig,
    logger_for: Callable[[str], AuditLogger],
) -> tuple[dict[str, DataFrame], list[str]]:
    """Each object to export, shaped as its file will hold it, and those that can't be read.

    Every name the settings use is checked before anything is written: a mistyped exclude
    would deliver the very columns it meant to hold back.
    """
    listed = _listed(spark, schema)
    problems: list[str] = []

    exported = listed
    if config.tables:
        exported, missing = _resolve(listed, config.tables)
        if missing:
            problems.append(f"export.tables names objects not in {schema}: {', '.join(missing)}")

    configured, missing = _resolve(listed, [*config.excludes, *config.renames])
    if missing:
        problems.append(
            f"export.excludes or export.renames name objects not in {schema}: "
            + ", ".join(missing)
        )

    frames: dict[str, DataFrame] = {}
    unreadable: list[str] = []
    for name in exported:
        source = _source(schema, name)
        try:
            frame = spark.table(source)
        except Exception as exc:
            # A broken view is the data engineers' to fix; the other objects still export.
            logger = logger_for(source)
            _log_failure(logger, exc)
            logger.flush()
            unreadable.append(name)
            continue
        kept, object_problems = _shape(name, frame.columns, config)
        problems.extend(object_problems)
        frames[name] = frame.select(*[F.col(f"`{old}`").alias(new) for old, new in kept.items()])

    if problems:
        raise ValueError("; ".join(problems))

    for name in sorted(set(configured) - set(exported)):
        logger = logger_for(_source(schema, name))
        logger.warning(
            name="export_settings_skipped",
            source=_SOURCE,
            description=f"{name} is not in export.tables; its excludes and renames are skipped",
        )
        logger.flush()

    return frames, unreadable


def _write_csv(spark: SparkSession, frame: DataFrame, path: str) -> int:
    """Write one object to a single CSV at path; return the rows the file holds."""
    staging = f"{path}_staging"
    # repartition, not coalesce: coalesce(1) would also run the whole read in one task.
    frame.repartition(1).write.options(**_CSV_OPTIONS).csv(staging)
    [part] = [name for name in os.listdir(staging) if name.startswith("part-")]
    shutil.move(os.path.join(staging, part), path)
    shutil.rmtree(staging)
    # Counted from the file, so the KPI reports what was exported, not a second read.
    return spark.read.options(**_CSV_OPTIONS).csv(path, multiLine=True).count()


def _file_name(name: str, stamp: str, with_stamp: bool) -> str:
    # Uppercase as our table names are; Unity Catalog hands them back in lowercase.
    stem = f"{name.upper()}_{stamp}" if with_stamp else name.upper()
    return f"{stem}.csv"


def export_objects(
    spark: SparkSession,
    schema: str,
    config: ExportConfig,
    stamp: str,
    logger_for: Callable[[str], AuditLogger],
) -> None:
    """Export each object to {preparation_path}/{schema_name}/{OBJECT}.csv.

    The name carries _{stamp} with export.with_ts_object_names. Settings that name a
    missing object or column fail the run before anything is written. Every object is
    attempted; the run fails afterwards if any of them did.
    """
    frames, failed = _plan(spark, schema, config, logger_for)

    # Only this schema's folder: the preparation path may hold other schemas' exports.
    folder = os.path.join(config.preparation_path, config.schema_name)
    if os.path.exists(folder):
        shutil.rmtree(folder)
    os.makedirs(folder)

    for name, frame in frames.items():
        source = _source(schema, name)
        logger = logger_for(source)
        try:
            path = os.path.join(folder, _file_name(name, stamp, config.with_ts_object_names))
            rows = _write_csv(spark, frame, path)
            logger.kpi(Kpi.ROWS_EXPORTED, rows, description=f"{source} exported to {folder}")
        except Exception as exc:
            _log_failure(logger, exc)
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

    # Taken once, so every name of the run carries the same stamp.
    now = datetime.now(ZoneInfo(config.export.timezone))
    stamp = now.strftime(config.export.stamp_format)

    # Unguarded, as in run.py: the first audit write proves the audit path before any work.
    logger.info(
        name="export_start",
        source=_SOURCE,
        description=f"Exporting {schema} as {config.export.name} with stamp {stamp}",
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
