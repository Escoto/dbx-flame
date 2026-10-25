"""Export entry point: a schema's tables and views → one CSV each in the preparation path."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import sys
import tempfile
import zipfile
from collections import Counter
from datetime import datetime
from typing import TYPE_CHECKING, NamedTuple, Optional
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from dbx_flame.context.config import ExportTaskConfig, Package
from dbx_flame.context.loader import load_config
from dbx_flame.entrypoints.run import LOCAL, _parse_argv, _run_identity
from dbx_flame.mechanics.columns import is_framework_column
from dbx_flame.observability.audit_logger import AuditLogger
from dbx_flame.observability.kpi import Kpi
from dbx_flame.output.mechanics import table_version
from dbx_flame.policies.platform import PlatformPolicy, PlatformPolicyViolation, violate

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.dbutils import DBUtils
    from pyspark.sql import DataFrame

    from dbx_flame.context.config import ChecksumAlgorithm, ExportConfig

_SOURCE = "export"

# RFC 4180: a quote inside a value is doubled. Spark's writer trims surrounding whitespace
# by default, which would alter the data, so both trims are switched off.
_CSV_OPTIONS = {
    "header": True,
    "escape": '"',
    "ignoreLeadingWhiteSpace": False,
    "ignoreTrailingWhiteSpace": False,
}

# The table types a Delta version pins. Views, materialized views and foreign tables
# have no time travel, so they are exported as read.
_VERSIONED = frozenset({"MANAGED", "EXTERNAL"})

_MANIFEST_HEADER = (
    "object",
    "object_type",
    "version",
    "row_count",
    "export_file_name",
    "checksum",
)

_CHUNK = 8 * 1024 * 1024


class _Planned(NamedTuple):
    frame: DataFrame
    object_type: Optional[str]
    # The Delta version the frame is pinned to; None when the object has no time travel.
    version: Optional[int]


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


def _object_types(spark: SparkSession, schema: str) -> dict[str, tuple[str, str]]:
    """Each object's Unity Catalog table type and storage format, by lowercase name."""
    catalog, _, name = schema.partition(".")
    rows = (
        spark.table(f"{catalog}.information_schema.tables")
        .where(F.lower("table_schema") == name.strip("`").lower())
        .select("table_name", "table_type", "data_source_format")
        .collect()
    )
    return {row.table_name.lower(): (row.table_type, row.data_source_format) for row in rows}


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


def _read(spark: SparkSession, source: str, kind: tuple[str, str]) -> _Planned:
    """The object as exported: a Delta table pinned to its current version, else as read."""
    object_type, source_format = kind
    if object_type in _VERSIONED and source_format == "DELTA":
        version = table_version(spark, source)
        return _Planned(
            spark.read.option("versionAsOf", version).table(source), object_type, version
        )
    return _Planned(spark.table(source), object_type, None)


def _plan(
    spark: SparkSession,
    schema: str,
    config: ExportConfig,
    logger_for: Callable[[str], AuditLogger],
) -> tuple[dict[str, _Planned], list[str]]:
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

    types = _object_types(spark, schema)
    planned: dict[str, _Planned] = {}
    unreadable: list[str] = []
    for name in exported:
        source = _source(schema, name)
        try:
            read = _read(spark, source, types.get(name.lower(), (None, None)))
        except Exception as exc:
            # A broken view is the data engineers' to fix; the other objects still export.
            logger = logger_for(source)
            _log_failure(logger, exc)
            logger.flush()
            unreadable.append(name)
            continue
        kept, object_problems = _shape(name, read.frame.columns, config)
        problems.extend(object_problems)
        shaped = read.frame.select(*[F.col(f"`{old}`").alias(new) for old, new in kept.items()])
        planned[name] = read._replace(frame=shaped)

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

    return planned, unreadable


def _write_csv(frame: DataFrame, path: str) -> None:
    """Write one object to a single CSV at path."""
    staging = f"{path}_staging"
    # repartition, not coalesce: coalesce(1) would also run the whole read in one task.
    frame.repartition(1).write.options(**_CSV_OPTIONS).csv(staging)
    [part] = [name for name in os.listdir(staging) if name.startswith("part-")]
    shutil.move(os.path.join(staging, part), path)
    shutil.rmtree(staging)


def _checksum(path: str, algorithm: ChecksumAlgorithm) -> tuple[str, int]:
    """Write the file's hash beside it; return the hash and the records the file holds.

    Both come from the one read of the file the hash needs anyway. Spark writes LF and
    doubles a quote inside a value, so a line break ends a record exactly when an even
    number of quotes precede it.
    """
    digest = hashlib.new(algorithm.value)
    breaks, quoted = 0, False
    with open(path, "rb") as handle:
        while chunk := handle.read(_CHUNK):
            digest.update(chunk)
            for i, part in enumerate(chunk.split(b'"')):
                if i:
                    quoted = not quoted
                if not quoted:
                    breaks += part.count(b"\n")

    checksum = digest.hexdigest()
    # The bare hash, as md5sum and its siblings print it: no file name, no line break.
    with open(f"{os.path.splitext(path)[0]}.{algorithm.value}", "w", encoding="ascii") as out:
        out.write(checksum)
    return checksum, breaks - 1  # the header


def _write_manifest(folder: str, file_name: str, rows: list[tuple], config: ExportConfig) -> None:
    path = os.path.join(folder, file_name)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(_MANIFEST_HEADER)
        writer.writerows(rows)
    _checksum(path, config.checksum)


def _get_dbutils(spark: SparkSession) -> DBUtils:
    """dbutils, created on first use: it only exists on a Databricks cluster."""
    from pyspark.dbutils import DBUtils

    return DBUtils(spark)


def _deliver(
    spark: SparkSession,
    folder: str,
    files: list[str],
    manifest: str,
    config: ExportConfig,
    stamp: str,
    logger: AuditLogger,
) -> None:
    """Copy the files, or their zip, to the destination; the file that marks it complete last.

    dbutils copies each object whole, so a consumer never reads a partial file.
    """
    algorithm = config.checksum.value
    with_checksums = [*files, *([manifest] if config.with_manifest else [])]
    names = [n for f in with_checksums for n in (f, f"{os.path.splitext(f)[0]}.{algorithm}")]

    if config.package == Package.ZIP:
        stem = f"{config.name}_{stamp}" if config.with_ts_zip else config.name
        # A Volume can't seek, which a zip needs to write its directory: build it on local
        # disk, then copy it in whole.
        with tempfile.TemporaryDirectory() as local:
            built = os.path.join(local, f"{stem}.zip")
            with zipfile.ZipFile(built, "w", zipfile.ZIP_DEFLATED) as z:
                for name in names:
                    z.write(os.path.join(folder, name), name)
            shutil.copyfile(built, os.path.join(folder, f"{stem}.zip"))
        names = [f"{stem}.zip"]
        if config.with_zip_checksum:
            _checksum(os.path.join(folder, f"{stem}.zip"), config.checksum)
            names.append(f"{stem}.{algorithm}")

    destination = config.destination or os.path.join(
        config.preparation_path, "deliveries", config.name
    )
    dbutils = _get_dbutils(spark)
    for name in names:
        dbutils.fs.cp(os.path.join(folder, name), os.path.join(destination, name))
    logger.info(
        name="delivery_complete",
        source=_SOURCE,
        description=f"{config.package.value.capitalize()} delivered to {destination}",
        total=len(names),
    )


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
    """Export each object to {preparation_path}/exports_temp/{name}/{OBJECT}.csv, then deliver.

    The name carries _{stamp} with export.with_ts_object_names. Settings that name a
    missing object or column fail the run before anything is written. Every object is
    attempted; the run fails afterwards if any of them did, and delivers nothing. Every
    file gets its checksum beside it, and the manifest lists the objects delivered.
    """
    planned, failed = _plan(spark, schema, config, logger_for)

    # Only this export's folder: the preparation path may hold other exports, even of
    # the same schema.
    folder = os.path.join(config.preparation_path, "exports_temp", config.name)
    if os.path.exists(folder):
        shutil.rmtree(folder)
    os.makedirs(folder)

    manifest: list[tuple] = []
    for name, read in planned.items():
        source = _source(schema, name)
        logger = logger_for(source)
        try:
            if read.version is None and read.object_type != "VIEW":
                logger.warning(
                    name="object_not_versioned",
                    source=_SOURCE,
                    description=f"{source} ({read.object_type}) has no Delta version to pin; "
                    "exported as read",
                )
            file_name = _file_name(name, stamp, config.with_ts_object_names)
            path = os.path.join(folder, file_name)
            _write_csv(read.frame, path)
            checksum, rows = _checksum(path, config.checksum)

            # Only a pinned table has a count to hold the file to: anything else may have
            # changed between two reads of it.
            expected = read.frame.count() if read.version is not None else rows
            if rows != expected:
                violate(
                    logger,
                    PlatformPolicy.EXPORT_ROW_COUNT_MISMATCH,
                    _SOURCE,
                    f"{file_name} holds {rows} rows; {source} holds {expected} "
                    f"at version {read.version}",
                    total=abs(rows - expected),
                )

            logger.kpi(Kpi.ROWS_EXPORTED, rows, description=f"{source} exported to {folder}")
            if config.skip_empty and not rows:
                logger.warning(
                    name="nothing_to_export",
                    source=_SOURCE,
                    description=f"{source} has no rows; left out of the delivery",
                )
                continue
            manifest.append(
                (name.upper(), read.object_type, read.version, rows, file_name, checksum)
            )
        except PlatformPolicyViolation:
            # violate() already wrote the audit row: no second object_export_failure.
            failed.append(name)
        except Exception as exc:
            _log_failure(logger, exc)
            failed.append(name)
        finally:
            logger.flush()

    manifest_name = f"{config.name}_MANIFEST_{stamp}.csv"
    _write_manifest(folder, manifest_name, manifest, config)

    if failed:
        raise RuntimeError(f"Export failed for {len(failed)} object(s): {', '.join(failed)}")

    logger = logger_for(schema)
    try:
        if manifest:
            files = [row[4] for row in manifest]
            _deliver(spark, folder, files, manifest_name, config, stamp, logger)
        else:
            logger.warning(
                name="nothing_to_export",
                source=_SOURCE,
                description=f"Every object of {schema} is empty; nothing delivered",
            )
    finally:
        logger.flush()


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
