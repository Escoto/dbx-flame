"""Buffered audit logger — writes to monitoring_{env}.audit.logs."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from dbx_flame.context.context import RunIdentity

_COLUMN_COMMENTS: dict[str, str] = {
    "__uuid": "Unique identifier for each log entry",
    "__workflow_id": "Databricks job (workflow) ID - stable across runs",
    "__workflow_run_id": "Databricks job run ID - one per workflow execution",
    "__task_key": "Task key within the workflow - stable across runs",
    "__task_run_id": "Databricks task run ID - one per task execution",
    "time_stamp": "Timestamp when the log entry was created",
    "type": "Log level: INFO, WARNING, or ERROR",
    "catalog": "Target catalog name parsed from the pipeline target table",
    "schema": "Target schema name parsed from the pipeline target table",
    "table": "Target table name parsed from the pipeline target table",
    "name": "Event name (e.g. bronze_new_records, pipeline_start, pipeline_failure)",
    "source": "Originating module or convention (e.g. KPI, run, CSVSource)",
    "total": "Numeric metric such as row count; 0 when not applicable",
    "description": "Human-readable message describing the event",
    "metadata": "Optional JSON string with structured context (e.g. failed file details, schema drift report)",
}


class AuditLogger:
    """Buffered logger that flushes per stage transition and on failure.

    Table contract: monitoring_{env}.audit.logs
    Schema: __uuid, __workflow_id, __workflow_run_id, __task_key, __task_run_id,
            time_stamp, type, catalog, schema, table, name, source, total,
            description, metadata
    Partitioned by: type, name
    """

    _SCHEMA = StructType(
        [
            StructField("__uuid", StringType(), False),
            StructField("__workflow_id", StringType(), False),
            StructField("__workflow_run_id", StringType(), False),
            StructField("__task_key", StringType(), False),
            StructField("__task_run_id", StringType(), False),
            StructField("time_stamp", TimestampType(), False),
            StructField("type", StringType(), False),
            StructField("catalog", StringType(), True),
            StructField("schema", StringType(), True),
            StructField("table", StringType(), True),
            StructField("name", StringType(), False),
            StructField("source", StringType(), False),
            StructField("total", IntegerType(), True),
            StructField("description", StringType(), True),
            StructField("metadata", StringType(), True),
        ]
    )

    def __init__(
        self,
        spark: SparkSession,
        env: str,
        target_table: str,
        run: RunIdentity,
        _audit_table_override: str | None = None,
    ):
        self._spark = spark
        self._env = env
        self._run = run
        self._buffer: list[tuple] = []

        clean = target_table.replace("`", "")
        parts = clean.split(".")
        self._catalog = parts[0] if len(parts) > 0 else None
        self._schema = parts[1] if len(parts) > 1 else None
        self._table = parts[2] if len(parts) > 2 else None

        self._audit_table_name = (
            _audit_table_override
            if _audit_table_override
            else f"`monitoring_{env}`.`audit`.`logs`"
        )

        self._ensure_table()

    def _ensure_table(self) -> None:
        if not self._spark.catalog.tableExists(self._audit_table_name):
            cols = []
            for field in self._SCHEMA.fields:
                nullable = "" if field.nullable else " NOT NULL"
                type_name = field.dataType.simpleString().upper()
                comment = _COLUMN_COMMENTS.get(field.name, "")
                comment_clause = f" COMMENT '{comment}'" if comment else ""
                cols.append(f"  `{field.name}` {type_name}{nullable}{comment_clause}")
            col_defs = ",\n".join(cols)
            ddl = (
                f"CREATE TABLE {self._audit_table_name} (\n{col_defs}\n)\n"
                f"USING DELTA\n"
                f"PARTITIONED BY (`type`, `name`)"
            )
            self._spark.sql(ddl)

    def _log(
        self,
        level: str,
        name: str,
        source: str,
        description: str,
        total: int,
        metadata: str | None,
    ) -> None:
        self._buffer.append(
            (
                str(uuid.uuid4()),
                self._run.workflow_id,
                self._run.workflow_run_id,
                self._run.task_key,
                self._run.task_run_id,
                datetime.now(),
                level,
                self._catalog,
                self._schema,
                self._table,
                name,
                source,
                total,
                description,
                metadata,
            )
        )

    def info(
        self,
        name: str,
        source: str,
        description: str,
        total: int = 0,
        metadata: str | None = None,
    ) -> None:
        self._log("INFO", name, source, description, total, metadata)

    def warning(
        self,
        name: str,
        source: str,
        description: str,
        total: int = 0,
        metadata: str | None = None,
    ) -> None:
        self._log("WARNING", name, source, description, total, metadata)

    def error(
        self,
        name: str,
        source: str,
        description: str,
        total: int = 0,
        metadata: str | None = None,
    ) -> None:
        self._log("ERROR", name, source, description, total, metadata)

    def kpi(self, name: str, total: int, description: str = "") -> None:
        self._log(
            "INFO", name=name, source="KPI", description=description, total=total, metadata=None
        )

    def flush(self) -> None:
        """Write all buffered entries to the audit table."""
        if not self._buffer:
            return
        df = self._spark.createDataFrame(self._buffer, self._SCHEMA)
        df.write.format("delta").mode("append").saveAsTable(self._audit_table_name)
        self._buffer.clear()
