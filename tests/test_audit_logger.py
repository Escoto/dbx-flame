"""Tests for AuditLogger — buffered audit logging to Delta Lake."""

from __future__ import annotations

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dbx_flame.context.context import RunIdentity
from dbx_flame.observability.audit_logger import AuditLogger

TARGET_TABLE = "`cro_dev_01`.`silver_cro`.`SUBJECTS`"
RUN = RunIdentity(
    workflow_id="wf-9",
    workflow_run_id="wfrun-9",
    task_key="bronze_to_silver",
    task_run_id="taskrun-9",
)


def _make_logger(spark, audit_db, table_suffix="logs"):
    return AuditLogger(
        spark=spark,
        env="dev_01",
        target_table=TARGET_TABLE,
        run=RUN,
        _audit_table_override=f"{audit_db}.{table_suffix}",
    )


def test_buffer_accumulates_without_writing(spark, audit_db):
    logger = _make_logger(spark, audit_db)
    logger.info("step_a", "source_a", "desc_a")
    logger.info("step_b", "source_b", "desc_b")
    logger.info("step_c", "source_c", "desc_c")

    rows = spark.table(f"{audit_db}.logs").count()
    assert rows == 0


def test_flush_writes_all_entries(spark, audit_db):
    logger = _make_logger(spark, audit_db)
    logger.info("i_step", "src", "info entry")
    logger.warning("w_step", "src", "warn entry")
    logger.error("e_step", "src", "error entry")
    logger.flush()

    df = spark.table(f"{audit_db}.logs")
    assert df.count() == 3

    types = sorted(r["type"] for r in df.select("type").collect())
    assert types == ["ERROR", "INFO", "WARNING"]


def test_schema_matches_contract(spark, audit_db):
    logger = _make_logger(spark, audit_db)
    logger.info("schema_test", "src", "check schema")
    logger.flush()

    expected = StructType(
        [
            StructField("__uuid", StringType(), True),
            StructField("__workflow_id", StringType(), True),
            StructField("__workflow_run_id", StringType(), True),
            StructField("__task_key", StringType(), True),
            StructField("__task_run_id", StringType(), True),
            StructField("time_stamp", TimestampType(), True),
            StructField("type", StringType(), True),
            StructField("catalog", StringType(), True),
            StructField("schema", StringType(), True),
            StructField("table", StringType(), True),
            StructField("name", StringType(), True),
            StructField("source", StringType(), True),
            StructField("total", IntegerType(), True),
            StructField("description", StringType(), True),
            StructField("metadata", StringType(), True),
        ]
    )

    actual = spark.table(f"{audit_db}.logs").schema
    expected_names = [f.name for f in expected.fields]
    actual_names = [f.name for f in actual.fields]
    assert actual_names == expected_names

    for exp_field, act_field in zip(expected.fields, actual.fields):
        assert (
            act_field.dataType == exp_field.dataType
        ), f"Column {exp_field.name}: expected {exp_field.dataType}, got {act_field.dataType}"


def test_table_auto_creation(spark, audit_db):
    table_name = f"{audit_db}.auto_created"
    assert not spark.catalog.tableExists(table_name)

    AuditLogger(
        spark=spark,
        env="dev_01",
        target_table=TARGET_TABLE,
        run=RUN,
        _audit_table_override=table_name,
    )
    assert spark.catalog.tableExists(table_name)


def test_kpi_convention(spark, audit_db):
    logger = _make_logger(spark, audit_db)
    logger.kpi("bronze_new_records", 42)
    logger.flush()

    row = spark.table(f"{audit_db}.logs").collect()[0]
    assert row["source"] == "KPI"
    assert row["total"] == 42
    assert row["name"] == "bronze_new_records"
    assert row["type"] == "INFO"


def test_info_level(spark, audit_db):
    logger = _make_logger(spark, audit_db)
    logger.info("test_name", "test_src", "test_desc")
    logger.flush()

    row = spark.table(f"{audit_db}.logs").collect()[0]
    assert row["type"] == "INFO"


def test_warning_level(spark, audit_db):
    logger = _make_logger(spark, audit_db)
    logger.warning("test_name", "test_src", "test_desc")
    logger.flush()

    row = spark.table(f"{audit_db}.logs").collect()[0]
    assert row["type"] == "WARNING"


def test_error_level(spark, audit_db):
    logger = _make_logger(spark, audit_db)
    logger.error("test_name", "test_src", "test_desc")
    logger.flush()

    row = spark.table(f"{audit_db}.logs").collect()[0]
    assert row["type"] == "ERROR"


def test_multiple_flushes(spark, audit_db):
    logger = _make_logger(spark, audit_db)
    logger.info("first", "src", "desc")
    logger.info("second", "src", "desc")
    logger.flush()

    assert spark.table(f"{audit_db}.logs").count() == 2

    logger.warning("third", "src", "desc")
    logger.flush()

    assert spark.table(f"{audit_db}.logs").count() == 3


def test_flush_empty_buffer_noop(spark, audit_db):
    logger = _make_logger(spark, audit_db)
    logger.flush()

    assert spark.table(f"{audit_db}.logs").count() == 0


def test_parsed_target_table_columns(spark, audit_db):
    logger = _make_logger(spark, audit_db)
    logger.info("parse_test", "src", "desc")
    logger.flush()

    row = spark.table(f"{audit_db}.logs").collect()[0]
    assert row["catalog"] == "cro_dev_01"
    assert row["schema"] == "silver_cro"
    assert row["table"] == "SUBJECTS"


def test_table_partitioned_by_type_and_name(spark, audit_db):
    table_name = f"{audit_db}.partitioned_check"
    AuditLogger(
        spark=spark,
        env="dev_01",
        target_table=TARGET_TABLE,
        run=RUN,
        _audit_table_override=table_name,
    )

    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
    partitions = list(detail["partitionColumns"])
    assert partitions == ["type", "name"]


def test_metadata_field(spark, audit_db):
    logger = _make_logger(spark, audit_db)
    import json

    meta = json.dumps({"file": "/vol/agents/bad.csv", "error": "parse failed"})
    logger.error("ingest_failure", "CSVSource", "File parse error", metadata=meta)
    logger.flush()

    row = spark.table(f"{audit_db}.logs").collect()[0]
    assert row["metadata"] is not None
    parsed = json.loads(row["metadata"])
    assert parsed["file"] == "/vol/agents/bad.csv"


def test_metadata_null_by_default(spark, audit_db):
    logger = _make_logger(spark, audit_db)
    logger.info("test", "src", "no metadata")
    logger.flush()

    row = spark.table(f"{audit_db}.logs").collect()[0]
    assert row["metadata"] is None


def test_column_comments(spark, audit_db):
    table_name = f"{audit_db}.commented"
    AuditLogger(
        spark=spark,
        env="dev_01",
        target_table=TARGET_TABLE,
        run=RUN,
        _audit_table_override=table_name,
    )

    desc_rows = spark.sql(f"DESCRIBE TABLE {table_name}").collect()
    comments = {r["col_name"]: r["comment"] for r in desc_rows if r["comment"]}
    assert "__uuid" in comments
    assert "Unique identifier" in comments["__uuid"]
    assert "metadata" in comments
    assert "JSON" in comments["metadata"]


def test_identity_columns_populated(spark, audit_db):
    """All four traceability ids land on every row, not just the run-scoped pair."""
    logger = _make_logger(spark, audit_db)
    logger.info("identity_test", "src", "desc")
    logger.flush()

    row = spark.table(f"{audit_db}.logs").collect()[0]
    assert row["__workflow_id"] == "wf-9"
    assert row["__workflow_run_id"] == "wfrun-9"
    assert row["__task_key"] == "bronze_to_silver"
    assert row["__task_run_id"] == "taskrun-9"


def test_every_entry_is_echoed_to_the_console(spark, audit_db, capsys):
    """The driver log keeps the trail even when the flush to the audit table never happens."""
    logger = _make_logger(spark, audit_db)
    logger.info("step_a", "src", "first line\nsecond line")
    logger.kpi("rows_appended", total=3, description="AppendWriter wrote")

    printed = capsys.readouterr().out
    assert "INFO src.step_a: first line\nsecond line" in printed
    assert "INFO KPI.rows_appended (total=3): AppendWriter wrote" in printed
