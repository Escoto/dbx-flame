"""Shared test fixtures — local SparkSession with Delta Lake support."""

from __future__ import annotations

import os
import sys
import uuid as _uuid

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create a local SparkSession with Delta Lake for testing."""
    builder = (
        SparkSession.builder.master("local[*]")
        .appName("dbx_flame_tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.warehouse.dir", "/tmp/dbx_flame_tests/warehouse")
        .config(
            "spark.driver.extraJavaOptions",
            "-Dderby.system.home=/tmp/dbx_flame_tests/derby",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


@pytest.fixture
def audit_db(spark):
    """Temporary database for audit logger tests."""
    db_name = f"test_audit_{_uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
    yield db_name
    spark.sql(f"DROP DATABASE IF EXISTS `{db_name}` CASCADE")


@pytest.fixture
def minimal_params() -> dict[str, str]:
    """Minimal valid flat params dict for load_config()."""
    return {
        "catalog": "cro",
        "env": "dev_01",
        "metadata_path": "/Volumes/meta/",
        "source.origin": "csv",
        "source.path": "/Volumes/inbound/",
        "source.directory": "agents",
        "output.verb": "append",
        "output.schema_name": "bronze_cro",
        "output.table": "AGENTS",
    }
