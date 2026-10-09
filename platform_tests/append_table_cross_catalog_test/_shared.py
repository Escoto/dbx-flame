"""Constants and assertions shared by the append_table_cross_catalog_test steps.

Deliberately dependency-free: these run as spark_python_task on a cluster, not
under pytest, so a failed assert is what fails the task.
"""

import sys

# Every step is called with [base, catalog, env, consumer_catalog, ...]; the framework
# resolves each pair the same way. Bronze lives in the producer's catalog, Silver and
# the checkpoints that track its reads in the consumer's.
CATALOG = f"{sys.argv[2]}_{sys.argv[3]}"
CONSUMER = f"{sys.argv[4]}_{sys.argv[3]}"
SCHEMA = "functional_testing"

BRONZE_TABLE = "APPEND_TABLE_CROSS_CATALOG_BRONZE_1"
SILVER_TABLE = "APPEND_TABLE_CROSS_CATALOG_SILVER_1"

VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/source_data"
INBOUND = f"{VOLUME}/inbound"
METADATA = f"{VOLUME}/metadata"
CONSUMER_METADATA = f"/Volumes/{CONSUMER}/{SCHEMA}/source_data/metadata"
SOURCE_DIRECTORY = "APPEND_TABLE_CROSS_CATALOG_SOURCE_1"

# Where the framework keeps Silver's checkpoint: {metadata_path}/{catalog}/{schema}/{table}.
SILVER_METADATA = f"{CONSUMER_METADATA}/{CONSUMER}/{SCHEMA}/{SILVER_TABLE}"
BRONZE_METADATA = f"{METADATA}/{CATALOG}/{SCHEMA}/{BRONZE_TABLE}"

COLUMNS = {
    "ID",
    "NAME",
    "SOURCE_SYSTEM",
    "__BRONZE_LAST_MODIFIED_DT",
    "__FILEPATH",
    "__EXPORT_DATE",
}


def bronze() -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{BRONZE_TABLE}`"


def silver() -> str:
    return f"`{CONSUMER}`.`{SCHEMA}`.`{SILVER_TABLE}`"


def expect_columns(df, expected: set) -> None:
    actual = set(df.columns)
    assert (
        actual == expected
    ), f"columns differ: missing={expected - actual} extra={actual - expected}"


def expect_rows(df, count: int) -> None:
    actual = df.count()
    assert actual == count, f"expected {count} rows, found {actual}"
