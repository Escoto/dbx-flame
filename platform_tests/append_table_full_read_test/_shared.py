"""Constants, fixtures and assertions shared by the append_table_full_read_test steps.

Deliberately dependency-free: these run as spark_python_task on a cluster, not
under pytest, so a failed assert is what fails the task.
"""

import sys

# Every step is called with [base, catalog, env, ...]; the framework resolves the
# catalog the same way.
CATALOG = f"{sys.argv[2]}_{sys.argv[3]}"
SCHEMA = "functional_testing"

# A plain table with none of our metadata: it stands in for a foreign (federated) one.
SOURCE_TABLE = "APPEND_TABLE_FULL_READ_SOURCE_1"
BRONZE_TABLE = "APPEND_TABLE_FULL_READ_BRONZE_1"
SILVER_TABLE = "APPEND_TABLE_FULL_READ_SILVER_1"

METADATA = f"/Volumes/{CATALOG}/{SCHEMA}/source_data/metadata"
# Where the framework keeps each target's checkpoint: {metadata_path}/{catalog}/{schema}/{table}.
BRONZE_METADATA = f"{METADATA}/{CATALOG}/{SCHEMA}/{BRONZE_TABLE}"
SILVER_METADATA = f"{METADATA}/{CATALOG}/{SCHEMA}/{SILVER_TABLE}"

SOURCE_SCHEMA = "ID string, NAME string, STATUS string"

# What the source table holds in each round. Between them one row changes (1), one
# stays (3), one is deleted (2) and one is added (4): a full read sees all of it.
SOURCE_ROWS = {
    "1": [("1", "Alice", "active"), ("2", "Bob", "active"), ("3", "Charlie", "active")],
    "2": [("1", "Alice", "inactive"), ("3", "Charlie", "active"), ("4", "Dana", "active")],
}

# Provenance a full read stamps: the read's kind, then the table as Spark names it.
SOURCE_PREFIX = f"TABLE:{CATALOG}.{SCHEMA}.{SOURCE_TABLE}@"

DATA_COLUMNS = {"ID", "NAME", "STATUS"}
BRONZE_COLUMNS = DATA_COLUMNS | {"__BRONZE_LAST_MODIFIED_DT", "__SOURCE", "__EXPORT_DATE"}
SILVER_COLUMNS = DATA_COLUMNS | {"__SILVER_LAST_MODIFIED_DT", "__SOURCE", "__EXPORT_DATE"}


def source() -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{SOURCE_TABLE}`"


def bronze() -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{BRONZE_TABLE}`"


def silver() -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{SILVER_TABLE}`"


def expect_columns(df, expected: set) -> None:
    actual = set(df.columns)
    assert (
        actual == expected
    ), f"columns differ: missing={expected - actual} extra={actual - expected}"


def expect_table_provenance(df) -> None:
    stray = df.filter(~df["__SOURCE"].startswith(SOURCE_PREFIX)).select("__SOURCE").first()
    assert stray is None, f"__SOURCE should start with {SOURCE_PREFIX}, found {stray}"


def data_rows(df) -> list:
    return sorted(tuple(row) for row in df.select("ID", "NAME", "STATUS").collect())
