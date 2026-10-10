"""Constants, fixtures and assertions shared by the append_table_delta_read_test steps.

Deliberately dependency-free: these run as spark_python_task on a cluster, not
under pytest, so a failed assert is what fails the task.
"""

import sys
from datetime import datetime

# Every step is called with [base, catalog, env, ...]; the framework resolves the
# catalog the same way.
CATALOG = f"{sys.argv[2]}_{sys.argv[3]}"
SCHEMA = "functional_testing"

# A plain table with none of our metadata: it stands in for a foreign (federated) one.
SOURCE_TABLE = "APPEND_TABLE_DELTA_READ_SOURCE_1"
BRONZE_TABLE = "APPEND_TABLE_DELTA_READ_BRONZE_1"
SILVER_TABLE = "APPEND_TABLE_DELTA_READ_SILVER_1"

METADATA = f"/Volumes/{CATALOG}/{SCHEMA}/source_data/metadata"
# Where the framework keeps each target's checkpoint: {metadata_path}/{catalog}/{schema}/{table}.
BRONZE_METADATA = f"{METADATA}/{CATALOG}/{SCHEMA}/{BRONZE_TABLE}"
SILVER_METADATA = f"{METADATA}/{CATALOG}/{SCHEMA}/{SILVER_TABLE}"

SOURCE_SCHEMA = "ID string, NAME string, STATUS string, MODIFIED_AT timestamp"

_DAY_1 = datetime(2026, 1, 1, 9, 0)
_DAY_2 = datetime(2026, 1, 2, 9, 0)

# What the source table holds in each round. Between them one row changes (1), one
# stays as it was (3), one is deleted (2) and one is added (4).
SOURCE_ROWS = {
    "1": [
        ("1", "Alice", "active", _DAY_1),
        ("2", "Bob", "active", _DAY_1),
        ("3", "Charlie", "active", _DAY_1),
    ],
    "2": [
        ("1", "Alice", "inactive", _DAY_2),
        ("3", "Charlie", "active", _DAY_1),
        ("4", "Dana", "active", _DAY_2),
    ],
}

# What each delta read hands on: everything the first time, then only the rows whose
# MODIFIED_AT moved. The unchanged row is not read again, and the delete is invisible.
CHANGED_ROWS = {
    "1": SOURCE_ROWS["1"],
    "2": [("1", "Alice", "inactive", _DAY_2), ("4", "Dana", "active", _DAY_2)],
}

# Silver (UPSERT on ID) after each round: the newest state of every key ever read.
# Bob stays: a delta read never sees a delete.
SILVER_ROWS = {
    "1": SOURCE_ROWS["1"],
    "2": [
        ("1", "Alice", "inactive", _DAY_2),
        ("2", "Bob", "active", _DAY_1),
        ("3", "Charlie", "active", _DAY_1),
        ("4", "Dana", "active", _DAY_2),
    ],
}

# Provenance a delta read stamps: the read's kind, then the table as Spark names it.
SOURCE_PREFIX = f"TABLE:{CATALOG}.{SCHEMA}.{SOURCE_TABLE}@"

DATA_COLUMNS = {"ID", "NAME", "STATUS", "MODIFIED_AT"}
PROVENANCE = {"__ANCHOR_DT", "__SOURCE", "__EXPORT_DATE"}
BRONZE_COLUMNS = DATA_COLUMNS | PROVENANCE | {"__BRONZE_LAST_MODIFIED_DT"}
SILVER_COLUMNS = DATA_COLUMNS | PROVENANCE | {"__SILVER_LAST_MODIFIED_DT"}


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


def expect_anchor_is_modified_at(df) -> None:
    """__ANCHOR_DT is the column the read followed, so the next read can watermark on it."""
    stray = df.filter(~(df["__ANCHOR_DT"] == df["MODIFIED_AT"])).first()
    assert stray is None, f"__ANCHOR_DT should equal MODIFIED_AT, found {stray}"


def data_rows(df) -> list:
    return sorted(tuple(row) for row in df.select("ID", "NAME", "STATUS", "MODIFIED_AT").collect())
