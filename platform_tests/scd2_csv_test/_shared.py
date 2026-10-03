"""Constants and assertions shared by the scd2_csv_test steps.

Deliberately dependency-free: these run as spark_python_task on a cluster, not
under pytest, so a failed assert is what fails the task.
"""

import sys

# Every step is called with [base, catalog, env, ...]; the framework resolves the same pair.
CATALOG = f"{sys.argv[2]}_{sys.argv[3]}"
SCHEMA = "functional_testing"

BRONZE_TABLE = "SCD2_CSV_BRONZE_1"
SILVER_TABLE = "SCD2_CSV_SILVER_1"

VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/source_data"
INBOUND = f"{VOLUME}/inbound"
METADATA = f"{VOLUME}/metadata"
SOURCE_DIRECTORY = "SCD2_CSV_SOURCE_1"

# The source's own event time, as a string in a format that sorts wrong
# lexically — a misparsed format produces NULL start dates rather than a subtly
# wrong order, and the day is past the 12th so a swapped pattern cannot parse.
EVENT_COLUMN = "UPDATE_DATE"
EVENT_FORMAT = "MM/dd/yyyy HH:mm:ss"

BUSINESS_COLUMNS = {"ID", "NAME", "SOURCE_SYSTEM", EVENT_COLUMN}

BRONZE_COLUMNS = BUSINESS_COLUMNS | {
    "__BRONZE_LAST_MODIFIED_DT",
    "__FILEPATH",
    "__EXPORT_DATE",
}

SILVER_COLUMNS = BUSINESS_COLUMNS | {
    "__FILEPATH",
    "__EXPORT_DATE",
    "__SILVER_LAST_MODIFIED_DT",
    "__START_DATE",
    "__END_DATE",
    "__CURRENT_FLAG",
    "__DELETED_FLAG",
}

# Round 1 establishes three records; round 2 updates one of them and adds two more.
FIRST_EXPORT = [
    "1,Alice,Source_A,01/15/2026 10:00:00",
    "2,Bob,Source_A,01/15/2026 10:00:00",
    "3,Charlie,Source_A,01/15/2026 10:00:00",
]
SECOND_EXPORT = [
    "1,Alice,Source_B,01/16/2026 09:30:00",
    "4,David,Source_B,01/16/2026 09:30:00",
    "5,Elise,Source_B,01/16/2026 09:30:00",
]
EXPORTS = {"1": FIRST_EXPORT, "2": SECOND_EXPORT}

HEADER = "ID,NAME,SOURCE_SYSTEM,UPDATE_DATE"

FIRST_EVENT = "2026-01-15 10:00:00"
SECOND_EVENT = "2026-01-16 09:30:00"


def qualified(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def expect_columns(df, expected: set) -> None:
    actual = set(df.columns)
    assert (
        actual == expected
    ), f"columns differ: missing={expected - actual} extra={actual - expected}"


def expect_rows(df, count: int) -> None:
    actual = df.count()
    assert actual == count, f"expected {count} rows, found {actual}"


def expect_value_count(df, column: str, value: str, count: int) -> None:
    actual = df.filter(df[column] == value).count()
    assert actual == count, f"expected {count} rows with {column}={value}, found {actual}"


def version(df, key: str, source_system: str):
    """The single history row for one key as written by one export."""
    rows = df.filter((df["ID"] == key) & (df["SOURCE_SYSTEM"] == source_system)).collect()
    assert len(rows) == 1, f"expected one {key}/{source_system} row, found {len(rows)}"
    return rows[0]


def expect_window(row, start: str, end, current: str) -> None:
    """Assert a history row's validity window, comparing timestamps as strings."""
    actual_start = str(row["__START_DATE"])
    actual_end = None if row["__END_DATE"] is None else str(row["__END_DATE"])
    assert actual_start == start, f"expected __START_DATE {start}, found {actual_start}"
    assert actual_end == end, f"expected __END_DATE {end}, found {actual_end}"
    assert (
        row["__CURRENT_FLAG"] == current
    ), f"expected __CURRENT_FLAG {current}, found {row['__CURRENT_FLAG']}"
    assert row["__DELETED_FLAG"] == "N", "nothing in this test is deleted"


def expect_stats_columns(spark, table: str, *required: str) -> None:
    """The table keeps data-skipping statistics on the columns the framework filters on."""
    rows = spark.sql(f"SHOW TBLPROPERTIES {qualified(table)}").collect()
    listed = {row.key: row.value for row in rows}.get("delta.dataSkippingStatsColumns", "")
    missing = [column for column in required if column not in listed.split(",")]
    assert not missing, f"{table}: delta.dataSkippingStatsColumns lacks {missing} ({listed!r})"
