"""Constants and assertions shared by the complete_delta_csv_test steps.

Deliberately dependency-free: these run as spark_python_task on a cluster, not
under pytest, so a failed assert is what fails the task.
"""

CATALOG = "testing_dev_01"
SCHEMA = "functional_testing"

BRONZE_TABLE = "COMPLETE_DELTA_CSV_BRONZE_1"
SILVER_TABLE = "COMPLETE_DELTA_CSV_SILVER_1"

VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/source_data"
INBOUND = f"{VOLUME}/inbound"
METADATA = f"{VOLUME}/metadata"
SOURCE_DIRECTORY = "COMPLETE_DELTA_CSV_SOURCE_1"

# The source's own event time. Day-first, and every day is past the 12th, so a
# pattern read the American way round cannot parse it into a plausible date — it
# lands NULL and the window assertions fail loudly.
EVENT_COLUMN = "CHANGE_TS"
EVENT_FORMAT = "dd-MM-yyyy HH:mm:ss"

BUSINESS_COLUMNS = {"ID", "NAME", "PAYLOAD", EVENT_COLUMN}

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

HEADER = "ID,NAME,PAYLOAD,CHANGE_TS"

# Round 1 is a backlog of three exports that all land before silver runs once.
# A changes in every one of them; B is written once and never touched again.
ROUND_ONE = [
    ["A,Anna,v1,15-01-2026 08:00:00", "B,Ben,v1,15-01-2026 08:00:00"],
    ["A,Anna,v2,16-01-2026 08:00:00"],
    ["A,Anna,v3,17-01-2026 08:00:00"],
]
# Round 2 is a single later export, to prove the watermark cuts at what silver holds.
ROUND_TWO = [
    ["A,Anna,v4,18-01-2026 08:00:00", "C,Cara,v1,18-01-2026 08:00:00"],
]
ROUNDS = {"1": ROUND_ONE, "2": ROUND_TWO}

EVENT_1 = "2026-01-15 08:00:00"
EVENT_2 = "2026-01-16 08:00:00"
EVENT_3 = "2026-01-17 08:00:00"
EVENT_4 = "2026-01-18 08:00:00"


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


def version(df, key: str, payload: str):
    """The single history row for one key at one version."""
    rows = df.filter((df["ID"] == key) & (df["PAYLOAD"] == payload)).collect()
    assert len(rows) == 1, f"expected one {key}/{payload} row, found {len(rows)}"
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
