"""Constants and assertions shared by the full_txt_weekly_test steps.

Deliberately dependency-free: these run as spark_python_task on a cluster, not
under pytest, so a failed assert is what fails the task.

A provider sends a full snapshot as a pipe-delimited .txt every Monday at 05:00, and
the pipeline runs on Mondays at 10:00. Silver needs only the latest snapshot.
"""

import sys

# Every step is called with [base, catalog, env, ...]; the framework resolves the same pair.
CATALOG = f"{sys.argv[2]}_{sys.argv[3]}"
SCHEMA = "functional_testing"

BRONZE_TABLE = "SUPPLIER_TXT_BRONZE_1"
SILVER_TABLE = "SUPPLIER_TXT_SILVER_1"

VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/source_data"
INBOUND = f"{VOLUME}/inbound"
METADATA = f"{VOLUME}/metadata"
SOURCE_DIRECTORY = "SUPPLIER_TXT_SOURCE_1"

HEADER = ("SUPPLIER_ID", "SUPPLIER_NAME", "COUNTRY", "STATUS")
BUSINESS_COLUMNS = set(HEADER)

BRONZE_COLUMNS = BUSINESS_COLUMNS | {
    "__BRONZE_LAST_MODIFIED_DT",
    "__SOURCE",
    "__EXPORT_DATE",
}

SILVER_COLUMNS = BUSINESS_COLUMNS | {
    "__SOURCE",
    "__EXPORT_DATE",
    "__SILVER_LAST_MODIFIED_DT",
}

# The commas in the names only survive as one column if the file is read as pipe-delimited.
EXPORT_1 = {
    "file": "SUPPLIER_20261005050000.txt",  # Monday 1, 05:00
    "stamp": "2026-10-05 05:00:00",
    "rows": [
        ("1", "Acme, Inc.", "NL", "ACTIVE"),
        ("2", "Globex", "DE", "ACTIVE"),
        ("3", "Initech", "BE", "ACTIVE"),
    ],
}
EXPORT_2 = {
    "file": "SUPPLIER_20261012050000.txt",  # Monday 2, 05:00
    "stamp": "2026-10-12 05:00:00",
    "rows": [
        ("1", "Acme, Inc.", "NL", "ACTIVE"),
        ("2", "Globex Europe", "DE", "ACTIVE"),  # renamed
        ("3", "Initech", "BE", "SUSPENDED"),  # status change
        ("4", "Umbrella, Ltd.", "FR", "ACTIVE"),  # new
    ],
}
# The provider testing their side: lands between scheduled runs, so the next run's
# batch carries it together with Monday 3's export, which supersedes it.
EXPORT_3 = {
    "file": "SUPPLIER_20261014142300.txt",  # Wednesday 2, 14:23
    "stamp": "2026-10-14 14:23:00",
    "rows": [
        ("1", "Acme, Inc.", "NL", "ACTIVE"),
        ("99", "PROVIDER TEST", "XX", "TEST"),
    ],
}
# Upper-case extension on purpose: the glob must match .TXT as well as .txt.
EXPORT_4 = {
    "file": "SUPPLIER_20261019050000.TXT",  # Monday 3, 05:00
    "stamp": "2026-10-19 05:00:00",
    "rows": [
        ("1", "Acme, Inc.", "NL", "INACTIVE"),
        ("2", "Globex Europe", "DE", "ACTIVE"),
        ("4", "Umbrella, Ltd.", "FR", "ACTIVE"),
        ("5", "Hooli", "US", "ACTIVE"),  # new; 3 dropped out of the snapshot
    ],
}

# What lands in inbound before each scheduled run.
LANDED = {
    "1": [EXPORT_1],
    "2": [EXPORT_2],
    "3": [EXPORT_3, EXPORT_4],
}

# What each round's state should be after its run.
BRONZE_EXPORTS = {
    "1": [EXPORT_1],
    "2": [EXPORT_1, EXPORT_2],
    "3": [EXPORT_1, EXPORT_2, EXPORT_3, EXPORT_4],
}
SILVER_SEEN = {
    "1": [EXPORT_1],
    "2": [EXPORT_1, EXPORT_2],
    "3": [EXPORT_1, EXPORT_2, EXPORT_4],  # never EXPORT_3
}


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


def business_rows(df) -> set:
    return {tuple(row) for row in df.select(*HEADER).collect()}


def export_stamps(df) -> list[str]:
    return sorted(str(row[0]) for row in df.select("__EXPORT_DATE").distinct().collect())
