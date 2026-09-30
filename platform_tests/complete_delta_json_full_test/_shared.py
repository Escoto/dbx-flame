"""Constants and assertions shared by the complete_delta_json_full_test steps.

Deliberately dependency-free: these run as spark_python_task on a cluster, not
under pytest, so a failed assert is what fails the task.

The two exports carry deliberately different payload shapes. No schema hints are
configured, so each feed's reader infers its own item schema — and the point of the
test is that the tables come out the same shape anyway, because record_envelope
projects to URI/STATUS/DATA/EXPORT_DATE whatever the payload does.
"""

import json

CATALOG = "testing_dev_01"
SCHEMA = "functional_testing"

# Two inbound feeds, two bronze tables, one silver — the production shape. The full
# export re-asserts the whole dataset; the delta export carries only what moved.
BRONZE_FULL_TABLE = "ADDRESS_JSON_BRONZE_FULL_1"
BRONZE_DELTA_TABLE = "ADDRESS_JSON_BRONZE_DELTA_1"
SILVER_TABLE = "ADDRESS_JSON_SILVER_1"

VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/source_data"
INBOUND = f"{VOLUME}/inbound"
METADATA = f"{VOLUME}/metadata"

FULL_DIRECTORY = "ADDRESS_JSON_FULL_SOURCE_1"
DELTA_DIRECTORY = "ADDRESS_JSON_DELTA_SOURCE_1"

# The two exports as committed under sample_data/. The 14-digit stamp in each name is
# what __EXPORT_DATE and the snapshot split are cut from.
FULL_EXPORT = "ADDRESS_FULL_20260101050000Z.json"
DELTA_EXPORT = "ADDRESS_DELTA_20260102050000Z.json"

FULL_EVENT = "2026-01-01 05:00:00"
DELTA_EVENT = "2026-01-02 05:00:00"

# Lifted out of each data[] item by source.envelope_fields; everything else stays
# inside DATA as JSON text. This set must not move when the payload does.
BUSINESS_COLUMNS = {"URI", "STATUS", "DATA", "EXPORT_DATE"}

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

# The address present in both feeds — the only one that gets a history.
MOVED = "https://r.testing.uri/000/0"
MOVED_ADDRESS_BEFORE = "12 American Road."
MOVED_ADDRESS_AFTER = "12th American Road."

# Only in the full export, so the delta feed never revisits them.
UNTOUCHED = ["https://r.testing.com/111/1", "https://r.testing.com/222/2"]

# Only in the delta export.
ADDED = ["https://r.testing.com/333/3", "https://r.testing.com/444/4"]

# The item that drifts hardest: it carries a field no other record has ("Type") and
# omits one every other record has ("related_cro").
DRIFTED = "https://r.testing.com/444/4"
DRIFT_FIELD = "Type"
DRIFT_VALUE = "TEST"

# In the full export this address has a city but no region; 111/1 has neither.
MUNICH = "https://r.testing.com/222/2"
NO_CITY = "https://r.testing.com/111/1"

# The whole full export predates "region"; every delta item carries it.
DRIFTED_FIELD_ABSENT_IN_FULL = "region"


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


def current(df):
    """Only the live version of each address."""
    return df.filter(df["__CURRENT_FLAG"] == "Y")


def version(df, uri: str, status: str):
    """The single history row for one address as written by one export."""
    rows = df.filter((df["URI"] == uri) & (df["STATUS"] == status)).collect()
    assert len(rows) == 1, f"expected one {uri}/{status} row, found {len(rows)}"
    return rows[0]


def payload(df, uri: str, status: str) -> dict:
    """The DATA column of one row, parsed back out of its JSON text."""
    return json.loads(version(df, uri, status)["DATA"])


def expect_window(row, start: str, end, flag: str) -> None:
    """Assert a history row's validity window, comparing timestamps as strings."""
    actual_start = str(row["__START_DATE"])
    actual_end = None if row["__END_DATE"] is None else str(row["__END_DATE"])
    assert actual_start == start, f"expected __START_DATE {start}, found {actual_start}"
    assert actual_end == end, f"expected __END_DATE {end}, found {actual_end}"
    assert row["__CURRENT_FLAG"] == flag, f"expected __CURRENT_FLAG {flag}"
    assert row["__DELETED_FLAG"] == "N", "nothing in this test is deleted"
