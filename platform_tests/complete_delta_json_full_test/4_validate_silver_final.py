"""Step 4 — silver after the delta feed merges onto the full baseline.

Two feeds, two bronze tables, one silver: the address that moved carries both of its
versions, the addresses nobody touched are untouched, and the addresses that only
appeared in the delta feed are present.

The two feeds were read under different inferred schemas, so this is also where the
drift shows: one address ends up with two versions whose payloads have different
keys. Silver's own columns never move, which is the property the envelope buys.
"""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    ADDED,
    DELTA_EVENT,
    DRIFT_FIELD,
    DRIFT_VALUE,
    DRIFTED,
    DRIFTED_FIELD_ABSENT_IN_FULL,
    FULL_EVENT,
    MOVED,
    MOVED_ADDRESS_AFTER,
    MOVED_ADDRESS_BEFORE,
    SILVER_COLUMNS,
    SILVER_TABLE,
    UNTOUCHED,
    current,
    expect_columns,
    expect_rows,
    expect_window,
    payload,
    qualified,
    version,
)
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()

df = spark.table(qualified(SILVER_TABLE))

# Five addresses, six rows: the one that moved keeps its superseded version.
assert df.select("URI").distinct().count() == 5, "expected five distinct addresses"
expect_rows(df, 6)
expect_rows(current(df), 5)

# Both feeds wrote here and the schema never moved, though their payloads differ.
expect_columns(df, SILVER_COLUMNS)

# The moved address: the full export's version closed at the delta export's time, the
# delta export's version left open. This is the history a plain overwrite would lose.
expect_window(version(df, MOVED, "ACTIVE"), start=FULL_EVENT, end=DELTA_EVENT, flag="N")
expect_window(version(df, MOVED, "INACTIVE"), start=DELTA_EVENT, end=None, flag="Y")

# Each version keeps the payload of the export it came from, not a merged one. The
# closed version predates "region" and carries the old street address; the open one
# has both. Nothing back-fills a superseded row.
before = payload(df, MOVED, "ACTIVE")
after = payload(df, MOVED, "INACTIVE")
assert DRIFTED_FIELD_ABSENT_IN_FULL not in before, f"the closed version gained a region: {before}"
assert after[DRIFTED_FIELD_ABSENT_IN_FULL] == "Western Australia", f"region missing: {after}"
assert before["street_address"] == MOVED_ADDRESS_BEFORE
assert after["street_address"] == MOVED_ADDRESS_AFTER

# Addresses the delta feed never mentioned keep the window the full load opened, and
# keep the full export's payload shape with them.
for uri in UNTOUCHED:
    expect_window(version(df, uri, "ACTIVE"), start=FULL_EVENT, end=None, flag="Y")
    assert DRIFTED_FIELD_ABSENT_IN_FULL not in payload(df, uri, "ACTIVE"), f"{uri} gained a region"

# Addresses that exist only in the delta feed still land.
for uri in ADDED:
    expect_window(version(df, uri, "ACTIVE"), start=DELTA_EVENT, end=None, flag="Y")

# The hardest-drifting item survives the whole way to silver: a field no other record
# has, and no entry for one that every other record has.
drifted = payload(df, DRIFTED, "ACTIVE")
assert drifted[DRIFT_FIELD] == DRIFT_VALUE, f"{DRIFT_FIELD} did not reach silver: {drifted}"
assert "related_cro" not in drifted, f"{DRIFTED} should carry no related_cro: {drifted}"

print("silver validated after the delta merge")
