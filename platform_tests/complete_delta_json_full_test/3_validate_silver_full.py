"""Step 3 — silver after the FULL load alone, before the delta feed is merged.

The full load runs first and under snapshot_scope=full, which expires every current
row before re-asserting the snapshot. On an empty target that is a no-op, but the
ordering is the contract: the delta feed has to merge onto an established baseline.

This is also the baseline for the drift assertions in step 4 — nothing here has a
region, because the full export predates that field entirely.
"""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    ADDED,
    DRIFTED_FIELD_ABSENT_IN_FULL,
    FULL_EVENT,
    MOVED,
    MUNICH,
    NO_CITY,
    SILVER_COLUMNS,
    SILVER_TABLE,
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

assert spark.catalog.tableExists(qualified(SILVER_TABLE)), "silver table was not created"
df = spark.table(qualified(SILVER_TABLE))

expect_columns(df, SILVER_COLUMNS)

# Only the full export has landed: three addresses, every one of them current.
expect_rows(df, 3)
expect_rows(current(df), 3)

# The delta feed has not run yet, so neither of its new addresses may be here.
for uri in ADDED:
    assert df.filter(df["URI"] == uri).count() == 0, f"{uri} arrived out of order"

# Each address opens its window at its own export time, not at write time.
expect_window(version(df, MOVED, "ACTIVE"), start=FULL_EVENT, end=None, flag="Y")

# The baseline payload: this feed's schema has no region, and 111/1 has no city.
for uri in (MOVED, MUNICH, NO_CITY):
    assert DRIFTED_FIELD_ABSENT_IN_FULL not in payload(
        df, uri, "ACTIVE"
    ), f"{uri} gained a region the full export never carried"
assert "city" not in payload(df, NO_CITY, "ACTIVE"), "111/1 was never supposed to carry a city"

print("silver validated after the full load")
