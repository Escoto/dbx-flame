"""Step 3 — one silver run replays three snapshots and rebuilds A's whole chain.

This is the assertion that separates COMPLETE_DELTA from SCD2. Both verbs saw the
identical bronze table; SCD2 would have deduplicated the backlog down to A v3 and
written a single current row. Here every intermediate version survives, with the
windows in the order the exports arrived rather than the order they were processed.
"""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    EVENT_1,
    EVENT_2,
    EVENT_3,
    SILVER_COLUMNS,
    SILVER_TABLE,
    expect_columns,
    expect_rows,
    expect_window,
    qualified,
    version,
)
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()

assert spark.catalog.tableExists(qualified(SILVER_TABLE)), "silver table was not created"
df = spark.table(qualified(SILVER_TABLE))

# Promotion drops the bronze timestamp; the history columns are added. Nothing
# internal to the replay — no snapshot id — is persisted.
expect_columns(df, SILVER_COLUMNS)

expect_rows(df, 4)
assert df.filter(df["__CURRENT_FLAG"] == "Y").count() == 2, "only A v3 and B should be current"

# A's full version chain: each window closes exactly where the next one opens.
expect_window(version(df, "A", "v1"), start=EVENT_1, end=EVENT_2, current="N")
expect_window(version(df, "A", "v2"), start=EVENT_2, end=EVENT_3, current="N")
expect_window(version(df, "A", "v3"), start=EVENT_3, end=None, current="Y")

# B was written by the first snapshot and never mentioned again — still open.
expect_window(version(df, "B", "v1"), start=EVENT_1, end=None, current="Y")

assert df.filter(df["__SILVER_LAST_MODIFIED_DT"].isNull()).count() == 0, "write time missing"

print("silver replay validated")
