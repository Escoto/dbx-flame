"""Step 4 — silver after the delta feed: the resync's 18 addresses plus 022.

The delta task's anchor is the latest last_modified_dt the resync carries, so the Jan 2
and Jan 3 deltas, whose changes the resync already holds, are never written.
"""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    BRONZE_DELTA_TABLE,
    BRONZE_FULL_TABLE,
    LATE_INSERT,
    RESYNC_URIS,
    SILVER_TABLE,
    SUPERSEDED,
    expect_absent,
    expect_anchor,
    expect_current,
    expect_stats_columns,
    opened_at,
    qualified,
)
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()

df = spark.table(qualified(SILVER_TABLE))

expect_current(df, RESYNC_URIS | {LATE_INSERT})
expect_absent(df, SUPERSEDED)

started = opened_at(df, LATE_INSERT)
assert started == "2026-01-05 09:00:00", f"{LATE_INSERT} opened at {started}"

for table in (BRONZE_FULL_TABLE, BRONZE_DELTA_TABLE, SILVER_TABLE):
    expect_anchor(spark.table(qualified(table)), table)

for table in (BRONZE_FULL_TABLE, BRONZE_DELTA_TABLE, SILVER_TABLE):
    expect_stats_columns(spark, table, "__EXPORT_DATE", "__ANCHOR_DT")

print("silver validated after the delta feed")
