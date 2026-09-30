"""Step 3 — silver must hold exactly the last full export, nothing else.

FULL's contract (docs/03_write_verbs.md) is "overwrite the target with the current
dataset", and the newest export alone is the current dataset. This run's batch holds
all three backlogged exports (70 rows, duplicate KEYSEQs); only the third's 30 rows
may land.
"""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    BRONZE_TABLE,
    SILVER_COLUMNS,
    SILVER_EXPECTED,
    SILVER_TABLE,
    expect_columns,
    expect_rows,
    expect_stats_columns,
    qualified,
)
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

spark = SparkSession.builder.getOrCreate()

assert spark.catalog.tableExists(qualified(SILVER_TABLE)), "silver table was not created"
df = spark.table(qualified(SILVER_TABLE))

expect_columns(df, SILVER_COLUMNS)

# The headline assertion: only the third export's 30 records should survive.
expect_rows(df, SILVER_EXPECTED)

# Every KEYSEQ from the third export appears exactly once — no leftovers from the
# two superseded exports riding along in the same overwrite.
duplicated = df.groupBy("KEYSEQ").count().filter(F.col("count") > 1).select("KEYSEQ").collect()
assert not duplicated, f"KEYSEQ(s) appear more than once: {[r['KEYSEQ'] for r in duplicated]}"

# The second half's update from export 3 (Completed, not export 2's Pending) must win.
still_pending = df.filter((F.col("KEYSEQ") == "15") & (F.col("MILESTONEVALUE") == "Pending"))
assert still_pending.isEmpty(), "KEYSEQ 15 should be Completed per export 3, not export 1/2"

# The new inserts from export 3 must be present.
assert df.filter(F.col("KEYSEQ") == "25").count() == 1, "new record from export 3 is missing"

for table in (BRONZE_TABLE, SILVER_TABLE):
    expect_stats_columns(spark, table, "__EXPORT_DATE")

print("silver validated")
