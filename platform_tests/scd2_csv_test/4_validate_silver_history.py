"""Step 4 — the second export closes the version it supersedes and opens a new one.

This is the whole point of SCD2 and where it parts company with UPSERT: ID 1 is
updated rather than overwritten, so silver ends with six rows — five current and one
expired — instead of staying at five.
"""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    BRONZE_TABLE,
    FIRST_EVENT,
    SECOND_EVENT,
    SILVER_TABLE,
    expect_rows,
    expect_stats_columns,
    expect_window,
    qualified,
    version,
)
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()

bronze = spark.table(qualified(BRONZE_TABLE))
silver = spark.table(qualified(SILVER_TABLE))

# Bronze accumulated both exports: 3 + 3.
expect_rows(bronze, 6)

expect_rows(silver, 6)
assert silver.filter(silver["__CURRENT_FLAG"] == "Y").count() == 5, "expected 5 current rows"
assert silver.filter(silver["__CURRENT_FLAG"] == "N").count() == 1, "expected 1 expired row"

# ID 1's first version is closed at exactly the second version's event time — the
# windows meet, they do not overlap and they leave no gap.
expect_window(version(silver, "1", "Source_A"), start=FIRST_EVENT, end=SECOND_EVENT, current="N")
expect_window(version(silver, "1", "Source_B"), start=SECOND_EVENT, end=None, current="Y")

# Records the second export did not mention are untouched: snapshot_scope is delta,
# so absence means "no news", not "deleted".
for key in ("2", "3"):
    expect_window(version(silver, key, "Source_A"), start=FIRST_EVENT, end=None, current="Y")

# New keys open their own history.
for key in ("4", "5"):
    expect_window(version(silver, key, "Source_B"), start=SECOND_EVENT, end=None, current="Y")

# Soft deletion is a deletes-feed concern; nothing here should be flagged.
assert silver.filter(silver["__DELETED_FLAG"] == "Y").count() == 0, "nothing was deleted"

for table in (BRONZE_TABLE, SILVER_TABLE):
    expect_stats_columns(spark, table, "__EXPORT_DATE")

print("silver history validated")
