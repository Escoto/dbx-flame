"""Step 4 — a later export extends the history, and only that export is read.

COMPLETE_DELTA has no checkpoint. Its increment is the watermark: everything in
bronze newer than the newest __EXPORT_DATE silver already holds. If that cut were
wrong the first three snapshots would be replayed a second time, and A's history
would come back with duplicated or reopened windows.
"""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    BRONZE_TABLE,
    EVENT_1,
    EVENT_2,
    EVENT_3,
    EVENT_4,
    SILVER_TABLE,
    expect_rows,
    expect_window,
    qualified,
    version,
)
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()

bronze = spark.table(qualified(BRONZE_TABLE))
silver = spark.table(qualified(SILVER_TABLE))

# Bronze accumulated the fourth export: 4 + 2.
expect_rows(bronze, 6)
assert bronze.select("__EXPORT_DATE").distinct().count() == 4, "expected four exports in bronze"

# Two new history rows, not a re-replay of the backlog.
expect_rows(silver, 6)
assert silver.filter(silver["__CURRENT_FLAG"] == "Y").count() == 3, "A v4, B and C are current"

# The new version closes the one it supersedes.
expect_window(version(silver, "A", "v3"), start=EVENT_3, end=EVENT_4, current="N")
expect_window(version(silver, "A", "v4"), start=EVENT_4, end=None, current="Y")

# The windows the first run built are untouched — the watermark cut them out.
expect_window(version(silver, "A", "v1"), start=EVENT_1, end=EVENT_2, current="N")
expect_window(version(silver, "A", "v2"), start=EVENT_2, end=EVENT_3, current="N")
expect_window(version(silver, "B", "v1"), start=EVENT_1, end=None, current="Y")

# A key first seen in the fourth export opens its own history.
expect_window(version(silver, "C", "v1"), start=EVENT_4, end=None, current="Y")

assert silver.filter(silver["__DELETED_FLAG"] == "Y").count() == 0, "nothing was deleted"

print("silver watermark validated")
