"""Step 3 — the first load opens a history for every record, none closed yet."""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    FIRST_EVENT,
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

# Promotion drops the bronze timestamp; SCD2 adds the four history columns.
expect_columns(df, SILVER_COLUMNS)

expect_rows(df, 3)
assert df.filter(df["__CURRENT_FLAG"] == "Y").count() == 3, "every first-load row is current"
assert df.filter(df["__END_DATE"].isNotNull()).count() == 0, "nothing should be closed yet"

# __START_DATE comes from UPDATE_DATE parsed with its configured format, not from
# the moment the job ran. A wrong format would land NULL here.
for key in ("1", "2", "3"):
    expect_window(version(df, key, "Source_A"), start=FIRST_EVENT, end=None, current="Y")

assert df.filter(df["__SILVER_LAST_MODIFIED_DT"].isNull()).count() == 0, "write time missing"

print("silver validated")
