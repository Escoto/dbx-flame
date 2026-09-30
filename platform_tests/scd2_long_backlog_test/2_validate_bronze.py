"""Step 2 — bronze holds all three exports verbatim; APPEND keeps every row."""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    BRONZE_COLUMNS,
    BRONZE_TABLE,
    expect_columns,
    expect_rows,
    qualified,
)
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()

assert spark.catalog.tableExists(qualified(BRONZE_TABLE)), "bronze table was not created"
df = spark.table(qualified(BRONZE_TABLE))

expect_columns(df, BRONZE_COLUMNS)
expect_rows(df, 20 + 20 + 23)  # the three raw exports, unfiltered

# Provenance is derived from the file name, not invented at write time.
assert df.filter(df["__EXPORT_DATE"].isNull()).count() == 0, "__EXPORT_DATE was not parsed"

# Three distinct exports landed as three distinct file names, all in one backlog.
assert df.select("__EXPORT_DATE").distinct().count() == 3, "expected three distinct exports"

print("bronze validated")
