"""Step 3 — bronze holds both exports verbatim, with provenance attached."""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    BRONZE_COLUMNS,
    BRONZE_TABLE,
    expect_columns,
    expect_rows,
    expect_value_count,
    qualified,
)
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()

assert spark.catalog.tableExists(qualified(BRONZE_TABLE)), "bronze table was not created"
df = spark.table(qualified(BRONZE_TABLE))

expect_columns(df, BRONZE_COLUMNS)

# APPEND keeps every row of both exports — 3 + 3.
expect_rows(df, 6)

# ID 1.0 is in both exports and bronze does not deduplicate.
expect_value_count(df, "ID", "1.0", 2)

# Provenance is derived from the file name, not invented at write time.
assert df.filter(df["__EXPORT_DATE"].isNull()).count() == 0, "__EXPORT_DATE was not parsed"
assert df.select("__EXPORT_DATE").distinct().count() == 2, "the two exports share a timestamp"

print("bronze validated")
