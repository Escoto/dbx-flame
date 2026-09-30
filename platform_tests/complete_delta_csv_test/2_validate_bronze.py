"""Step 2 — bronze holds the whole backlog, three exports kept apart."""

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

# Two rows in the first export, one in each of the other two.
expect_rows(df, 4)

# A appears once per export: the backlog silver has to replay rather than collapse.
assert df.filter(df["ID"] == "A").count() == 3, "A should be present in all three exports"

# Three distinct exports, which is what the snapshot split will key on.
assert df.filter(df["__EXPORT_DATE"].isNull()).count() == 0, "__EXPORT_DATE was not parsed"
assert df.select("__EXPORT_DATE").distinct().count() == 3, "the exports share a timestamp"
assert df.filter(df["__FILEPATH"].isNull()).count() == 0, "__FILEPATH is required to split"

# The event time arrives as an unparsed string: bronze keeps the source's format.
assert dict(df.dtypes)["CHANGE_TS"] == "string", "bronze should not have typed CHANGE_TS"

print("bronze validated")
