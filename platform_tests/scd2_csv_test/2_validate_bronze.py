"""Step 2 — bronze holds the first export verbatim, with provenance attached."""

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
expect_rows(df, 3)

# Provenance is derived from the file name, not invented at write time.
assert df.filter(df["__EXPORT_DATE"].isNull()).count() == 0, "__EXPORT_DATE was not parsed"

# The event time arrives as an unparsed string: bronze keeps the source's format.
assert dict(df.dtypes)["UPDATE_DATE"] == "string", "bronze should not have typed UPDATE_DATE"

print("bronze validated")
