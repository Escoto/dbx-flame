"""Step 2 — bronze, in the producer's catalog, holds every row of round 1."""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    COLUMNS,
    bronze,
    expect_columns,
    expect_file_provenance,
    expect_rows,
)
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()

assert spark.catalog.tableExists(bronze()), "bronze table was not created"
df = spark.table(bronze())

expect_columns(df, COLUMNS)

# APPEND keeps both exports whole: 3 + 3.
expect_rows(df, 6)

expect_file_provenance(df)

print("bronze validated")
