"""Step 3 — silver, in the consumer's catalog, holds bronze's rows; its checkpoint too.

Takes the number of rows silver should hold, since round 2 runs it again: only the
new export may arrive, which proves the checkpoint in the consumer's catalog resumed
the read instead of starting over.
"""

import os
import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    COLUMNS,
    SILVER_METADATA,
    bronze,
    expect_columns,
    expect_file_provenance,
    expect_rows,
    silver,
)
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()
expected = int(sys.argv[5])

assert spark.catalog.tableExists(silver()), "silver table was not created"
df = spark.table(silver())

# APPEND carries bronze's columns through untouched, its timestamp included.
expect_columns(df, COLUMNS)
expect_rows(df, expected)

# Every row came across exactly once.
missing = spark.table(bronze()).exceptAll(df)
assert missing.isEmpty(), f"rows in bronze never reached silver: {missing.collect()}"

# Promotion never re-stamps provenance: each Silver row still names the file it came from.
expect_file_provenance(df)

checkpoint = f"{SILVER_METADATA}/_checkpoint"
assert os.path.isdir(
    checkpoint
), f"silver's checkpoint is not in the consumer's catalog: {checkpoint}"

print("silver validated")
