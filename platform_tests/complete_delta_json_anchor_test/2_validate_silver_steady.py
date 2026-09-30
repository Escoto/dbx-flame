"""Step 2 — silver after the pipeline's usual round: the full export plus one delta.

The delta task anchors on last_modified_dt, so the insert it carries opens at the
moment the record changed, not at the moment its export was cut.
"""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    BRONZE_DELTA_TABLE,
    BRONZE_FULL_TABLE,
    SILVER_TABLE,
    STEADY_INSERTED,
    STEADY_URIS,
    expect_anchor,
    expect_current,
    opened_at,
    qualified,
)
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()

assert spark.catalog.tableExists(qualified(SILVER_TABLE)), "silver table was not created"
df = spark.table(qualified(SILVER_TABLE))

expect_current(df, STEADY_URIS)

started = opened_at(df, STEADY_INSERTED)
assert started == "2025-11-15 09:00:00", f"{STEADY_INSERTED} opened at {started}"

for table in (BRONZE_FULL_TABLE, BRONZE_DELTA_TABLE, SILVER_TABLE):
    expect_anchor(spark.table(qualified(table)), table)

print("silver validated after the steady round")
