"""Step 3 — Silver (UPSERT) holds the newest state of every key, with its own provenance.

Takes the round just run. In round 2 Silver mixes both reads: the changed and added
rows come from the second, the untouched ones still name the first. The deleted row
stays, the documented limit of a delta read.
"""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    SILVER_COLUMNS,
    SILVER_ROWS,
    bronze,
    data_rows,
    expect_anchor_is_modified_at,
    expect_columns,
    expect_table_provenance,
    silver,
)
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()
latest_round = sys.argv[4]

assert spark.catalog.tableExists(silver()), "silver table was not created"
df = spark.table(silver())

expect_columns(df, SILVER_COLUMNS)
expect_table_provenance(df)
expect_anchor_is_modified_at(df)

expected = sorted(SILVER_ROWS[latest_round])
assert data_rows(df) == expected, f"silver rows differ: {data_rows(df)}"

# Promotion never re-stamps provenance: every Silver row names the exact read that
# brought it into Bronze.
traced = ["ID", "NAME", "STATUS", "MODIFIED_AT", "__SOURCE", "__EXPORT_DATE"]
untraced = df.select(traced).subtract(spark.table(bronze()).select(traced))
assert untraced.count() == 0, f"silver rows with provenance Bronze never had: {untraced.collect()}"

reads = df.select("__EXPORT_DATE", "__SOURCE").distinct().count()
assert reads == int(latest_round), f"expected rows from {latest_round} read(s), found {reads}"

print("silver validated")
