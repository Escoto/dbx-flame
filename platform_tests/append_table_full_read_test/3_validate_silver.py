"""Step 3 — Silver (FULL) is exactly the source as of the latest read.

Takes the round just run. In round 2 that proves the whole chain: the changed row is
updated, the added one is there, and the row deleted at the source is gone, which no
incremental read of the source could have revealed.
"""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    SILVER_COLUMNS,
    SOURCE_ROWS,
    bronze,
    data_rows,
    expect_columns,
    expect_table_provenance,
    silver,
)
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

spark = SparkSession.builder.getOrCreate()
latest_round = sys.argv[4]

assert spark.catalog.tableExists(silver()), "silver table was not created"
df = spark.table(silver())

expect_columns(df, SILVER_COLUMNS)

expected = sorted(SOURCE_ROWS[latest_round])
assert data_rows(df) == expected, f"silver rows differ: {data_rows(df)}"

# Promotion never re-stamps provenance: Silver names the same read Bronze's newest rows do.
expect_table_provenance(df)
newest = spark.table(bronze()).agg(F.max("__EXPORT_DATE")).first()[0]
newest_source = (
    spark.table(bronze()).filter(F.col("__EXPORT_DATE") == newest).select("__SOURCE").first()[0]
)
stamps = df.select("__EXPORT_DATE", "__SOURCE").distinct().collect()
assert [tuple(s) for s in stamps] == [
    (newest, newest_source)
], f"silver should carry the newest read's provenance only, found {stamps}"

print("silver validated")
