"""Step 2 — Bronze holds every delta read so far, each under its own stamp.

Takes the round just run: Bronze must hold the first read whole and, from the second
on, only the rows whose MODIFIED_AT moved past the previous read.
"""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    BRONZE_COLUMNS,
    CHANGED_ROWS,
    bronze,
    data_rows,
    expect_anchor_is_modified_at,
    expect_columns,
    expect_table_provenance,
)
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()
rounds = int(sys.argv[4])

assert spark.catalog.tableExists(bronze()), "bronze table was not created"
df = spark.table(bronze())

expect_columns(df, BRONZE_COLUMNS)
expect_table_provenance(df)
expect_anchor_is_modified_at(df)

# The unchanged row was read once: a second copy would mean the watermark was ignored.
expected = sorted(row for r in range(1, rounds + 1) for row in CHANGED_ROWS[str(r)])
assert data_rows(df) == expected, f"bronze rows differ: {data_rows(df)}"

# One stamp per read: a stamp shared across reads would merge them into one export.
assert df.filter(df["__EXPORT_DATE"].isNull()).count() == 0, "__EXPORT_DATE was not stamped"
reads = df.select("__EXPORT_DATE", "__SOURCE").distinct().count()
assert reads == rounds, f"expected {rounds} read(s), found {reads}"

print("bronze validated")
