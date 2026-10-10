"""Step 2 — Bronze holds one stamped snapshot per full read so far.

Takes the round just run: Bronze must hold every round's rows, each round under its
own __EXPORT_DATE and __SOURCE, since APPEND keeps every read whole.
"""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    BRONZE_COLUMNS,
    SOURCE_ROWS,
    bronze,
    data_rows,
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

expected = sorted(row for r in range(1, rounds + 1) for row in SOURCE_ROWS[str(r)])
assert data_rows(df) == expected, f"bronze rows differ: {data_rows(df)}"

# One snapshot per read: a stamp shared across reads would merge them into one.
assert df.filter(df["__EXPORT_DATE"].isNull()).count() == 0, "__EXPORT_DATE was not stamped"
snapshots = df.select("__EXPORT_DATE", "__SOURCE").distinct().count()
assert snapshots == rounds, f"expected {rounds} snapshot(s), found {snapshots}"

print("bronze validated")
