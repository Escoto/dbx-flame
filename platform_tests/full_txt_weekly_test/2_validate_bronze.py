"""Step 2 — bronze keeps every export it has received, the accidental one included."""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    BRONZE_COLUMNS,
    BRONZE_EXPORTS,
    BRONZE_TABLE,
    business_rows,
    expect_columns,
    expect_rows,
    export_stamps,
    qualified,
)
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

round_number = sys.argv[4]
expected = BRONZE_EXPORTS[round_number]

spark = SparkSession.builder.getOrCreate()

assert spark.catalog.tableExists(qualified(BRONZE_TABLE)), "bronze table was not created"
df = spark.table(qualified(BRONZE_TABLE))

expect_columns(df, BRONZE_COLUMNS)
expect_rows(df, sum(len(export["rows"]) for export in expected))

stamps = export_stamps(df)
assert stamps == [export["stamp"] for export in expected], f"bronze exports: {stamps}"

# Each export landed whole and unsplit: the pipe delimiter kept "Acme, Inc." in one column.
for export in expected:
    landed = df.filter(F.col("__FILEPATH").endswith(export["file"]))
    assert business_rows(landed) == set(export["rows"]), f"{export['file']} landed altered"

print(f"round {round_number}: bronze validated")
