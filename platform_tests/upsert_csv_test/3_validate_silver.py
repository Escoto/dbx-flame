"""Step 5 — silver holds one row per key, resolved to the newer export."""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    CATALOG,
    SCHEMA,
    SILVER_COLUMNS,
    SILVER_TABLE,
    expect_columns,
    expect_rows,
    expect_value_count,
    qualified,
)
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

spark = SparkSession.builder.getOrCreate()

assert spark.catalog.tableExists(qualified(SILVER_TABLE)), "silver table was not created"
df = spark.table(qualified(SILVER_TABLE))

# Promotion drops the bronze timestamp and stamps its own.
expect_columns(df, SILVER_COLUMNS)

# Five distinct keys across the two exports: 1.0 appeared twice.
expect_rows(df, 5)
expect_value_count(df, "ID", "1.0", 1)

# Dedup ordered by __EXPORT_DATE keeps the row from the later export, so the
# surviving 1.0 is the Source_B one rather than whichever arrived last.
survivor = df.filter(df["ID"] == "1.0").collect()[0]
assert (
    survivor["SOURCE_SYSTEM"] == "Source_B"
), f"expected the newer export to win, got {survivor['SOURCE_SYSTEM']}"

assert df.filter(df["__SILVER_LAST_MODIFIED_DT"].isNull()).count() == 0, "write time missing"

# The framework keeps output.tags on its target.
tags = {
    row.tag_name: row.tag_value
    for row in spark.table(f"`{CATALOG}`.information_schema.table_tags")
    .where((F.lower("schema_name") == SCHEMA) & (F.lower("table_name") == SILVER_TABLE.lower()))
    .collect()
}
expected_tags = {"project": "dbx-flame", "environment": sys.argv[3]}
assert expected_tags.items() <= tags.items(), f"expected tags {expected_tags}, found {tags}"

print("silver validated")
