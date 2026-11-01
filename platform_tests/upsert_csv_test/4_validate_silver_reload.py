"""Step 9 — the same two exports land again; UPSERT leaves every row as it was.

Round 2 resends exactly the records of round 1 under new export stamps. UPSERT keeps
one row per key, and since no record's __RECORD_HASH changed, it rewrites none of them
(gh #81): every row still carries the export that first brought its content.
"""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    BRONZE_TABLE,
    SILVER_TABLE,
    expect_rows,
    expect_stats_columns,
    expect_value_count,
    qualified,
)
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()

bronze = spark.table(qualified(BRONZE_TABLE))
silver = spark.table(qualified(SILVER_TABLE))

# Bronze accumulated the second pair of exports: 6 + 6.
expect_rows(bronze, 12)
expect_value_count(bronze, "ID", "1.0", 4)

# Silver still holds one row per key — no history, by design.
expect_rows(silver, 5)
expect_value_count(silver, "ID", "1.0", 1)

# Four exports have landed; the oldest two are round 1's.
exports = sorted(row[0] for row in bronze.select("__EXPORT_DATE").distinct().collect())
assert len(exports) == 4, f"expected 4 distinct exports, found {len(exports)}"
first_a, first_b, second_a = exports[0], exports[1], exports[2]


# Each key sits at the round-1 export that last changed it: the Source_A file carries
# 1.0/2.0/3.0 and the Source_B file 1.0/4.0/5.0, and dedup resolved 1.0 to Source_B.
def keys_at(export):
    return {row["ID"] for row in silver.filter(silver["__EXPORT_DATE"] == export).collect()}


assert keys_at(first_b) == {"1.0", "4.0", "5.0"}, f"Source_B keys wrong: {keys_at(first_b)}"
assert keys_at(first_a) == {"2.0", "3.0"}, f"Source_A keys wrong: {keys_at(first_a)}"

# Round 2 changed no record, so no row was rewritten with its export.
rewritten = silver.filter(silver["__EXPORT_DATE"] >= second_a).count()
assert not rewritten, f"{rewritten} unchanged row(s) rewritten by round 2"

for table in (BRONZE_TABLE, SILVER_TABLE):
    expect_stats_columns(spark, table, "__EXPORT_DATE")


# gh #85: each target is clustered by what its verb filters on.
def clustering(table: str) -> list:
    return list(spark.sql(f"DESCRIBE DETAIL {qualified(table)}").collect()[0]["clusteringColumns"])


assert clustering(BRONZE_TABLE) == ["__EXPORT_DATE"], clustering(BRONZE_TABLE)
assert clustering(SILVER_TABLE) == ["ID"], clustering(SILVER_TABLE)

print("silver reload validated")
