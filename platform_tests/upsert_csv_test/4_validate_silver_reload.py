"""Step 9 — a second pair of exports lands; UPSERT keeps one row per key.

This is where SCD1 and SCD2 diverge. An SCD2 run would leave 5 current plus 5
expired rows here; UPSERT keeps no history, so the count stays at 5 and the payload
is the one from the newest export that contained each key. When P4 lands, the SCD2
variant of this workflow asserts the 5 + 5 shape against the same fixtures.
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

# Four exports have landed; the newest two are this round's.
exports = sorted(row[0] for row in bronze.select("__EXPORT_DATE").distinct().collect())
assert len(exports) == 4, f"expected 4 distinct exports, found {len(exports)}"
source_a, source_b = exports[-2], exports[-1]


# Every key is refreshed to the newest export *that contained it*, which is not the
# same as the newest export overall: the Source_A file carries 1.0/2.0/3.0 and the
# Source_B file carries 1.0/4.0/5.0, so 2.0 and 3.0 legitimately sit one export back.
# 1.0 is in both and dedup resolves it to Source_B.
def keys_at(export):
    return {row["ID"] for row in silver.filter(silver["__EXPORT_DATE"] == export).collect()}


assert keys_at(source_b) == {"1.0", "4.0", "5.0"}, f"Source_B keys wrong: {keys_at(source_b)}"
assert keys_at(source_a) == {"2.0", "3.0"}, f"Source_A keys wrong: {keys_at(source_a)}"

# Nothing survived from the first round: every key was re-upserted.
assert not silver.filter(silver["__EXPORT_DATE"] < source_a).count(), "stale rows from round 1"

for table in (BRONZE_TABLE, SILVER_TABLE):
    expect_stats_columns(spark, table, "__EXPORT_DATE")

print("silver reload validated")
