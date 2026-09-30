"""Step 3 — every key has exactly one current row, at its newest version.

SCD2 takes changes: a key the newest export omits is untouched, not deleted. KEYSEQ
15 and 20 therefore stay current at export 2's version, which wins over export 1's
identical resend because it is the newer export.
"""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    OMITTED_KEYS,
    OMITTED_KEYS_EXPORT,
    SILVER_COLUMNS,
    SILVER_EXPECTED,
    SILVER_TABLE,
    current,
    expect_columns,
    expect_rows,
    qualified,
)
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

spark = SparkSession.builder.getOrCreate()

assert spark.catalog.tableExists(qualified(SILVER_TABLE)), "silver table was not created"
df = spark.table(qualified(SILVER_TABLE))

expect_columns(df, SILVER_COLUMNS)

# The first run creates the target from one deduplicated batch: one row per key.
expect_rows(df, SILVER_EXPECTED)

live = current(df)
expect_rows(live, SILVER_EXPECTED)

duplicated = live.groupBy("KEYSEQ").count().filter(F.col("count") > 1).select("KEYSEQ").collect()
assert not duplicated, f"KEYSEQ(s) current more than once: {[r['KEYSEQ'] for r in duplicated]}"

# Omitted by export 3, so untouched: current, at the newer of the two identical resends.
for key in OMITTED_KEYS:
    kept = live.filter(
        (F.col("KEYSEQ") == key)
        & (F.col("__EXPORT_DATE") == F.to_timestamp(F.lit(OMITTED_KEYS_EXPORT)))
    )
    assert kept.count() == 1, f"KEYSEQ {key} should stay current at export 2's version"

# A key export 3 updated must reflect export 3, not an earlier export.
completed = live.filter((F.col("KEYSEQ") == "11") & (F.col("MILESTONEVALUE") == "Completed"))
assert completed.count() == 1, "KEYSEQ 11 should be Completed per export 3"

# The new inserts from export 3 must be present.
assert live.filter(F.col("KEYSEQ") == "24").count() == 1, "new record from export 3 is missing"

print("silver validated")
