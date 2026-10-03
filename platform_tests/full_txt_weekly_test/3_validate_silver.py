"""Step 3 — silver holds the latest export, and has only ever held the scheduled ones.

Every FULL overwrite is a Delta version, so time travel shows each export silver held.
In round 3 the batch carries Wednesday's push and Monday 3's export together; FULL
keeps the newest, so the push must never appear in any version.
"""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    SILVER_COLUMNS,
    SILVER_SEEN,
    SILVER_TABLE,
    business_rows,
    expect_columns,
    export_stamps,
    qualified,
)
from pyspark.sql import SparkSession  # noqa: E402

round_number = sys.argv[4]
seen = SILVER_SEEN[round_number]
latest = seen[-1]

spark = SparkSession.builder.getOrCreate()

table = qualified(SILVER_TABLE)
assert spark.catalog.tableExists(table), "silver table was not created"
df = spark.table(table)

expect_columns(df, SILVER_COLUMNS)
assert business_rows(df) == set(latest["rows"]), f"silver is not {latest['file']}"
assert export_stamps(df) == [latest["stamp"]], f"silver exports: {export_stamps(df)}"

# Property changes add versions with unchanged data, so collapse consecutive repeats.
history = []
versions = sorted(row["version"] for row in spark.sql(f"DESCRIBE HISTORY {table}").collect())
for version in versions:
    stamps = export_stamps(spark.read.option("versionAsOf", version).table(table))
    if not history or history[-1] != stamps:
        history.append(stamps)

expected = [[export["stamp"]] for export in seen]
assert history == expected, f"silver held {history}, expected {expected}"

print(f"round {round_number}: silver validated")
