"""Step 3 — the same week of snapshots under snapshot_scope=full.

Identical fixtures to the `delta` variant, one parameter different, and a very
different silver. Under `full` each snapshot re-asserts the entire dataset and so
supersedes every one before it: of the seven pending, only the newest is replayed. The
older six stay in bronze, which is where export-by-export history is kept.

What the two scopes agree on is the current state — the nine open rows are identical
either way.
"""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    ENROLLMENT_HOUR,
    FULL_UPDATE_HOUR,
    MILESTONES,
    ROWS_PER_SNAPSHOT,
    SILVER_COLUMNS,
    SILVER_TABLE,
    SNAPSHOTS,
    STATIC_START,
    STUDIES,
    expect_columns,
    expect_rows,
    expect_types,
    milestone,
    qualified,
    started_at,
)
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()

assert spark.catalog.tableExists(qualified(SILVER_TABLE)), "silver table was not created"
df = spark.table(qualified(SILVER_TABLE))

expect_columns(df, SILVER_COLUMNS)
expect_types(df)

# Only the newest snapshot reached silver: 9 records, all open.
expect_rows(df, ROWS_PER_SNAPSHOT)
assert df.filter(df["__CURRENT_FLAG"] == "Y").count() == 9, "expected 9 current records"
assert df.filter(df["__DELETED_FLAG"] == "Y").count() == 0, "omission is not a soft delete"
assert df.filter(df["__START_DATE"].isNull()).count() == 0, "UPDATEDATE failed to parse"

exports = df.select("__EXPORT_DATE").distinct().collect()
assert len(exports) == 1, f"silver spans {len(exports)} exports, expected only the last"

for study in STUDIES:
    for code in MILESTONES:
        versions = milestone(df, study, code)
        assert len(versions) == 1, f"{study}/{code}: {len(versions)} rows, expected 1"

# Open versions open at the event time the record itself carries.
for code in MILESTONES:
    starts = {str(row["__START_DATE"]) for row in milestone(df, "TST_ST_111", code)}
    assert starts == {STATIC_START}, f"TST_ST_111/{code} start dates drifted: {starts}"

for study, code, hour in (
    ("TST_ST_246", "ENR", ENROLLMENT_HOUR),
    ("TST_ST_782", "FPI", FULL_UPDATE_HOUR),
):
    row = milestone(df, study, code)[0]
    expected = started_at(SNAPSHOTS - 1, hour)
    assert str(row["__START_DATE"]) == expected, f"{study}/{code}: {row['__START_DATE']}"


# Same final values as the delta variant: the two scopes disagree about history,
# never about the present.
def current_value(study: str, code: str):
    return milestone(df, study, code)[0]


assert str(current_value("TST_ST_246", "ENR")["MILESTONEVALUE"]) == "230.00"
assert str(current_value("TST_ST_782", "ENR")["MILESTONEVALUE"]) == "68.00"
assert str(current_value("TST_ST_782", "FPI")["MILESTONEDATE"]) == "2026-03-07"
assert str(current_value("TST_ST_111", "ENR")["MILESTONEVALUE"]) == "120.00"

print("silver full-scope replay validated")
