"""Step 2 — the whole backlog landed, and landed typed.

Casting happens on the way into bronze, as it does in production, so this is where
studymilestones.yml is proved: the five configured columns arrive as real decimals,
ints, dates and timestamps, and the thirteen it says nothing about stay strings.
"""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    BRONZE_COLUMNS,
    BRONZE_ROWS,
    BRONZE_TABLE,
    ROWS_PER_SNAPSHOT,
    SNAPSHOTS,
    STUDIES,
    expect_columns,
    expect_rows,
    expect_types,
    qualified,
)
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()

assert spark.catalog.tableExists(qualified(BRONZE_TABLE)), "bronze table was not created"
df = spark.table(qualified(BRONZE_TABLE))

expect_columns(df, BRONZE_COLUMNS)
expect_types(df)

# Seven snapshots, nine records each, none deduplicated on the way in.
expect_rows(df, BRONZE_ROWS)
assert df.select("__FILEPATH").distinct().count() == SNAPSHOTS, "expected 7 source files"
assert df.select("__EXPORT_DATE").distinct().count() == SNAPSHOTS, "exports share a timestamp"
assert df.filter(df["__EXPORT_DATE"].isNull()).count() == 0, "__EXPORT_DATE was not parsed"

# Every study is re-sent in every snapshot, including the one that never changes.
for study in STUDIES:
    found = df.filter(df["STUDYID"] == study).count()
    expected = SNAPSHOTS * (ROWS_PER_SNAPSHOT // len(STUDIES))
    assert found == expected, f"{study} has {found} bronze rows, expected {expected}"

# A cast that quietly produced NULL would have failed the task already
# (typing.validate_casts), but the columns that must always hold a value are worth
# asserting outright.
for column in ("MILESTONESEQ", "KEYSEQ", "UPDATEDATE"):
    nulls = df.filter(df[column].isNull()).count()
    assert nulls == 0, f"{column} has {nulls} NULLs after casting"

# The genuinely empty fields Elluminate sends: a DATE milestone carries no value and
# a NUMBER milestone carries no date. These are empty at source, not failed casts.
date_milestones = SNAPSHOTS * len(STUDIES) * 2
number_milestones = SNAPSHOTS * len(STUDIES)
assert (
    df.filter(df["MILESTONEVALUE"].isNull()).count() == date_milestones
), "date milestones should carry no MILESTONEVALUE"
assert (
    df.filter(df["MILESTONEDATE"].isNull()).count() == number_milestones
), "number milestones should carry no MILESTONEDATE"

print("bronze validated")
