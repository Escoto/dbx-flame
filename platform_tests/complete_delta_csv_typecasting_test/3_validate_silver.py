"""Step 3 — silver holds the week's evolution, and only the evolution.

One silver run replayed all seven snapshots. What it should have built:

  TST_ST_111  three rows, still on their original version. Re-sending a record
              unchanged, seven days running, must not manufacture seven versions —
              the anti-filter drops it because its UPDATEDATE never moved.
  TST_ST_246  the enrollment count has a seven-link chain; its two date milestones
              never moved and stay on one row each.
  TST_ST_782  every milestone has the full seven-link chain.

Under snapshot_scope=delta, a record absent from an increment simply goes untouched —
which for a feed that re-sends everything daily means the unchanged studies cost
nothing. The `full` variant of this test asserts the opposite behaviour.
"""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    ENROLLMENT_HOUR,
    FULL_UPDATE_HOUR,
    SILVER_COLUMNS,
    SILVER_TABLE,
    SNAPSHOTS,
    STATIC_START,
    expect_columns,
    expect_rows,
    expect_types,
    expect_window,
    milestone,
    qualified,
    started_at,
)
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()

assert spark.catalog.tableExists(qualified(SILVER_TABLE)), "silver table was not created"
df = spark.table(qualified(SILVER_TABLE))

# Promotion drops the bronze timestamp and adds the four history columns. The types
# bronze was given survive the merge; nothing was re-cast or widened back to string.
expect_columns(df, SILVER_COLUMNS)
expect_types(df)

# 3 unchanged + (2 unchanged + 7 versions) + (3 x 7 versions)
expect_rows(df, 33)
assert df.filter(df["__CURRENT_FLAG"] == "Y").count() == 9, "expected 9 current records"
assert df.filter(df["__CURRENT_FLAG"] == "N").count() == 24, "expected 24 expired records"
assert df.filter(df["__DELETED_FLAG"] == "Y").count() == 0, "nothing was deleted"
assert df.filter(df["__START_DATE"].isNull()).count() == 0, "UPDATEDATE failed to parse"


def expect_chain(study: str, code: str, hour: str) -> list:
    """Seven versions whose windows meet end-to-end, the last one still open."""
    versions = milestone(df, study, code)
    assert len(versions) == SNAPSHOTS, f"{study}/{code}: {len(versions)} versions, expected 7"
    for offset, row in enumerate(versions):
        closes = None if offset == SNAPSHOTS - 1 else started_at(offset + 1, hour)
        current = "Y" if offset == SNAPSHOTS - 1 else "N"
        expect_window(row, start=started_at(offset, hour), end=closes, current=current)
    return versions


def expect_untouched(study: str, code: str) -> None:
    """One row, still open, still on the version it was first seen at."""
    versions = milestone(df, study, code)
    assert len(versions) == 1, f"{study}/{code}: {len(versions)} versions, expected 1"
    expect_window(versions[0], start=STATIC_START, end=None, current="Y")


# --- TST_ST_111: re-sent identically seven times, and it shows nowhere ------
for code in ("FPI", "LPI", "ENR"):
    expect_untouched("TST_ST_111", code)

assert (
    df.filter(df["STUDYID"] == "TST_ST_111").count() == 3
), "an unchanged daily snapshot must not create history"

# It was written once, by the first snapshot, and never rewritten since.
first_export = (
    df.filter(df["STUDYID"] == "TST_ST_111").select("__EXPORT_DATE").distinct().collect()
)
assert len(first_export) == 1, "TST_ST_111 should carry only the first export's provenance"

# --- TST_ST_246: one metric moves, the dates do not ------------------------
for code in ("FPI", "LPI"):
    expect_untouched("TST_ST_246", code)

enrollment = expect_chain("TST_ST_246", "ENR", ENROLLMENT_HOUR)
# Climbing by five a day, from 200 on the first snapshot to 230 on the seventh.
assert str(enrollment[0]["MILESTONEVALUE"]) == "200.00", enrollment[0]["MILESTONEVALUE"]
assert str(enrollment[-1]["MILESTONEVALUE"]) == "230.00", enrollment[-1]["MILESTONEVALUE"]

# --- TST_ST_782: everything moves, every day -------------------------------
for code in ("FPI", "LPI", "ENR"):
    expect_chain("TST_ST_782", code, FULL_UPDATE_HOUR)

assert df.filter(df["STUDYID"] == "TST_ST_782").count() == 21, "expected 3 x 7 versions"

# The date milestone really did move a day per snapshot, as a date and not a string.
replanned = milestone(df, "TST_ST_782", "FPI")
assert str(replanned[0]["MILESTONEDATE"]) == "2026-03-01", replanned[0]["MILESTONEDATE"]
assert str(replanned[-1]["MILESTONEDATE"]) == "2026-03-07", replanned[-1]["MILESTONEDATE"]

# Enrollment climbing by three a day, 50 to 68.
counts = milestone(df, "TST_ST_782", "ENR")
assert str(counts[0]["MILESTONEVALUE"]) == "50.00", counts[0]["MILESTONEVALUE"]
assert str(counts[-1]["MILESTONEVALUE"]) == "68.00", counts[-1]["MILESTONEVALUE"]

print("silver replay validated")
