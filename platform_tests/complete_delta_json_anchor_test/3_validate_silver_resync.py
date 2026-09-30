"""Step 3 — silver after the full task works through the backlog.

Two full exports are pending. Each supersedes everything before it, so only the resync
(Jan 4) is replayed: exactly its 18 addresses are current, and 018-020, which it
dropped, are not. The stale Jan 1 export never reaches silver.

Every version the resync expired closes at the resync's own export time, not at the
run's: the latest a dropped address can have left the source, and exactly where a
re-asserted one reopens.
"""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    FULL_STALE,
    RESYNC_URIS,
    SILVER_TABLE,
    STEADY_URIS,
    UPDATED,
    UPDATED_STREET,
    current,
    expect_absent,
    expect_current,
    qualified,
)
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()

df = spark.table(qualified(SILVER_TABLE))

expect_current(df, RESYNC_URIS)
expect_absent(df, [FULL_STALE])

# The update reached silver through the resync, which carries it too.
street = current(df).filter(df["URI"] == UPDATED).select("DATA").first()[0]
assert UPDATED_STREET in street, f"{UPDATED} is not at its updated address: {street}"

# --- Validity windows ------------------------------------------------------
exports = current(df).select("__EXPORT_DATE").distinct().collect()
assert len(exports) == 1, f"current rows span {len(exports)} exports, expected the resync only"
resync = exports[0][0]

expired = df.filter(df["__CURRENT_FLAG"] == "N")
assert expired.count() == len(STEADY_URIS), "expected each steady-round version expired"

closes = {row[0] for row in expired.select("__END_DATE").distinct().collect()}
assert closes == {resync}, f"expired versions closed at {closes}, expected {resync}"

# The full task's event time is __EXPORT_DATE, so re-asserted addresses reopen at the
# very instant their previous version closed: no two windows overlap.
opens = {row[0] for row in current(df).select("__START_DATE").distinct().collect()}
assert opens == {resync}, f"current versions opened at {opens}, expected {resync}"

print("silver validated after the resync")
