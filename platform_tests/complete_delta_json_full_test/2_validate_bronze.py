"""Step 2 — both bronze tables hold one row per data[] item, payload intact.

No schema hints are configured, so each feed's reader infers its own item schema from
its own file. Those schemas differ: the full export predates "region" entirely, and
only the delta export carries "Type". What this step proves is that the difference
stays inside DATA — both tables come out with the same columns.
"""

import json
import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    ADDED,
    BRONZE_COLUMNS,
    BRONZE_DELTA_TABLE,
    BRONZE_FULL_TABLE,
    DELTA_EVENT,
    DRIFT_FIELD,
    DRIFT_VALUE,
    DRIFTED,
    DRIFTED_FIELD_ABSENT_IN_FULL,
    FULL_EVENT,
    MOVED,
    MOVED_ADDRESS_AFTER,
    MOVED_ADDRESS_BEFORE,
    MUNICH,
    NO_CITY,
    expect_columns,
    expect_rows,
    qualified,
)
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()


def item(df, uri: str) -> dict:
    rows = df.filter(df["URI"] == uri).collect()
    assert len(rows) == 1, f"expected one {uri} row, found {len(rows)}"
    return json.loads(rows[0]["DATA"])


for table in (BRONZE_FULL_TABLE, BRONZE_DELTA_TABLE):
    assert spark.catalog.tableExists(qualified(table)), f"{table} was not created"

full = spark.table(qualified(BRONZE_FULL_TABLE))
delta = spark.table(qualified(BRONZE_DELTA_TABLE))

# The headline: the payloads diverge, the table shapes do not.
expect_columns(full, BRONZE_COLUMNS)
expect_columns(delta, BRONZE_COLUMNS)
assert set(full.columns) == set(delta.columns), "the two feeds produced different columns"

# One row per item in data[], not one row per file.
expect_rows(full, 3)
expect_rows(delta, 3)
assert full.select("URI").distinct().count() == 3, "URIs are not one per item"

# The envelope's own stamp, parsed despite the trailing Z on '20260101050000Z'.
assert full.filter(full["EXPORT_DATE"].isNull()).count() == 0, "EXPORT_DATE was not parsed"
assert str(full.select("EXPORT_DATE").first()[0]) == FULL_EVENT
assert str(delta.select("EXPORT_DATE").first()[0]) == DELTA_EVENT

# Provenance survived the explode — COMPLETE_DELTA cuts its snapshots from __FILEPATH.
assert full.filter(full["__FILEPATH"].isNull()).count() == 0, "__FILEPATH did not survive"
assert full.filter(full["__EXPORT_DATE"].isNull()).count() == 0, "__EXPORT_DATE was not parsed"

# A field no item in this feed carries is absent from every payload in it, and present
# in the other feed. Two readers, two inferred schemas, one table shape.
for uri in (MOVED, MUNICH, NO_CITY):
    assert DRIFTED_FIELD_ABSENT_IN_FULL not in item(full, uri), f"{uri} gained a region"
assert item(delta, MOVED)[DRIFTED_FIELD_ABSENT_IN_FULL] == "Western Australia"

# A field only one item carries still lands, and one it omits stays omitted rather
# than arriving as an explicit null.
drifted = item(delta, DRIFTED)
assert drifted[DRIFT_FIELD] == DRIFT_VALUE, f"{DRIFT_FIELD} did not survive: {drifted}"
assert "related_cro" not in drifted, f"{DRIFTED} should carry no related_cro: {drifted}"
for uri in ADDED:
    assert delta.filter(delta["URI"] == uri).count() == 1, f"{uri} is missing from the feed"

# An absent key stays absent rather than becoming null, per item, within one feed.
assert "city" not in item(full, NO_CITY), "111/1 was never supposed to carry a city"
assert item(full, MUNICH)["city"] == "Munich", "the unconfigured fields were lost"
assert item(full, MUNICH)["related_cro"] == [
    "0-NA7SH0 [ICON]",
    "0-NAP01W [SYNEOS]",
], "the nested array did not survive as JSON text"

# The same address in both feeds, at different statuses and different payloads.
assert full.filter(full["URI"] == MOVED).select("STATUS").first()[0] == "ACTIVE"
assert delta.filter(delta["URI"] == MOVED).select("STATUS").first()[0] == "INACTIVE"
assert item(full, MOVED)["street_address"] == MOVED_ADDRESS_BEFORE
assert item(delta, MOVED)["street_address"] == MOVED_ADDRESS_AFTER

print("bronze validated")
