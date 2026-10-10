"""Step 1 — set the source table to this round's state, the way a live database changes.

Overwritten rather than appended: a foreign table holds only its current state, and
MODIFIED_AT is all a delta read has to tell what changed.
"""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import SOURCE_ROWS, SOURCE_SCHEMA, source  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()

rows = SOURCE_ROWS[sys.argv[4]]
spark.createDataFrame(rows, SOURCE_SCHEMA).write.format("delta").mode("overwrite").saveAsTable(
    source()
)
print(f"wrote {len(rows)} rows to {source()}")
