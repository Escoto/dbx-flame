"""Step 0 — put this test's own tables and directories back to nothing.

Scoped deliberately: the inbound and metadata roots are shared with the other
platform tests, so wiping them wholesale would delete a neighbour's checkpoints and
make its next run re-read files it had already consumed.
"""

import os
import shutil
import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    BRONZE_METADATA,
    INBOUND,
    SILVER_METADATA,
    SOURCE_DIRECTORY,
    bronze,
    silver,
)
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()

for table in (bronze(), silver()):
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"dropped {table}")

# Each table's checkpoint lives in its own catalog's volume; left behind, the next run
# would read nothing, believing it had already consumed these rows.
for directory in (f"{INBOUND}/{SOURCE_DIRECTORY}", BRONZE_METADATA, SILVER_METADATA):
    shutil.rmtree(directory, ignore_errors=True)
    os.makedirs(directory, exist_ok=True)
    print(f"cleared {directory}")
