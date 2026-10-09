"""Step 0 — put this test's own tables and directories back to nothing.

Scoped deliberately: the metadata root is shared with the other platform tests, so
wiping it wholesale would delete a neighbour's checkpoints mid-run.
"""

import os
import shutil
import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import BRONZE_METADATA, SILVER_METADATA, bronze, silver, source  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()

for table in (source(), bronze(), silver()):
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"dropped {table}")

# A full read keeps no checkpoint, but Silver's would make its next run read nothing.
for directory in (BRONZE_METADATA, SILVER_METADATA):
    shutil.rmtree(directory, ignore_errors=True)
    os.makedirs(directory, exist_ok=True)
    print(f"cleared {directory}")
