"""Step 0 — put this test's own corner of the volume back to nothing.

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
    BRONZE_TABLE,
    CATALOG,
    INBOUND,
    METADATA,
    SCHEMA,
    SILVER_TABLE,
    SOURCE_DIRECTORY,
    qualified,
)
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()

for table in (BRONZE_TABLE, SILVER_TABLE):
    spark.sql(f"DROP TABLE IF EXISTS {qualified(table)}")
    print(f"dropped {qualified(table)}")

# The checkpoints live under METADATA. Leaving them would make the next run read
# nothing, because the stream would believe it had already consumed these files.
# One directory per table, matching how the framework lays metadata out:
# {metadata_path}/{catalog}/{schema}/{table}/_checkpoint/
directories = [f"{INBOUND}/{SOURCE_DIRECTORY}"] + [
    f"{METADATA}/{CATALOG}/{SCHEMA}/{table}" for table in (BRONZE_TABLE, SILVER_TABLE)
]

for directory in directories:
    shutil.rmtree(directory, ignore_errors=True)
    os.makedirs(directory, exist_ok=True)
    print(f"cleared {directory}")
