"""Step 0 — put this test's own objects, export folder and deliveries back to nothing.

Scoped deliberately: the exports root is shared with the other platform tests.
"""

import shutil
import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    CONTACT_TABLE,
    DEFAULT_DELIVERY,
    DESTINATION,
    FOLDER,
    SITE_TABLE,
    SITE_VIEW,
    table,
)
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()

spark.sql(f"DROP VIEW IF EXISTS {table(SITE_VIEW)}")
for name in (SITE_TABLE, CONTACT_TABLE):
    spark.sql(f"DROP TABLE IF EXISTS {table(name)}")
print(f"dropped {SITE_VIEW}, {SITE_TABLE}, {CONTACT_TABLE}")

for folder in (FOLDER, DESTINATION, DEFAULT_DELIVERY):
    shutil.rmtree(folder, ignore_errors=True)
    print(f"cleared {folder}")
