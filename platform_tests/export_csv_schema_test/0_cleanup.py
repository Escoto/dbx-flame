"""Step 0 — put this test's own objects and export folder back to nothing.

Scoped deliberately: the exports root is shared with the other platform tests.
"""

import shutil
import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import CONTACT_TABLE, FOLDER, SITE_TABLE, SITE_VIEW, table  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()

spark.sql(f"DROP VIEW IF EXISTS {table(SITE_VIEW)}")
for name in (SITE_TABLE, CONTACT_TABLE):
    spark.sql(f"DROP TABLE IF EXISTS {table(name)}")
print(f"dropped {SITE_VIEW}, {SITE_TABLE}, {CONTACT_TABLE}")

shutil.rmtree(FOLDER, ignore_errors=True)
print(f"cleared {FOLDER}")
