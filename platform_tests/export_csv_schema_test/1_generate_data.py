"""Step 1 — two tables and a view over one of them: the schema the export reads."""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    CONTACT_ROWS,
    CONTACT_SCHEMA,
    CONTACT_TABLE,
    SITE_COLUMNS,
    SITE_ROWS,
    SITE_SOURCE,
    SITE_TABLE,
    SITE_VIEW,
    table,
)
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

spark = SparkSession.builder.getOrCreate()

# Strings first, then cast: a timestamp with its zone parses to the same instant on any
# cluster, and the export writes it back as the same text.
site = (
    spark.createDataFrame(SITE_ROWS, ", ".join(f"{name} string" for name in SITE_COLUMNS))
    .withColumn("OPENED", F.col("OPENED").cast("date"))
    .withColumn("UPDATED_AT", F.col("UPDATED_AT").cast("timestamp"))
    .withColumn("BUDGET", F.col("BUDGET").cast("decimal(20,10)"))
    .withColumn("__SOURCE", F.lit(SITE_SOURCE))
)
site.write.format("delta").saveAsTable(table(SITE_TABLE))

spark.createDataFrame(CONTACT_ROWS, CONTACT_SCHEMA).write.format("delta").saveAsTable(
    table(CONTACT_TABLE)
)

spark.sql(
    f"CREATE VIEW {table(SITE_VIEW)} AS "
    f"SELECT SITE_ID, NAME FROM {table(SITE_TABLE)} WHERE COUNTRY = 'DE'"
)
print(f"created {SITE_TABLE}, {CONTACT_TABLE} and the view {SITE_VIEW}")
