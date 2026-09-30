"""Constants and assertions shared by the full_csv_long_backlog_test steps.

Deliberately dependency-free: these run as spark_python_task on a cluster, not
under pytest, so a failed assert is what fails the task.

The three exports are committed under sample_data/ rather than generated at run
time: they are fully synthetic (a fictional study, invented milestones), but the
row counts and per-key evolution across the three files are the fixture the test
exercises, so they are fixed data rather than something rebuilt on every run.
"""

CATALOG = "testing_dev_01"
SCHEMA = "functional_testing"

BRONZE_TABLE = "STUDY_MILESTONE_CSV_BRONZE_1"
SILVER_TABLE = "STUDY_MILESTONE_CSV_SILVER_1"

VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/source_data"
INBOUND = f"{VOLUME}/inbound"
METADATA = f"{VOLUME}/metadata"
SOURCE_DIRECTORY = "STUDY_MILESTONE_CSV_SOURCE_1"

BUSINESS_COLUMNS = {
    "STUDYID",
    "SUBSTUDYID",
    "PART",
    "COHORT",
    "ARM",
    "MILESTONESEQ",
    "KEYSEQ",
    "KEYFLAG",
    "MILESTONENAME",
    "DISPLAYNAME",
    "MILESTONETYPE",
    "DISPLAYTYPE",
    "MILESTONEDATE",
    "MILESTONEVALUE",
    "MILESTONECODE",
    "DATASOURCE",
    "CREATEDBY",
    "REFRESHDATE",
    "DC_CONFIG_KEY",
}

BRONZE_COLUMNS = BUSINESS_COLUMNS | {
    "__BRONZE_LAST_MODIFIED_DT",
    "__FILEPATH",
    "__EXPORT_DATE",
}

SILVER_COLUMNS = BUSINESS_COLUMNS | {
    "__FILEPATH",
    "__EXPORT_DATE",
    "__SILVER_LAST_MODIFIED_DT",
}

# The three exports as committed under sample_data/. The 14-digit stamp in each name
# is what __EXPORT_DATE and file ordering are both cut from — a week apart, so
# ordering is unambiguous.
EXPORT_1 = "STUDY_MILESTONE_20260901050000Z.csv"  # baseline: 20 records, all pending
EXPORT_2 = "STUDY_MILESTONE_20260908050000Z.csv"  # resend of 20: keys 1-10 completed
EXPORT_3 = "STUDY_MILESTONE_20260915050000Z.csv"  # resend of 30: 11-20 completed, 21-30 new
EXPORTS = (EXPORT_1, EXPORT_2, EXPORT_3)

EXPORT_1_COUNT = 20
EXPORT_2_COUNT = 20
EXPORT_3_COUNT = 30

BRONZE_TOTAL = EXPORT_1_COUNT + EXPORT_2_COUNT + EXPORT_3_COUNT  # 70
SILVER_EXPECTED = EXPORT_3_COUNT  # 30 — the last full export is the whole truth


def qualified(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def expect_columns(df, expected: set) -> None:
    actual = set(df.columns)
    assert (
        actual == expected
    ), f"columns differ: missing={expected - actual} extra={actual - expected}"


def expect_rows(df, count: int) -> None:
    actual = df.count()
    assert actual == count, f"expected {count} rows, found {actual}"


def expect_stats_columns(spark, table: str, *required: str) -> None:
    """The table keeps data-skipping statistics on the columns the framework filters on."""
    rows = spark.sql(f"SHOW TBLPROPERTIES {qualified(table)}").collect()
    listed = {row.key: row.value for row in rows}.get("delta.dataSkippingStatsColumns", "")
    missing = [column for column in required if column not in listed.split(",")]
    assert not missing, f"{table}: delta.dataSkippingStatsColumns lacks {missing} ({listed!r})"
