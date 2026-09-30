"""Fixtures and assertions for the Elluminate study-milestones platform tests.

The scenario: Elluminate exports one CSV snapshot per day containing *every* study,
and the pipeline has not run for a week. Seven snapshots are therefore sitting in
inbound when it finally does, which is precisely the backlog COMPLETE_DELTA exists to
replay rather than collapse.

Three studies, three milestones each, one cohort per milestone:
  TST_ST_111  never changes — it is re-sent identically every single day
  TST_ST_246  one metric moves each day (enrollment count); its dates never move
  TST_ST_782  every milestone moves each day

Deliberately dependency-free: these run as spark_python_task on a cluster, not under
pytest, so a failed assert is what fails the task.
"""

import datetime

CATALOG = "testing_dev_01"
SCHEMA = "functional_testing"

BRONZE_TABLE = "COMPLETE_DELTA_STUDYMILESTONES_BRONZE_3"
SILVER_TABLE = "COMPLETE_DELTA_STUDYMILESTONES_SILVER_3"

VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/source_data"
INBOUND = f"{VOLUME}/inbound"
METADATA = f"{VOLUME}/metadata"
SOURCE_DIRECTORY = "COMPLETE_DELTA_STUDYMILESTONES_SOURCE_3"

FILE_PREFIX = "StudyMilestones"
SNAPSHOTS = 7

# Elluminate's own column list, in export order.
COLUMNS = [
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
    "UPDATEDATE",
]
HEADER = ",".join(COLUMNS)

METADATA_COLUMNS = {"__BRONZE_LAST_MODIFIED_DT", "__FILEPATH", "__EXPORT_DATE"}
HISTORY_COLUMNS = {"__START_DATE", "__END_DATE", "__CURRENT_FLAG", "__DELETED_FLAG"}

BRONZE_COLUMNS = set(COLUMNS) | METADATA_COLUMNS
SILVER_COLUMNS = (
    set(COLUMNS) | HISTORY_COLUMNS | {"__FILEPATH", "__EXPORT_DATE", "__SILVER_LAST_MODIFIED_DT"}
)

# The five types studymilestones.yml declares; everything else stays string.
CAST_TYPES = {
    "MILESTONESEQ": "decimal(10,2)",
    "KEYSEQ": "int",
    "MILESTONEDATE": "date",
    "UPDATEDATE": "timestamp",
    "MILESTONEVALUE": "decimal(10,2)",
}

# Records that were last touched well before the backlog began. The day is past the
# 12th on purpose: read the other way round, MM/dd/yyyy would yield month 28, so a
# misconfigured format fails the cast validation instead of silently shifting dates.
STATIC_UPDATE = "08/28/2026 08:00:00"
STATIC_START = "2026-08-28 08:00:00"

# The week of snapshots. Elluminate exports at 23:00, after the day's edits.
FIRST_DAY = datetime.date(2026, 9, 13)
EXPORT_HOUR = "230000"

STUDIES = ("TST_ST_111", "TST_ST_246", "TST_ST_782")

# MILESTONECODE -> (SEQ, KEYSEQ, KEYFLAG, NAME, TYPE, DISPLAYTYPE)
MILESTONES = {
    "FPI": ("1.0", "1", "Y", "First Patient In", "DATE", "Date"),
    "LPI": ("2.0", "2", "Y", "Last Patient In", "DATE", "Date"),
    "ENR": ("3.0", "3", "N", "Subjects Enrolled", "NUMBER", "Integer"),
}

# Per study, the fixed part of each milestone and how (or whether) it moves.
BASELINE = {
    "TST_ST_111": {"FPI": "2026-01-15", "LPI": "2026-06-30", "ENR": "120"},
    "TST_ST_246": {"FPI": "2026-02-02", "LPI": "2026-07-20", "ENR": "200"},
    "TST_ST_782": {"FPI": "2026-03-01", "LPI": "2026-09-05", "ENR": "50"},
}


def day(offset: int) -> datetime.date:
    return FIRST_DAY + datetime.timedelta(days=offset)


def export_stamp(offset: int) -> str:
    """The 14 digits __EXPORT_DATE and the snapshot id are both read from."""
    return f"{day(offset).strftime('%Y%m%d')}{EXPORT_HOUR}"


def updated_at(offset: int, hour: str) -> str:
    """A record's own UPDATEDATE, in Elluminate's MM/dd/yyyy HH:mm:ss."""
    return f"{day(offset).strftime('%m/%d/%Y')} {hour}"


def started_at(offset: int, hour: str) -> str:
    """The same instant as __START_DATE renders it once cast."""
    return f"{day(offset).strftime('%Y-%m-%d')} {hour}"


# The two studies that move, and the time of day their edits carry.
ENROLLMENT_HOUR = "06:15:00"
FULL_UPDATE_HOUR = "07:45:00"


def _shift(iso_date: str, days: int) -> str:
    return (datetime.date.fromisoformat(iso_date) + datetime.timedelta(days=days)).isoformat()


def milestone_values(study: str, code: str, offset: int) -> tuple:
    """(MILESTONEDATE, MILESTONEVALUE, UPDATEDATE) for one record in one snapshot.

    A DATE milestone carries no value and a NUMBER milestone carries no date, exactly
    as Elluminate sends them — which also means the cast validation has to tolerate
    genuinely empty fields rather than treat them as failed casts.
    """
    base = BASELINE[study][code]
    numeric = code == "ENR"

    if study == "TST_ST_111":
        # Re-sent unchanged every day: same values, same UPDATEDATE.
        value = f"{base}.00" if numeric else ""
        return ("" if numeric else base, value, STATIC_UPDATE)

    if study == "TST_ST_246":
        if not numeric:
            return (base, "", STATIC_UPDATE)
        # Enrollment climbs by five a day.
        return ("", f"{int(base) + 5 * offset}.00", updated_at(offset, ENROLLMENT_HOUR))

    # TST_ST_782: every milestone moves every day.
    if numeric:
        return ("", f"{int(base) + 3 * offset}.00", updated_at(offset, FULL_UPDATE_HOUR))
    return (_shift(base, offset), "", updated_at(offset, FULL_UPDATE_HOUR))


def snapshot_rows(offset: int) -> list:
    """One day's export: every study, every milestone, one cohort each."""
    rows = []
    for study in STUDIES:
        for code, (seq, keyseq, keyflag, name, kind, display) in MILESTONES.items():
            date_value, number_value, updated = milestone_values(study, code, offset)
            rows.append(
                ",".join(
                    [
                        study,
                        f"{study}_SUB1",
                        "A",
                        "COHORT_1",
                        "ARM_1",
                        seq,
                        keyseq,
                        keyflag,
                        name,
                        name,
                        kind,
                        display,
                        date_value,
                        number_value,
                        code,
                        "ELLUMINATE",
                        "ELLUMINATE_ETL",
                        updated,
                    ]
                )
            )
    return rows


ROWS_PER_SNAPSHOT = len(STUDIES) * len(MILESTONES)
BRONZE_ROWS = ROWS_PER_SNAPSHOT * SNAPSHOTS


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


def expect_types(df) -> None:
    """The five configured columns are typed; every other business column is string."""
    types = dict(df.dtypes)
    for column, expected in CAST_TYPES.items():
        assert types[column] == expected, f"{column} is {types[column]}, expected {expected}"
    for column in COLUMNS:
        if column not in CAST_TYPES:
            assert types[column] == "string", f"{column} should be string, is {types[column]}"


def milestone(df, study: str, code: str):
    """Every history row for one milestone, oldest version first."""
    rows = df.filter((df["STUDYID"] == study) & (df["MILESTONECODE"] == code)).collect()
    return sorted(rows, key=lambda row: row["__START_DATE"])


def expect_window(row, start: str, end, current: str) -> None:
    actual_start = str(row["__START_DATE"])
    actual_end = None if row["__END_DATE"] is None else str(row["__END_DATE"])
    assert actual_start == start, f"expected __START_DATE {start}, found {actual_start}"
    assert actual_end == end, f"expected __END_DATE {end}, found {actual_end}"
    assert (
        row["__CURRENT_FLAG"] == current
    ), f"expected __CURRENT_FLAG {current}, found {row['__CURRENT_FLAG']}"
    assert row["__DELETED_FLAG"] == "N", "nothing in this scenario is deleted"
