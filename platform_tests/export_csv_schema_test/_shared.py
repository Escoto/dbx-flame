"""Constants, fixtures and assertions shared by the export_csv_schema_test steps.

Deliberately dependency-free: these run as spark_python_task on a cluster, not
under pytest, so a failed assert is what fails the task.
"""

import csv
import io
import os
import sys
from datetime import datetime, timezone

# Every step is called with [base, catalog, env, ...]; the framework resolves the
# catalog the same way.
CATALOG = f"{sys.argv[2]}_{sys.argv[3]}"

# A schema of its own: a whole-schema export of the shared functional_testing schema
# would pick up whatever the tests running beside this one create there.
SCHEMA = "export_testing"
SITE_TABLE = "SITE"
CONTACT_TABLE = "CONTACT"
SITE_VIEW = "SITE_DE"

# The export clears and refills {preparation_path}/{schema_name}, so this is the folder
# both rounds check.
FOLDER = f"/Volumes/{CATALOG}/functional_testing/source_data/exports/{SCHEMA}"

# A quote, a line break, a NULL and an empty string: the values RFC 4180 and the
# NULL-as-empty rule have to keep apart.
SITE_ROWS = [
    ("S1", 'Clinic "North"', "DE", "2024-01-15", "2026-10-01T08:30:00Z"),
    ("S2", "Line\nbreak", "FR", "2024-02-29", "2026-10-02T23:59:59Z"),
    ("S3", None, "", "2024-03-01", "2026-10-03T00:00:00Z"),
]
SITE_COLUMNS = ["SITE_ID", "NAME", "COUNTRY", "OPENED", "UPDATED_AT"]
# A framework column on every SITE row: left out unless export.exclude_metadata is off.
SITE_SOURCE = "site_export.csv"

CONTACT_ROWS = [(1, "S1", "a@example.org"), (2, "S2", "b@example.org")]
CONTACT_COLUMNS = ["CONTACT_ID", "SITE_ID", "EMAIL"]
CONTACT_SCHEMA = "CONTACT_ID int, SITE_ID string, EMAIL string"


def table(name: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{name}`"


def exported_files() -> list:
    return sorted(os.listdir(FOLDER))


def _read(name: str) -> tuple:
    path = os.path.join(FOLDER, name)
    with open(path, newline="", encoding="utf-8") as handle:
        text = handle.read()
    rows = list(csv.reader(io.StringIO(text)))
    return text, rows[0], sorted(rows[1:])


def expect_site(name: str, header: list = SITE_COLUMNS) -> None:
    """SITE's rows under header; a sixth column is the framework's __SOURCE."""
    text, written_header, rows = _read(name)
    assert written_header == header, f"{name} header differs: {written_header}"
    assert len(rows) == len(SITE_ROWS), f"{name} holds {len(rows)} rows: {rows}"
    for row, (site_id, site_name, country, opened, updated) in zip(rows, SITE_ROWS):
        assert row[:4] == [site_id, site_name or "", country, opened], f"{name} row: {row}"
        # Spark writes the timestamp in its session zone; it must be the same instant.
        written = datetime.fromisoformat(row[4])
        expected = datetime.fromisoformat(updated).astimezone(timezone.utc)
        assert written == expected, f"{name} {site_id} UPDATED_AT: {row[4]} != {updated}"
        assert row[5:] == [SITE_SOURCE] * (len(header) - 5), f"{name} row: {row}"
    # csv.reader can't tell NULL from an empty string; the file must.
    assert '\nS3,,"",' in text, f"{name} must write NULL as empty and '' as \"\""


def expect_contact(name: str, header: list = CONTACT_COLUMNS) -> None:
    """CONTACT's rows, holding only the leading columns header names."""
    _, written_header, rows = _read(name)
    assert written_header == header, f"{name} header differs: {written_header}"
    expected = sorted([str(i), site, email][: len(header)] for i, site, email in CONTACT_ROWS)
    assert rows == expected, f"{name} rows differ: {rows}"


def expect_site_view(name: str) -> None:
    _, header, rows = _read(name)
    assert header == ["SITE_ID", "NAME"], f"{name} header differs: {header}"
    assert rows == [["S1", 'Clinic "North"']], f"{name} rows differ: {rows}"
