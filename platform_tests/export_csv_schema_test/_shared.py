"""Constants, fixtures and assertions shared by the export_csv_schema_test steps.

Deliberately dependency-free: these run as spark_python_task on a cluster, not
under pytest, so a failed assert is what fails the task.
"""

import csv
import hashlib
import io
import json
import os
import sys

# Every step is called with [base, catalog, env, ...]; the framework resolves the
# catalog the same way.
CATALOG = f"{sys.argv[2]}_{sys.argv[3]}"

# A schema of its own: a whole-schema export of the shared functional_testing schema
# would pick up whatever the tests running beside this one create there.
SCHEMA = "export_testing"
SITE_TABLE = "SITE"
CONTACT_TABLE = "CONTACT"
SITE_VIEW = "SITE_DE"

_VOLUME = f"/Volumes/{CATALOG}/functional_testing/source_data"
# The export clears and refills {preparation_path}/exports_temp/{name}, so this is the
# folder both rounds check.
FOLDER = f"{_VOLUME}/exports/exports_temp/PLATFORM_TEST"
# The whole round delivers loose files to a destination of its own; the subset round has
# none, so its zip lands in {preparation_path}/deliveries/{name}.
DESTINATION = f"{_VOLUME}/deliveries/export_csv_schema"
DEFAULT_DELIVERY = f"{_VOLUME}/exports/deliveries/PLATFORM_TEST"

# A quote, a line break, a NULL and an empty string: the values RFC 4180 and the
# NULL-as-empty rule have to keep apart. Timestamps and decimals are written as the
# export must write them back: each holds exactly its own digits, which Spark's own CSV
# format would cut (microseconds), pad (.120) or turn to E-notation (0E-10).
SITE_ROWS = [
    ("S1", 'Clinic "North"', "DE", "2024-01-15", "2026-10-01T08:30:00.123456Z", "0.0000000000"),
    ("S2", "Line\nbreak", "FR", "2024-02-29", "2026-10-02T23:59:59Z", "0.0000000100"),
    ("S3", None, "", "2024-03-01", "2026-10-03T00:00:00.12Z", None),
]
SITE_COLUMNS = ["SITE_ID", "NAME", "COUNTRY", "OPENED", "UPDATED_AT", "BUDGET"]
# A framework column on every SITE row: left out unless export.exclude_metadata is off.
SITE_SOURCE = "site_export.csv"

CONTACT_ROWS = [(1, "S1", "a@example.org"), (2, "S2", "b@example.org")]
CONTACT_COLUMNS = ["CONTACT_ID", "SITE_ID", "EMAIL"]
CONTACT_SCHEMA = "CONTACT_ID int, SITE_ID string, EMAIL string"


def table(name: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{name}`"


def exported_files() -> list:
    return sorted(os.listdir(FOLDER))


def data_files(files: list) -> list:
    """The exported CSVs, without their checksums and the manifest."""
    return [name for name in files if name.endswith(".csv") and "_MANIFEST_" not in name]


def _read(name: str) -> tuple:
    path = os.path.join(FOLDER, name)
    with open(path, newline="", encoding="utf-8") as handle:
        text = handle.read()
    rows = list(csv.reader(io.StringIO(text)))
    return text, rows[0], sorted(rows[1:])


def expect_site(name: str, header: list = SITE_COLUMNS) -> None:
    """SITE's rows under header; a seventh column is the framework's __SOURCE."""
    text, written_header, rows = _read(name)
    assert written_header == header, f"{name} header differs: {written_header}"
    assert len(rows) == len(SITE_ROWS), f"{name} holds {len(rows)} rows: {rows}"
    width = len(SITE_COLUMNS)
    for row, site in zip(rows, SITE_ROWS):
        # Exact text: the same on any cluster, whatever its session time zone.
        assert row[:width] == [value or "" for value in site], f"{name} row: {row}"
        assert row[width:] == [SITE_SOURCE] * (len(header) - width), f"{name} row: {row}"
    # csv.reader can't tell NULL from an empty string; the file must.
    assert '\nS3,,"",' in text, f"{name} must write NULL as empty and '' as \"\""


def expect_contact(name: str, header: list = CONTACT_COLUMNS) -> None:
    """CONTACT's rows, holding only the leading columns header names."""
    _, written_header, rows = _read(name)
    assert written_header == header, f"{name} header differs: {written_header}"
    expected = sorted([str(i), site, email][: len(header)] for i, site, email in CONTACT_ROWS)
    assert rows == expected, f"{name} rows differ: {rows}"


def expect_manifest(files: list, algorithm: str, expected: dict) -> None:
    """Every CSV has its bare hash beside it, and the manifest lists each object once.

    expected maps an object to its (object_type, pinned, row_count, file name).
    """
    for name in (name for name in files if name.endswith(".csv")):
        stem = os.path.join(FOLDER, name[:-4])
        with open(f"{stem}.csv", "rb") as handle:
            digest = hashlib.new(algorithm, handle.read()).hexdigest()
        with open(f"{stem}.{algorithm}", encoding="ascii") as handle:
            written = handle.read()
        assert written == digest, f"{name}: {algorithm} file holds {written!r}, not {digest}"

    [manifest] = [
        name
        for name in files
        if name.startswith("PLATFORM_TEST_MANIFEST_") and name.endswith(".csv")
    ]
    _, header, rows = _read(manifest)
    assert header == [
        "object",
        "object_type",
        "version",
        "row_count",
        "export_file_name",
        "checksum",
    ], f"manifest header differs: {header}"
    listed = {row[0]: row[1:] for row in rows}
    assert sorted(listed) == sorted(expected), f"manifest lists {sorted(listed)}"
    for name, (object_type, pinned, count, file_name) in expected.items():
        kind, version, row_count, listed_file, checksum = listed[name]
        assert (kind, row_count, listed_file) == (object_type, str(count), file_name), listed[name]
        # A table is pinned to a version; a view has none to record.
        assert version.isdigit() if pinned else version == "", f"{name} version: {version!r}"
        with open(os.path.join(FOLDER, f"{file_name[:-4]}.{algorithm}"), encoding="ascii") as h:
            assert checksum == h.read(), f"{name}: manifest checksum differs from its file's"


def expect_built(path: str, name: str) -> None:
    """The delivered file at path is, byte for byte, the one built as name in FOLDER."""
    with open(path, "rb") as delivered, open(os.path.join(FOLDER, name), "rb") as built:
        assert delivered.read() == built.read(), f"{path} differs from the {name} built"


def expect_audited(task_key: str, algorithm: str, expected: dict) -> None:
    """Each delivered file has one file_delivered row holding the hash of what arrived.

    expected maps a logged file name to (its location, the file whose bytes it hashes).
    A validation step is called with the job's run id after [base, catalog, env], so it
    reads only the rows its own export task wrote in this run.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    rows = (
        spark.table(f"`monitoring_{sys.argv[3]}`.`audit`.`logs`")
        .where(
            (F.col("__workflow_run_id") == sys.argv[4])
            & (F.col("__task_key") == task_key)
            & (F.col("name") == "file_delivered")
        )
        .select("metadata")
        .collect()
    )
    logged = [json.loads(row.metadata) for row in rows]
    by_file = {record["file"]: record for record in logged}
    assert len(by_file) == len(logged), f"a file is logged more than once: {logged}"
    assert sorted(by_file) == sorted(expected), f"file_delivered rows for {sorted(by_file)}"

    # The manifest names the version each data file holds; the manifest and zip hold none.
    [manifest] = [name for name in expected if "_MANIFEST_" in name]
    with open(os.path.join(FOLDER, manifest), newline="", encoding="utf-8") as handle:
        versions = {row["export_file_name"]: row["version"] for row in csv.DictReader(handle)}

    for name, (location, path) in expected.items():
        record = by_file[name]
        with open(path, "rb") as handle:
            digest = hashlib.new(algorithm, handle.read()).hexdigest()
        assert record["algorithm"] == algorithm, f"{name}: {record}"
        assert record["checksum"] == digest, f"{name}: logged {record['checksum']}, not {digest}"
        assert record["location"] == location, f"{name}: logged at {record['location']}"
        version = versions.get(name) or None
        assert record["version"] == (version and int(version)), f"{name}: {record}"


def expect_site_view(name: str) -> None:
    _, header, rows = _read(name)
    assert header == ["SITE_ID", "NAME"], f"{name} header differs: {header}"
    assert rows == [["S1", 'Clinic "North"']], f"{name} rows differ: {rows}"
