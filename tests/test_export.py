"""Export: one CSV per object in the preparation path, and the export task's config."""

from __future__ import annotations

import csv
import hashlib
import os
import uuid
from collections import defaultdict
from unittest.mock import MagicMock, patch

import pytest

from dbx_flame.context.config import ExportConfig, ExportTaskConfig
from dbx_flame.context.loader import ConfigValidationError, load_config
from dbx_flame.entrypoints import export
from dbx_flame.entrypoints.export import export_objects, main
from dbx_flame.observability.kpi import Kpi

STAMP = "20261013T101500"

PARAMS = {
    "catalog": "codex",
    "env": "dev_01",
    "export.name": "CRO",
    "export.schema_name": "gold_odr",
    "export.preparation_path": "/Volumes/codex_dev_01/metadata/exports/",
    "export.stamp_format": "%Y%m%dT%H%M%S",
    "export.checksum": "sha256",
}

MANIFEST = f"CRO_MANIFEST_{STAMP}"


@pytest.fixture(autouse=True)
def object_types(spark):
    """Unity Catalog's table types, which a local session has no information_schema for."""
    overrides: dict[str, tuple] = {}

    def lookup(_spark, schema):
        tables = spark.catalog.listTables(schema.strip("`"))
        found = {
            t.name: (t.tableType, "DELTA" if t.tableType == "MANAGED" else None) for t in tables
        }
        return {**found, **overrides}

    with patch("dbx_flame.entrypoints.export._object_types", lookup):
        yield overrides


@pytest.fixture
def schema(spark):
    name = f"test_export_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE `{name}`")
    spark.createDataFrame(
        [(1, 'say "hi"'), (2, "line\nbreak"), (3, "  padded  "), (4, None), (5, "")],
        "ID INT, TEXT STRING",
    ).write.format("delta").saveAsTable(f"`{name}`.`SUBJECTS`")
    spark.createDataFrame(
        [(10, "DE", "sites.csv")], "ID INT, COUNTRY STRING, __SOURCE STRING"
    ).write.format("delta").saveAsTable(f"`{name}`.`SITES`")
    yield name
    spark.sql(f"DROP DATABASE IF EXISTS `{name}` CASCADE")


def _config(schema, tmp_path, tables=(), with_ts=False, **settings):
    return ExportConfig(
        name="CRO",
        schema_name=schema,
        tables=list(tables),
        preparation_path=str(tmp_path),
        stamp_format="%Y",
        with_ts_object_names=with_ts,
        **{"checksum": "sha256", **settings},
    )


def _export(spark, schema, tmp_path, tables=(), with_ts=False, **settings):
    loggers = defaultdict(MagicMock)
    config = _config(schema, tmp_path, tables, with_ts, **settings)
    export_objects(spark, f"`{schema}`", config, STAMP, loggers.__getitem__)
    return tmp_path / schema, loggers


def _csvs(folder):
    """The data files, without their checksums and the manifest."""
    return sorted(
        name for name in os.listdir(folder) if name.endswith(".csv") and "MANIFEST" not in name
    )


def _manifest(folder):
    with open(folder / f"{MANIFEST}.csv", newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))
    assert rows[0] == list(export._MANIFEST_HEADER)
    return {row[0]: row[1:] for row in rows[1:]}


def _header(path):
    with open(path, newline="", encoding="utf-8") as handle:
        return next(csv.reader(handle))


def _previous_export(tmp_path, schema):
    """A file from an earlier run, which a refused run must leave in place."""
    previous = tmp_path / schema / "previous_export.csv"
    previous.parent.mkdir()
    previous.write_text("kept")
    return previous


def _argv(params):
    return ["dbx-flame-export"] + [f"--{key}={value}" for key, value in params.items()]


def test_export_writes_one_csv_per_object_of_the_schema(spark, schema, tmp_path):
    folder, loggers = _export(spark, schema, tmp_path)

    assert sorted(os.listdir(folder)) == [
        f"{MANIFEST}.csv",
        f"{MANIFEST}.sha256",
        "SITES.csv",
        "SITES.sha256",
        "SUBJECTS.csv",
        "SUBJECTS.sha256",
    ]
    kpi = loggers[f"`{schema}`.`subjects`"].kpi
    kpi.assert_called_once()
    assert kpi.call_args.args[:2] == (Kpi.ROWS_EXPORTED, 5)


def test_export_stamps_object_names_only_when_asked(spark, schema, tmp_path):
    folder, _ = _export(spark, schema, tmp_path, with_ts=True)

    assert _csvs(folder) == [f"SITES_{STAMP}.csv", f"SUBJECTS_{STAMP}.csv"]
    assert (folder / f"SITES_{STAMP}.sha256").exists()


def test_export_follows_rfc_4180_and_keeps_values_as_they_are(spark, schema, tmp_path):
    folder, _ = _export(spark, schema, tmp_path, tables=["SUBJECTS"])
    path = folder / "SUBJECTS.csv"
    text = path.read_text(encoding="utf-8")

    with open(path, newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))
    assert rows[0] == ["ID", "TEXT"]
    assert sorted(rows[1:]) == [
        ["1", 'say "hi"'],
        ["2", "line\nbreak"],
        ["3", "  padded  "],
        ["4", ""],
        ["5", ""],
    ]
    # csv.reader can't tell NULL from an empty string; the file must.
    assert "\n4,\n" in text
    assert '\n5,""\n' in text


def test_export_of_named_objects_matches_names_in_any_case(spark, schema, tmp_path):
    folder, _ = _export(spark, schema, tmp_path, tables=["Sites"])

    assert _csvs(folder) == ["SITES.csv"]


def test_export_refuses_an_unknown_object_before_touching_the_folder(spark, schema, tmp_path):
    previous = _previous_export(tmp_path, schema)

    with pytest.raises(ValueError, match="not in .*: MISSING"):
        _export(spark, schema, tmp_path, tables=["SITES", "MISSING"])

    assert os.listdir(previous.parent) == ["previous_export.csv"]


def test_export_leaves_framework_columns_out_by_default(spark, schema, tmp_path):
    folder, _ = _export(spark, schema, tmp_path, tables=["SITES"])

    assert _header(folder / "SITES.csv") == ["ID", "COUNTRY"]


def test_export_keeps_framework_columns_when_asked(spark, schema, tmp_path):
    folder, _ = _export(spark, schema, tmp_path, tables=["SITES"], exclude_metadata=False)

    assert _header(folder / "SITES.csv") == ["ID", "COUNTRY", "__SOURCE"]


def test_export_excludes_then_renames_columns_per_object(spark, schema, tmp_path):
    """COUNTRY is excluded first, so renaming ID onto its name is no clash."""
    folder, _ = _export(
        spark,
        schema,
        tmp_path,
        excludes={"sites": ["COUNTRY"]},
        renames={"SITES": {"ID": "COUNTRY"}},
    )

    assert _header(folder / "SITES.csv") == ["COUNTRY"]
    assert _header(folder / "SUBJECTS.csv") == ["ID", "TEXT"]


@pytest.mark.parametrize(
    ("settings", "message"),
    [
        # A mistyped exclude would deliver the very columns it meant to hold back.
        ({"excludes": {"SITE": ["COUNTRY"]}}, "name objects not in .*: SITE"),
        ({"renames": {"SITE": {"ID": "SITE_ID"}}}, "name objects not in .*: SITE"),
        ({"excludes": {"SITES": ["COUNTRYY"]}}, "sites has no column\\(s\\): COUNTRYY"),
        ({"renames": {"SITES": {"IDD": "SITE_ID"}}}, "sites has no column\\(s\\): IDD"),
        ({"renames": {"SITES": {"ID": "COUNTRY"}}}, "more than one column named: COUNTRY"),
        ({"excludes": {"SITES": ["ID", "COUNTRY"]}}, "sites would be exported without any"),
    ],
)
def test_export_refuses_settings_that_miss_before_touching_the_folder(
    spark, schema, tmp_path, settings, message
):
    previous = _previous_export(tmp_path, schema)

    with pytest.raises(ValueError, match=message):
        _export(spark, schema, tmp_path, **settings)

    assert os.listdir(previous.parent) == ["previous_export.csv"]


def test_export_skips_settings_of_an_object_it_does_not_export(spark, schema, tmp_path):
    folder, loggers = _export(
        spark, schema, tmp_path, tables=["SUBJECTS"], excludes={"SITES": ["COUNTRY"]}
    )

    assert _csvs(folder) == ["SUBJECTS.csv"]
    warning = loggers[f"`{schema}`.`sites`"].warning
    assert warning.call_args.kwargs["name"] == "export_settings_skipped"


def test_export_clears_only_its_own_schema_folder(spark, schema, tmp_path):
    stale = tmp_path / schema / "stale.csv"
    neighbour = tmp_path / "other_schema" / "keep.csv"
    for path in (stale, neighbour):
        path.parent.mkdir()
        path.write_text("x")

    _export(spark, schema, tmp_path, tables=["SITES"])

    assert not stale.exists()
    assert neighbour.exists()


def test_export_attempts_every_object_before_failing_the_run(spark, schema, tmp_path):
    spark.sql(f"CREATE TABLE `{schema}`.`GONE` (ID INT) USING DELTA")
    spark.sql(f"CREATE VIEW `{schema}`.`BROKEN` AS SELECT * FROM `{schema}`.`GONE`")
    spark.sql(f"DROP TABLE `{schema}`.`GONE`")
    loggers = defaultdict(MagicMock)

    with pytest.raises(RuntimeError, match="1 object\\(s\\): broken"):
        export_objects(spark, f"`{schema}`", _config(schema, tmp_path), STAMP, loggers.__getitem__)

    assert _csvs(tmp_path / schema) == ["SITES.csv", "SUBJECTS.csv"]
    assert sorted(_manifest(tmp_path / schema)) == ["SITES", "SUBJECTS"]
    error = loggers[f"`{schema}`.`broken`"].error
    assert error.call_args.kwargs["name"] == "object_export_failure"


@pytest.mark.parametrize("algorithm", ["md5", "sha256", "sha512"])
def test_export_checksum_file_holds_the_bare_hash_of_its_file(spark, schema, tmp_path, algorithm):
    """Exactly what md5sum, sha256sum or sha512sum print for the file: nothing else."""
    folder, _ = _export(spark, schema, tmp_path, checksum=algorithm)

    for stem in ("SITES", "SUBJECTS", MANIFEST):
        expected = hashlib.new(algorithm, (folder / f"{stem}.csv").read_bytes()).hexdigest()
        assert (folder / f"{stem}.{algorithm}").read_text(encoding="ascii") == expected


def test_export_manifest_lists_each_object_with_its_version_count_and_checksum(
    spark, schema, tmp_path
):
    spark.sql(f"CREATE VIEW `{schema}`.`SUBJECT_IDS` AS SELECT ID FROM `{schema}`.`SUBJECTS`")
    spark.sql(f"CREATE TABLE `{schema}`.`EMPTY` (ID INT) USING DELTA")

    folder, _ = _export(spark, schema, tmp_path)

    checksum = (folder / "SUBJECTS.sha256").read_text(encoding="ascii")
    assert _manifest(folder) == {
        "EMPTY": ["MANAGED", "0", "0", "EMPTY.csv", (folder / "EMPTY.sha256").read_text()],
        "SITES": ["MANAGED", "0", "1", "SITES.csv", (folder / "SITES.sha256").read_text()],
        "SUBJECTS": ["MANAGED", "0", "5", "SUBJECTS.csv", checksum],
        # A view has no version to pin; its count is the file's.
        "SUBJECT_IDS": [
            "VIEW",
            "",
            "5",
            "SUBJECT_IDS.csv",
            (folder / "SUBJECT_IDS.sha256").read_text(),
        ],
    }


def test_export_counts_records_across_any_chunk_boundary(spark, schema, tmp_path):
    """SUBJECTS quotes a quote and a line break; a one-byte read splits every one of them."""
    with patch("dbx_flame.entrypoints.export._CHUNK", 1):
        _, loggers = _export(spark, schema, tmp_path, tables=["SUBJECTS"])

    assert loggers[f"`{schema}`.`subjects`"].kpi.call_args.args[:2] == (Kpi.ROWS_EXPORTED, 5)


def test_export_reads_a_table_at_the_version_it_recorded(spark, schema, tmp_path):
    """A commit landing mid-export reaches neither the file nor the count it is held to."""
    write = export._write_csv

    def commit_then_write(frame, path):
        spark.sql(f"INSERT INTO `{schema}`.`SUBJECTS` VALUES (6, 'late')")
        write(frame, path)

    with patch("dbx_flame.entrypoints.export._write_csv", commit_then_write):
        folder, _ = _export(spark, schema, tmp_path, tables=["SUBJECTS"])

    assert _manifest(folder)["SUBJECTS"][:3] == ["MANAGED", "0", "5"]


def test_export_fails_an_object_whose_file_holds_fewer_rows_than_its_version(
    spark, schema, tmp_path
):
    write = export._write_csv

    def truncated(frame, path):
        write(frame, path)
        with open(path, encoding="utf-8") as handle:
            lines = handle.readlines()
        with open(path, "w", encoding="utf-8") as handle:
            handle.writelines(lines[:-1])

    loggers = defaultdict(MagicMock)
    with (
        patch("dbx_flame.entrypoints.export._write_csv", truncated),
        pytest.raises(RuntimeError, match="1 object\\(s\\): sites"),
    ):
        export_objects(
            spark, f"`{schema}`", _config(schema, tmp_path, ["SITES"]), STAMP, loggers.__getitem__
        )

    logger = loggers[f"`{schema}`.`sites`"]
    logger.error.assert_called_once()  # the policy's row, not a second object_export_failure
    assert logger.error.call_args.kwargs["name"] == "export_row_count_mismatch"
    assert "holds 0 rows" in logger.error.call_args.kwargs["description"]
    logger.kpi.assert_not_called()
    assert _manifest(tmp_path / schema) == {}


def test_export_warns_of_a_table_it_cannot_pin(spark, schema, tmp_path, object_types):
    """A foreign table, like a view, is exported as read; unlike a view, it says so."""
    object_types["sites"] = ("FOREIGN", None)

    folder, loggers = _export(spark, schema, tmp_path, tables=["SITES"])

    assert _manifest(folder)["SITES"][:3] == ["FOREIGN", "", "1"]
    warning = loggers[f"`{schema}`.`sites`"].warning.call_args.kwargs
    assert warning["name"] == "object_not_versioned"


@patch("dbx_flame.entrypoints.export.export_objects", side_effect=RuntimeError("boom"))
@patch("dbx_flame.entrypoints.export.AuditLogger")
@patch("dbx_flame.entrypoints.export.SparkSession")
def test_export_failure_is_logged_and_flushed_before_it_propagates(_spark, logger_cls, _export):
    with patch("sys.argv", _argv(PARAMS)), pytest.raises(RuntimeError, match="boom"):
        main()

    logger = logger_cls.return_value
    assert logger.error.call_args.kwargs["name"] == "export_failure"
    assert logger.flush.call_count == 2


@pytest.mark.parametrize(
    ("timezone", "offsets"),
    [("UTC", {"+0000"}), ("Europe/Berlin", {"+0100", "+0200"})],
)
@patch("dbx_flame.entrypoints.export.export_objects")
@patch("dbx_flame.entrypoints.export.AuditLogger")
@patch("dbx_flame.entrypoints.export.SparkSession")
def test_export_takes_the_stamp_in_its_timezone(_spark, _logger, export, timezone, offsets):
    params = {**PARAMS, "export.stamp_format": "%Y%m%dT%H%M%S%z", "export.timezone": timezone}

    with patch("sys.argv", _argv(params)):
        main()

    stamp = export.call_args.args[3]
    assert stamp[-5:] in offsets


def test_export_config_loads_a_comma_separated_subset():
    config = load_config({**PARAMS, "export.tables": "SITES, SUBJECTS"}, ExportTaskConfig)

    assert config.export.tables == ["SITES", "SUBJECTS"]


@pytest.mark.parametrize(
    ("key", "value", "message"),
    [
        ("export.stamp_format", "YYYYMMDD", "strftime format"),
        ("export.stamp_format", "%Y/%m/%d", "strftime format"),
        ("export.preparation_path", "/Volumes/other_dev_01/metadata/exports/", "Volume"),
        # A run clears {preparation_path}/{schema}: never directly under a catalog.
        ("export.preparation_path", "/Volumes/codex_dev_01/metadata", "Volume"),
        ("export.unknown", "x", "Unknown parameter: 'export.unknown'"),
        ("export.name", "cro", "UPPERCASE file name part"),
        ("export.name", "IC/ON", "UPPERCASE file name part"),
        ("export.timezone", "Mars/Olympus", "IANA time zone"),
        ("export.timezone", "../etc", "IANA time zone"),
        ("export.checksum", "crc32", "'md5', 'sha256' or 'sha512'"),
    ],
)
def test_export_config_rejects(key, value, message):
    with pytest.raises(ConfigValidationError, match=message):
        load_config({**PARAMS, key: value}, ExportTaskConfig)


def test_export_config_loads_excludes_and_renames_per_object():
    params = {
        **PARAMS,
        "export.excludes.SITES": "COUNTRY, REGION",
        "export.renames.SITES.ID": "SITE_ID",
        "export.exclude_metadata": "false",
    }

    config = load_config(params, ExportTaskConfig).export

    assert config.excludes == {"SITES": ["COUNTRY", "REGION"]}
    assert config.renames == {"SITES": {"ID": "SITE_ID"}}
    assert config.exclude_metadata is False


@pytest.mark.parametrize(
    ("settings", "message"),
    [
        (
            {"export.excludes.SITES": "ID", "export.renames.sites.ID": "SITE_ID"},
            "export.renames.sites renames excluded column\\(s\\): ID",
        ),
        # Framework columns are excluded by default.
        ({"export.renames.SITES.__SOURCE": "FILE"}, "renames excluded column\\(s\\): __SOURCE"),
        ({"export.renames.SITES.ID": " "}, "gives no new name for: ID"),
    ],
)
def test_export_config_rejects_renames(settings, message):
    with pytest.raises(ConfigValidationError, match=message):
        load_config({**PARAMS, **settings}, ExportTaskConfig)


def test_export_config_renames_framework_columns_it_keeps():
    params = {
        **PARAMS,
        "export.exclude_metadata": "false",
        "export.renames.SITES.__SOURCE": "FILE",
    }

    assert load_config(params, ExportTaskConfig).export.renames == {"SITES": {"__SOURCE": "FILE"}}


@pytest.mark.parametrize("key", ["export.name", "export.checksum"])
def test_export_config_requires(key):
    params = {name: value for name, value in PARAMS.items() if name != key}

    with pytest.raises(ConfigValidationError, match=key):
        load_config(params, ExportTaskConfig)
