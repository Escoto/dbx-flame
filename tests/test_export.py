"""Export: one CSV per object in the preparation path, and the export task's config."""

from __future__ import annotations

import csv
import os
import uuid
from collections import defaultdict
from unittest.mock import MagicMock, patch

import pytest

from dbx_flame.context.config import ExportConfig, ExportTaskConfig
from dbx_flame.context.loader import ConfigValidationError, load_config
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
}


@pytest.fixture
def schema(spark):
    name = f"test_export_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE `{name}`")
    spark.createDataFrame(
        [(1, 'say "hi"'), (2, "line\nbreak"), (3, "  padded  "), (4, None), (5, "")],
        "ID INT, TEXT STRING",
    ).write.saveAsTable(f"`{name}`.`SUBJECTS`")
    spark.createDataFrame([(10, "DE")], "ID INT, COUNTRY STRING").write.saveAsTable(
        f"`{name}`.`SITES`"
    )
    yield name
    spark.sql(f"DROP DATABASE IF EXISTS `{name}` CASCADE")


def _config(schema, tmp_path, tables=(), with_ts=False):
    return ExportConfig(
        name="CRO",
        schema_name=schema,
        tables=list(tables),
        preparation_path=str(tmp_path),
        stamp_format="%Y",
        with_ts_object_names=with_ts,
    )


def _export(spark, schema, tmp_path, tables=(), with_ts=False):
    loggers = defaultdict(MagicMock)
    config = _config(schema, tmp_path, tables, with_ts)
    export_objects(spark, f"`{schema}`", config, STAMP, loggers.__getitem__)
    return tmp_path / schema, loggers


def _argv(params):
    return ["dbx-flame-export"] + [f"--{key}={value}" for key, value in params.items()]


def test_export_writes_one_csv_per_object_of_the_schema(spark, schema, tmp_path):
    folder, loggers = _export(spark, schema, tmp_path)

    assert sorted(os.listdir(folder)) == ["SITES.csv", "SUBJECTS.csv"]
    kpi = loggers[f"`{schema}`.`subjects`"].kpi
    kpi.assert_called_once()
    assert kpi.call_args.args[:2] == (Kpi.ROWS_EXPORTED, 5)


def test_export_stamps_object_names_only_when_asked(spark, schema, tmp_path):
    folder, _ = _export(spark, schema, tmp_path, with_ts=True)

    assert sorted(os.listdir(folder)) == [f"SITES_{STAMP}.csv", f"SUBJECTS_{STAMP}.csv"]


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

    assert os.listdir(folder) == ["SITES.csv"]


def test_export_refuses_an_unknown_object_before_touching_the_folder(spark, schema, tmp_path):
    previous = tmp_path / schema / "previous_export.csv"
    previous.parent.mkdir()
    previous.write_text("kept")

    with pytest.raises(ValueError, match="not in .*: MISSING"):
        _export(spark, schema, tmp_path, tables=["SITES", "MISSING"])

    assert os.listdir(previous.parent) == ["previous_export.csv"]


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

    assert sorted(os.listdir(tmp_path / schema)) == ["SITES.csv", "SUBJECTS.csv"]
    error = loggers[f"`{schema}`.`broken`"].error
    assert error.call_args.kwargs["name"] == "object_export_failure"


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
    ],
)
def test_export_config_rejects(key, value, message):
    with pytest.raises(ConfigValidationError, match=message):
        load_config({**PARAMS, key: value}, ExportTaskConfig)


def test_export_config_requires_a_name():
    params = {key: value for key, value in PARAMS.items() if key != "export.name"}

    with pytest.raises(ConfigValidationError, match="export.name"):
        load_config(params, ExportTaskConfig)
