"""Tests for pipelines.csv_source — Auto Loader options, and the CSV flow end to end."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from dbx_flame.context.config import (
    Origin,
    OutputConfig,
    SchemaEvolution,
    SourceConfig,
    SourceOptions,
    TaskConfig,
    TypingConfig,
    Verb,
)
from dbx_flame.context.context import RunIdentity
from dbx_flame.context.loader import build_context
from dbx_flame.pipelines.csv_source import CsvSource, reader_options
from dbx_flame.pipelines.enrichment import add_provenance, sanitize_column_names
from dbx_flame.typecast.service import CastService

RUN = RunIdentity(
    workflow_id="wf-1",
    workflow_run_id="wfrun-1",
    task_key="inbound_to_bronze",
    task_run_id="taskrun-1",
)


@pytest.fixture
def mock_spark() -> MagicMock:
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    return spark


def _context(mock_spark, schema_evolution=None, **source_overrides):
    source = dict(origin=Origin.CSV, path="/Volumes/in/", directory="agents")
    source.update(source_overrides)
    config = TaskConfig(
        catalog="cro",
        env="dev_01",
        metadata_path="/Volumes/meta/",
        **({"schema_evolution": schema_evolution} if schema_evolution else {}),
        source=SourceConfig(**source),
        output=OutputConfig(verb=Verb.APPEND, schema_name="bronze_cro", table="AGENTS"),
    )
    return build_context(config, mock_spark, RUN)


def test_default_options(mock_spark):
    options = reader_options(_context(mock_spark))

    assert options["cloudFiles.format"] == "csv"
    assert options["ignoreMissingFiles"] == "true"
    assert options["header"] == "true"
    assert options["delimiter"] == ","
    assert options["multiLine"] == "true"
    assert options["quote"] == '"'
    assert options["escape"] == "\\"


def test_schema_location_points_at_the_resolved_hints_path(mock_spark):
    ctx = _context(mock_spark)

    assert reader_options(ctx)["cloudFiles.schemaLocation"] == ctx.schema_hints_location
    assert ctx.schema_hints_location.endswith("_schema_hints/")


@pytest.mark.parametrize(
    "evolution,expected",
    [
        (SchemaEvolution.FAIL_ON_NEW_COLUMNS, "failOnNewColumns"),
        (SchemaEvolution.ADD_NEW_COLUMNS, "addNewColumns"),
        (
            SchemaEvolution.ADD_NEW_COLUMNS_WITH_TYPE_WIDENING,
            "addNewColumnsWithTypeWidening",
        ),
        (SchemaEvolution.NONE, "none"),
    ],
)
def test_schema_evolution_maps_to_the_auto_loader_mode(mock_spark, evolution, expected):
    ctx = _context(mock_spark, schema_evolution=evolution)

    assert reader_options(ctx)["cloudFiles.schemaEvolutionMode"] == expected


def test_configured_options_override_the_defaults(mock_spark):
    ctx = _context(
        mock_spark,
        options=SourceOptions(header=False, delimiter="|", multiline=False, quote="'"),
    )

    options = reader_options(ctx)

    assert options["header"] == "false"
    assert options["delimiter"] == "|"
    assert options["multiLine"] == "false"
    assert options["quote"] == "'"


def test_a_txt_source_still_uses_the_csv_reader(mock_spark):
    ctx = _context(mock_spark, file_extension="txt")

    assert reader_options(ctx)["cloudFiles.format"] == "csv"
    assert ctx.inbound_glob.endswith("*.txt")


def test_read_streams_from_the_resolved_glob(mock_spark):
    ctx = _context(mock_spark)

    CsvSource().read(ctx)

    mock_spark.readStream.format.assert_called_once_with("cloudFiles")
    loaded = mock_spark.readStream.format.return_value.options.return_value.load
    loaded.assert_called_once_with(ctx.inbound_glob)


def test_a_csv_fixture_flows_to_a_typed_dataframe(spark, mock_spark, tmp_path):
    """P2 exit criterion: source file to typed DataFrame with provenance and sanitization.

    Read with the plain csv reader rather than Auto Loader — cloudFiles does not
    exist off Databricks, and everything downstream of the reader is identical.
    """
    ctx = _context(mock_spark)
    ctx_with_logger = MagicMock()
    fixture = tmp_path / "AGENTS_20240115103000.csv"
    fixture.write_text("agent id,Full Name,age\n1,alice,42\n", encoding="utf-8")

    raw = spark.read.option("header", "true").csv(str(fixture))
    enriched = sanitize_column_names(add_provenance(raw, ctx), ctx_with_logger)
    typed = CastService().apply(enriched, ctx.config.typing, ctx_with_logger)

    row = typed.collect()[0]
    assert typed.columns == [
        "AGENT_ID",
        "FULL_NAME",
        "AGE",
        "__BRONZE_LAST_MODIFIED_DT",
        "__FILEPATH",
        "__EXPORT_DATE",
    ]
    assert row["AGENT_ID"] == "1"
    assert row["FULL_NAME"] == "alice"
    assert row["__EXPORT_DATE"] == datetime(2024, 1, 15, 10, 30)
    assert row["__FILEPATH"].endswith("AGENTS_20240115103000.csv")


def test_a_cast_config_applies_to_the_same_flow(spark, mock_spark, tmp_path):
    fixture = tmp_path / "AGENTS_20240115103000.csv"
    fixture.write_text("agent id,age\n1,42\n", encoding="utf-8")
    cast_config = tmp_path / "cast.yml"
    cast_config.write_text(
        "columns:\n  - name: AGE\n    cast:\n      target_type: int\n", encoding="utf-8"
    )
    logger_ctx = MagicMock()

    raw = spark.read.option("header", "true").csv(str(fixture))
    enriched = sanitize_column_names(add_provenance(raw, _context(mock_spark)), logger_ctx)
    typed = CastService().apply(enriched, TypingConfig(cast_config=str(cast_config)), logger_ctx)

    assert dict(typed.dtypes)["AGE"] == "int"
    assert typed.collect()[0]["AGE"] == 42
