"""Tests for pipelines.json_source and the record_envelope pre-processor."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from dbx_flame.context.config import (
    Origin,
    OutputConfig,
    SchemaEvolution,
    SourceConfig,
    SourceOptions,
    TaskConfig,
    Verb,
)
from dbx_flame.context.context import RunIdentity
from dbx_flame.context.loader import build_context, validate_requirements
from dbx_flame.pipelines.enrichment import sanitize_column_names
from dbx_flame.pipelines.json_source import JsonSource, reader_options
from dbx_flame.pipelines.preprocessors import RecordEnvelope

RUN = RunIdentity(
    workflow_id="wf-1",
    workflow_run_id="wfrun-1",
    task_key="inbound_to_bronze",
    task_run_id="taskrun-1",
)

HINTS = "metadata STRING, data STRING"

# One export as the vendor sends it: a stamp with a timezone suffix the format string
# cannot parse, one item missing a key another has, and an array inside an item.
ENVELOPE = {
    "metadata": {"object_type": "MDM_FULL_EXPORT", "export_date": "20260101050000Z"},
    "data": [
        {"uri": "a/1", "status": "ACTIVE", "region": "Hubei", "related": ["x", "y"]},
        {"uri": "a/2", "status": "ACTIVE", "related": []},
    ],
}


@pytest.fixture
def mock_spark() -> MagicMock:
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    return spark


def _config(**source_overrides) -> TaskConfig:
    source = dict(origin=Origin.JSON, path="/Volumes/in/", directory="address")
    source.update(source_overrides)
    return TaskConfig(
        catalog="cro",
        env="dev_01",
        metadata_path="/Volumes/meta/",
        source=SourceConfig(**source),
        output=OutputConfig(verb=Verb.APPEND, schema_name="bronze_cro", table="ADDRESS"),
    )


def _envelope_ctx(fields: list[str]) -> MagicMock:
    """The pre-processor reads only the configured field list off the Context."""
    ctx = MagicMock()
    ctx.config.source.envelope_fields = fields
    return ctx


def _written(spark, tmp_path, payload: dict) -> str:
    path = str(tmp_path / "ADDRESS_FULL_20260101050000Z.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    return path


# ── reader options ───────────────────────────────────────────────────────────


def test_reader_options_carry_the_json_format_and_evolution_mode(mock_spark):
    ctx = build_context(_config(), mock_spark, RUN)

    options = reader_options(ctx)

    assert options["cloudFiles.format"] == "json"
    assert options["cloudFiles.schemaEvolutionMode"] == "failOnNewColumns"
    assert options["multiLine"] == "true"


def test_schema_hints_are_passed_through_when_configured(mock_spark):
    ctx = build_context(_config(options=SourceOptions(schema_hints=HINTS)), mock_spark, RUN)

    assert reader_options(ctx)["cloudFiles.schemaHints"] == HINTS


def test_schema_hints_are_absent_rather_than_empty_when_unset(mock_spark):
    """An empty hint string is not the same as no hint; Auto Loader would reject it."""
    ctx = build_context(_config(), mock_spark, RUN)

    assert "cloudFiles.schemaHints" not in reader_options(ctx)


def test_every_evolution_mode_has_an_auto_loader_spelling(mock_spark):
    for mode in SchemaEvolution:
        config = _config()
        config.schema_evolution = mode
        ctx = build_context(config, mock_spark, RUN)

        assert reader_options(ctx)["cloudFiles.schemaEvolutionMode"]


def test_read_streams_from_the_resolved_glob(mock_spark):
    ctx = build_context(_config(), mock_spark, RUN)

    JsonSource().read(ctx)

    mock_spark.readStream.format.assert_called_once_with("cloudFiles")
    loaded = mock_spark.readStream.format.return_value.options.return_value.load
    loaded.assert_called_once_with("/Volumes/in/address/*.[jJ][sS][oO][nN]")


# ── the envelope ─────────────────────────────────────────────────────────────


def test_envelope_yields_one_row_per_item(spark, tmp_path):
    raw = spark.read.option("multiLine", "true").json(_written(spark, tmp_path, ENVELOPE))

    unwrapped = RecordEnvelope().apply(raw, _envelope_ctx(["uri"]))

    assert unwrapped.count() == 2


def test_envelope_lifts_only_the_configured_fields(spark, tmp_path):
    raw = spark.read.option("multiLine", "true").json(_written(spark, tmp_path, ENVELOPE))

    unwrapped = RecordEnvelope().apply(raw, _envelope_ctx(["uri", "status"]))

    assert unwrapped.columns == ["uri", "status", "DATA", "EXPORT_DATE"]


def test_a_key_missing_from_one_item_is_null_rather_than_an_error(spark, tmp_path):
    raw = spark.read.option("multiLine", "true").json(_written(spark, tmp_path, ENVELOPE))

    rows = RecordEnvelope().apply(raw, _envelope_ctx(["uri", "region"])).collect()

    assert {row["uri"]: row["region"] for row in rows} == {"a/1": "Hubei", "a/2": None}


def test_the_payload_survives_whole_as_json_text(spark, tmp_path):
    raw = spark.read.option("multiLine", "true").json(_written(spark, tmp_path, ENVELOPE))

    rows = RecordEnvelope().apply(raw, _envelope_ctx(["uri"])).collect()
    payloads = {row["uri"]: json.loads(row["DATA"]) for row in rows}

    assert payloads["a/1"]["related"] == ["x", "y"]
    assert payloads["a/1"]["status"] == "ACTIVE"


def test_an_array_inside_an_item_does_not_multiply_rows(spark, tmp_path):
    """One row per data[] item — a nested array stays as its JSON text."""
    raw = spark.read.option("multiLine", "true").json(_written(spark, tmp_path, ENVELOPE))

    rows = RecordEnvelope().apply(raw, _envelope_ctx(["uri", "related"])).collect()

    assert len(rows) == 2
    assert json.loads([row for row in rows if row["uri"] == "a/1"][0]["related"]) == ["x", "y"]


def test_the_export_stamp_tolerates_a_timezone_suffix(spark, tmp_path):
    """to_timestamp on '20260101050000Z' throws; the 14-digit extract is what saves it."""
    raw = spark.read.option("multiLine", "true").json(_written(spark, tmp_path, ENVELOPE))

    rows = RecordEnvelope().apply(raw, _envelope_ctx(["uri"])).collect()

    assert str(rows[0]["EXPORT_DATE"]) == "2026-01-01 05:00:00"


def test_the_envelope_reads_a_hinted_string_column_too(spark, tmp_path):
    """With schema hints both halves arrive as strings; the output must not differ."""
    path = _written(spark, tmp_path, ENVELOPE)
    hinted = spark.read.option("multiLine", "true").schema(HINTS).json(path)
    inferred = spark.read.option("multiLine", "true").json(path)

    from_hinted = RecordEnvelope().apply(hinted, _envelope_ctx(["uri", "status"])).collect()
    from_inferred = RecordEnvelope().apply(inferred, _envelope_ctx(["uri", "status"])).collect()

    assert [(row["uri"], row["status"]) for row in from_hinted] == [
        (row["uri"], row["status"]) for row in from_inferred
    ]


def test_columns_added_before_the_envelope_are_carried_through(spark, tmp_path):
    """Provenance is attached upstream and COMPLETE_DELTA cuts snapshots from __FILEPATH."""
    from pyspark.sql import functions as F

    raw = spark.read.option("multiLine", "true").json(_written(spark, tmp_path, ENVELOPE))
    with_provenance = raw.withColumn("__FILEPATH", F.lit("/Volumes/in/address/x.json"))

    unwrapped = RecordEnvelope().apply(with_provenance, _envelope_ctx(["uri"]))

    assert "__FILEPATH" in unwrapped.columns
    assert unwrapped.select("__FILEPATH").distinct().count() == 1


def test_sanitization_uppercases_the_lifted_fields(spark, tmp_path):
    """The ruleset and the merge keys see URI, not uri."""
    raw = spark.read.option("multiLine", "true").json(_written(spark, tmp_path, ENVELOPE))
    unwrapped = RecordEnvelope().apply(raw, _envelope_ctx(["uri", "status"]))

    sanitized = sanitize_column_names(unwrapped, MagicMock())

    assert sanitized.columns == ["URI", "STATUS", "DATA", "EXPORT_DATE"]


# ── Start-time validation ────────────────────────────────────────────────────


def test_fields_without_the_envelope_are_refused():
    errors = validate_requirements(_config(envelope_fields=["uri"]))

    assert len(errors) == 1
    assert "record_envelope" in errors[0]


def test_the_envelope_without_fields_is_refused():
    errors = validate_requirements(_config(preprocessors=["record_envelope"]))

    assert len(errors) == 1
    assert "envelope_fields" in errors[0]


def test_the_pair_together_validates():
    config = _config(preprocessors=["record_envelope"], envelope_fields=["uri"])

    assert validate_requirements(config) == []


def test_neither_half_is_still_valid():
    """A JSON source that needs no unwrapping stays legal."""
    assert validate_requirements(_config()) == []
