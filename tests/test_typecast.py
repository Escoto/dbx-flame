"""Tests for typecast — cast resolution, ordering, and silent-NULL validation."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from dbx_flame.context.table_config import TableConfig
from dbx_flame.policies.platform import PlatformPolicyViolation
from dbx_flame.typecast.service import (
    CastService,
    MissingColumnException,
)


def _ctx(*columns: dict[str, Any], validate_casts: bool = True) -> MagicMock:
    """The cast service reads only the table_config, typing.validate_casts and the logger."""
    ctx = MagicMock()
    ctx.table_config = TableConfig(columns=list(columns)) if columns else None
    ctx.config.table_config = "table.yml"
    ctx.config.typing.validate_casts = validate_casts
    return ctx


def _cast(name: str, target_type: str, format: str | None = None) -> dict[str, Any]:
    cast = {"target_type": target_type, **({"format": format} if format else {})}
    return {"name": name, "cast": cast}


@pytest.fixture
def ctx() -> MagicMock:
    """A task that names no table_config."""
    return _ctx()


@pytest.fixture
def service() -> CastService:
    return CastService()


def _typed_batch(spark):
    """A batch that already carries real types, as a table source hands one over."""
    return spark.createDataFrame(
        [("1", 42, Decimal("1.50"), datetime(2024, 1, 15, 10, 30))],
        "ID string, AGE int, SCORE decimal(10,2), UPDATED timestamp",
    )


def _people(spark):
    """A typical post-enrichment batch: string payload plus framework metadata."""
    return spark.createDataFrame(
        [("1", "alice", "42", datetime(2024, 1, 15, 10, 30))],
        "ID string, NAME string, AGE string, __EXPORT_DATE timestamp",
    )


def test_without_a_config_a_string_batch_stays_a_string_batch(spark, service, ctx):
    """The Auto Loader case: nothing is declared, and nothing needs to change."""
    result = service.apply(_people(spark), ctx)

    types = dict(result.dtypes)
    assert types["ID"] == "string"
    assert types["NAME"] == "string"
    assert types["AGE"] == "string"


def test_without_a_config_an_already_typed_batch_keeps_its_types(spark, service, ctx):
    """Promotion must not undo the previous layer's types.

    Bronze declares the types once; a Bronze-to-Silver task that declares nothing is
    saying "no changes here", not "flatten everything back to string". Forcing string
    would also be impossible to work around, because re-declaring the same config at
    Silver would re-parse an already-typed timestamp with its source format and fail.
    """
    typed = _typed_batch(spark)

    result = service.apply(typed, ctx)

    assert dict(result.dtypes) == dict(typed.dtypes)


def test_metadata_columns_keep_their_type_on_the_default_path(spark, service, ctx):
    """Framework metadata columns are exempt: the framework owns their types."""
    result = service.apply(_people(spark), ctx)

    assert dict(result.dtypes)["__EXPORT_DATE"] == "timestamp"


def test_configured_columns_come_first_in_yaml_order(spark, service):
    ctx = _ctx(_cast("AGE", "int"), _cast("ID", "int"))

    result = service.apply(_people(spark), ctx)

    assert result.columns == ["AGE", "ID", "NAME", "__EXPORT_DATE"]


def test_a_column_with_only_a_comment_is_ordered_but_keeps_its_type(spark, service):
    """The file doubles as a column order, whatever a column's entry sets."""
    ctx = _ctx({"name": "NAME", "comment": "Full name."}, _cast("AGE", "int"))

    result = service.apply(_people(spark), ctx)

    assert result.columns == ["NAME", "AGE", "ID", "__EXPORT_DATE"]
    assert dict(result.dtypes)["NAME"] == "string"


def test_unconfigured_columns_trail_with_the_type_they_arrived_with(spark, service):
    ctx = _ctx(_cast("AGE", "int"))

    result = service.apply(_people(spark), ctx)

    types = dict(result.dtypes)
    assert types["AGE"] == "int"
    assert types["NAME"] == "string"


def test_a_partial_config_leaves_the_columns_it_does_not_name_alone(spark, service):
    """Declaring one column says nothing about the others."""
    ctx = _ctx(_cast("AGE", "bigint"))

    result = service.apply(_typed_batch(spark), ctx)

    types = dict(result.dtypes)
    assert types["AGE"] == "bigint"
    assert types["SCORE"] == "decimal(10,2)"
    assert types["UPDATED"] == "timestamp"


def test_a_configured_column_the_source_lacks_is_fatal(spark, service):
    ctx = _ctx(_cast("MISSING", "int"))

    with pytest.raises(MissingColumnException, match="table.yml.*MISSING"):
        service.apply(_people(spark), ctx)


def test_a_commented_column_the_source_lacks_is_fatal_too(spark, service):
    """Caught here, before the write, rather than when the comment is set after it."""
    ctx = _ctx({"name": "MISSING", "tags": {"PII": "no"}})

    with pytest.raises(MissingColumnException, match="MISSING"):
        service.apply(_people(spark), ctx)


def test_date_targets_honour_the_configured_format(spark, service):
    df = spark.createDataFrame([("15/01/2024",)], "STARTED string")
    ctx = _ctx(_cast("STARTED", "date", "dd/MM/yyyy"))

    result = service.apply(df, ctx)

    assert result.collect()[0]["STARTED"] == date(2024, 1, 15)


def test_timestamp_targets_honour_the_configured_format(spark, service):
    df = spark.createDataFrame([("20240115103000",)], "SEEN string")
    ctx = _ctx(_cast("SEEN", "timestamp", "yyyyMMddHHmmss"))

    result = service.apply(df, ctx)

    assert result.collect()[0]["SEEN"] == datetime(2024, 1, 15, 10, 30)


def test_a_spark_ddl_type_is_passed_through_to_cast(spark, service):
    df = spark.createDataFrame([("12.34",)], "AMOUNT string")

    result = service.apply(df, _ctx(_cast("AMOUNT", "decimal(10,2)")))

    assert dict(result.dtypes)["AMOUNT"] == "decimal(10,2)"


def test_a_value_silently_cast_to_null_fails_the_run(spark, service):
    df = spark.createDataFrame([("1", "abc")], "ID string, AGE string")
    ctx = _ctx(_cast("AGE", "int"))

    with pytest.raises(PlatformPolicyViolation, match="AGE"):
        service.apply(df, ctx)

    logged = ctx.logger.error.call_args
    assert logged.kwargs["name"] == "cast_silent_null"
    assert json.loads(logged.kwargs["metadata"]) == {"AGE": "abc"}


def test_every_failing_column_is_named_however_noisy_its_neighbour(spark, service):
    """A column used to be missed when a noisier one filled the shared row budget."""
    rows = [(f"bad_a_{i}", "1") for i in range(12)] + [("1", "bad_b_0")]
    df = spark.createDataFrame(rows, "A string, B string").coalesce(1)
    ctx = _ctx(_cast("A", "int"), _cast("B", "int"))

    with pytest.raises(PlatformPolicyViolation) as failure:
        service.apply(df, ctx)

    reported = json.loads(ctx.logger.error.call_args.kwargs["metadata"])
    assert sorted(reported) == ["A", "B"]
    assert reported["B"] == "bad_b_0"
    assert "A" in str(failure.value) and "B" in str(failure.value)


def test_one_bad_value_is_enough_to_condemn_a_column(spark, service):
    """The row count is deliberately not gathered: total is the number of bad columns."""
    rows = [("1", "1")] * 500 + [("nope", "1")]
    df = spark.createDataFrame(rows, "A string, B string")
    ctx = _ctx(_cast("A", "int"))

    with pytest.raises(PlatformPolicyViolation):
        service.apply(df, ctx)

    logged = ctx.logger.error.call_args.kwargs
    assert logged["total"] == 1
    assert json.loads(logged["metadata"]) == {"A": "nope"}


def test_a_genuinely_empty_value_is_not_a_silent_null(spark, service):
    """An empty string was never a value, so casting it to NULL is not data loss."""
    df = spark.createDataFrame([("1", "  ")], "ID string, AGE string")

    result = service.apply(df, _ctx(_cast("AGE", "int")))

    assert result.collect()[0]["AGE"] is None


def test_validation_can_be_switched_off(spark, service):
    df = spark.createDataFrame([("1", "abc")], "ID string, AGE string")
    ctx = _ctx(_cast("AGE", "int"), validate_casts=False)

    result = service.apply(df, ctx)

    assert result.collect()[0]["AGE"] is None
    ctx.logger.error.assert_not_called()


def test_the_comparison_columns_never_reach_the_caller(spark, service):
    """_orig_* exists only inside the batch, and is gone before the write."""
    df = spark.createDataFrame([("1", "42")], "ID string, AGE string")

    result = service.apply(df, _ctx(_cast("AGE", "int")))

    assert result.columns == ["AGE", "ID"]


def test_a_production_shaped_cast_config(spark, service):
    """The shape real files use: decimals with precision, a date format with spaces."""
    ctx = _ctx(
        _cast("METRICSEQ", "decimal(10,2)"),
        _cast("YEAR", "decimal(8,0)"),
        _cast("METRICVALUE", "decimal(10,2)"),
        _cast("REFRESHDATE", "timestamp", "MM/dd/yyyy HH:mm:ss"),
    )
    df = spark.createDataFrame(
        [("1234.56", "2024", "99.10", "03/15/2024 14:30:00", "EU")],
        "METRICSEQ string, YEAR string, METRICVALUE string, REFRESHDATE string, REGION string",
    )

    result = service.apply(df, ctx)

    assert result.dtypes == [
        ("METRICSEQ", "decimal(10,2)"),
        ("YEAR", "decimal(8,0)"),
        ("METRICVALUE", "decimal(10,2)"),
        ("REFRESHDATE", "timestamp"),
        ("REGION", "string"),
    ]
    assert result.collect()[0]["REFRESHDATE"] == datetime(2024, 3, 15, 14, 30)
    ctx.logger.error.assert_not_called()
