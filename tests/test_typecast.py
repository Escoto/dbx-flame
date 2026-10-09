"""Tests for typecast — cast resolution, ordering, and silent-NULL validation."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from dbx_flame.context.config import TypingConfig
from dbx_flame.policies.platform import PlatformPolicyViolation
from dbx_flame.typecast.models import load_cast_configuration
from dbx_flame.typecast.service import (
    CastService,
    MissingColumnException,
)


@pytest.fixture
def ctx() -> MagicMock:
    """Only ctx.logger is touched by the cast service."""
    return MagicMock()


@pytest.fixture
def service() -> CastService:
    return CastService()


def _cast_config(tmp_path, body: str) -> str:
    path = tmp_path / "cast.yml"
    path.write_text(body, encoding="utf-8")
    return str(path)


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
    result = service.apply(_people(spark), TypingConfig(), ctx)

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

    result = service.apply(typed, TypingConfig(), ctx)

    assert dict(result.dtypes) == dict(typed.dtypes)


def test_metadata_columns_keep_their_type_on_the_default_path(spark, service, ctx):
    """Framework metadata columns are exempt: the framework owns their types."""
    result = service.apply(_people(spark), TypingConfig(), ctx)

    assert dict(result.dtypes)["__EXPORT_DATE"] == "timestamp"


def test_configured_columns_come_first_in_yaml_order(spark, service, ctx, tmp_path):
    config = _cast_config(
        tmp_path,
        "columns:\n"
        "  - name: AGE\n"
        "    cast:\n"
        "      target_type: int\n"
        "  - name: ID\n"
        "    cast:\n"
        "      target_type: int\n",
    )

    result = service.apply(_people(spark), TypingConfig(cast_config=config), ctx)

    assert result.columns == ["AGE", "ID", "NAME", "__EXPORT_DATE"]


def test_unconfigured_columns_trail_with_the_type_they_arrived_with(spark, service, ctx, tmp_path):
    config = _cast_config(
        tmp_path,
        "columns:\n  - name: AGE\n    cast:\n      target_type: int\n",
    )

    result = service.apply(_people(spark), TypingConfig(cast_config=config), ctx)

    types = dict(result.dtypes)
    assert types["AGE"] == "int"
    assert types["NAME"] == "string"


def test_a_partial_config_leaves_the_columns_it_does_not_name_alone(spark, service, ctx, tmp_path):
    """Declaring one column says nothing about the others."""
    config = _cast_config(
        tmp_path,
        "columns:\n  - name: AGE\n    cast:\n      target_type: bigint\n",
    )

    result = service.apply(_typed_batch(spark), TypingConfig(cast_config=config), ctx)

    types = dict(result.dtypes)
    assert types["AGE"] == "bigint"
    assert types["SCORE"] == "decimal(10,2)"
    assert types["UPDATED"] == "timestamp"


def test_a_configured_column_the_source_lacks_is_fatal(spark, service, ctx, tmp_path):
    config = _cast_config(
        tmp_path,
        "columns:\n  - name: MISSING\n    cast:\n      target_type: int\n",
    )

    with pytest.raises(MissingColumnException, match="MISSING"):
        service.apply(_people(spark), TypingConfig(cast_config=config), ctx)


def test_a_repeated_column_takes_the_last_cast(spark, service, ctx, tmp_path):
    config = _cast_config(
        tmp_path,
        "columns:\n"
        "  - name: AGE\n"
        "    cast:\n"
        "      target_type: string\n"
        "  - name: AGE\n"
        "    cast:\n"
        "      target_type: int\n",
    )

    result = service.apply(_people(spark), TypingConfig(cast_config=config), ctx)

    assert dict(result.dtypes)["AGE"] == "int"


def test_date_targets_honour_the_configured_format(spark, service, ctx, tmp_path):
    df = spark.createDataFrame([("15/01/2024",)], "STARTED string")
    config = _cast_config(
        tmp_path,
        "columns:\n"
        "  - name: STARTED\n"
        "    cast:\n"
        "      target_type: date\n"
        '      format: "dd/MM/yyyy"\n',
    )

    result = service.apply(df, TypingConfig(cast_config=config), ctx)

    assert result.collect()[0]["STARTED"] == date(2024, 1, 15)


def test_timestamp_targets_honour_the_configured_format(spark, service, ctx, tmp_path):
    df = spark.createDataFrame([("20240115103000",)], "SEEN string")
    config = _cast_config(
        tmp_path,
        "columns:\n"
        "  - name: SEEN\n"
        "    cast:\n"
        "      target_type: timestamp\n"
        '      format: "yyyyMMddHHmmss"\n',
    )

    result = service.apply(df, TypingConfig(cast_config=config), ctx)

    assert result.collect()[0]["SEEN"] == datetime(2024, 1, 15, 10, 30)


def test_a_spark_ddl_type_is_passed_through_to_cast(spark, service, ctx, tmp_path):
    df = spark.createDataFrame([("12.34",)], "AMOUNT string")
    config = _cast_config(
        tmp_path,
        "columns:\n  - name: AMOUNT\n    cast:\n      target_type: decimal(10,2)\n",
    )

    result = service.apply(df, TypingConfig(cast_config=config), ctx)

    assert dict(result.dtypes)["AMOUNT"] == "decimal(10,2)"


def test_a_metadata_column_in_the_config_is_left_alone_and_flagged(spark, service, ctx, tmp_path):
    config = _cast_config(
        tmp_path,
        "columns:\n  - name: __EXPORT_DATE\n    cast:\n      target_type: string\n",
    )

    result = service.apply(_people(spark), TypingConfig(cast_config=config), ctx)

    assert dict(result.dtypes)["__EXPORT_DATE"] == "timestamp"
    warning = ctx.logger.warning.call_args
    assert warning.kwargs["name"] == "cast_config_ignored_metadata"
    assert "__EXPORT_DATE" in warning.kwargs["description"]


def test_a_value_silently_cast_to_null_fails_the_run(spark, service, ctx, tmp_path):
    df = spark.createDataFrame([("1", "abc")], "ID string, AGE string")
    config = _cast_config(
        tmp_path,
        "columns:\n  - name: AGE\n    cast:\n      target_type: int\n",
    )

    with pytest.raises(PlatformPolicyViolation, match="AGE"):
        service.apply(df, TypingConfig(cast_config=config), ctx)

    logged = ctx.logger.error.call_args
    assert logged.kwargs["name"] == "cast_silent_null"
    assert json.loads(logged.kwargs["metadata"]) == {"AGE": "abc"}


def test_every_failing_column_is_named_however_noisy_its_neighbour(spark, service, ctx, tmp_path):
    """A column used to be missed when a noisier one filled the shared row budget."""
    rows = [(f"bad_a_{i}", "1") for i in range(12)] + [("1", "bad_b_0")]
    df = spark.createDataFrame(rows, "A string, B string").coalesce(1)
    config = _cast_config(
        tmp_path,
        """columns:
  - name: A
    cast:
      target_type: int
  - name: B
    cast:
      target_type: int
""",
    )

    with pytest.raises(PlatformPolicyViolation) as failure:
        service.apply(df, TypingConfig(cast_config=config), ctx)

    reported = json.loads(ctx.logger.error.call_args.kwargs["metadata"])
    assert sorted(reported) == ["A", "B"]
    assert reported["B"] == "bad_b_0"
    assert "A" in str(failure.value) and "B" in str(failure.value)


def test_one_bad_value_is_enough_to_condemn_a_column(spark, service, ctx, tmp_path):
    """The row count is deliberately not gathered: total is the number of bad columns."""
    rows = [("1", "1")] * 500 + [("nope", "1")]
    df = spark.createDataFrame(rows, "A string, B string")
    config = _cast_config(
        tmp_path,
        """columns:
  - name: A
    cast:
      target_type: int
""",
    )

    with pytest.raises(PlatformPolicyViolation):
        service.apply(df, TypingConfig(cast_config=config), ctx)

    logged = ctx.logger.error.call_args.kwargs
    assert logged["total"] == 1
    assert json.loads(logged["metadata"]) == {"A": "nope"}


def test_a_genuinely_empty_value_is_not_a_silent_null(spark, service, ctx, tmp_path):
    """An empty string was never a value, so casting it to NULL is not data loss."""
    df = spark.createDataFrame([("1", "  ")], "ID string, AGE string")
    config = _cast_config(
        tmp_path,
        "columns:\n  - name: AGE\n    cast:\n      target_type: int\n",
    )

    result = service.apply(df, TypingConfig(cast_config=config), ctx)

    assert result.collect()[0]["AGE"] is None


def test_validation_can_be_switched_off(spark, service, ctx, tmp_path):
    df = spark.createDataFrame([("1", "abc")], "ID string, AGE string")
    config = _cast_config(
        tmp_path,
        "columns:\n  - name: AGE\n    cast:\n      target_type: int\n",
    )

    result = service.apply(df, TypingConfig(cast_config=config, validate_casts=False), ctx)

    assert result.collect()[0]["AGE"] is None
    ctx.logger.error.assert_not_called()


def test_the_comparison_columns_never_reach_the_caller(spark, service, ctx, tmp_path):
    """_orig_* exists only inside the batch, and is gone before the write."""
    df = spark.createDataFrame([("1", "42")], "ID string, AGE string")
    config = _cast_config(
        tmp_path,
        "columns:\n  - name: AGE\n    cast:\n      target_type: int\n",
    )

    result = service.apply(df, TypingConfig(cast_config=config), ctx)

    assert result.columns == ["AGE", "ID"]


def test_an_unknown_key_in_the_cast_yaml_is_rejected(tmp_path):
    """enable_validation moved to typing.validate_casts; a stale one must not be ignored."""
    path = _cast_config(tmp_path, "enable_validation: false\ncolumns: []\n")

    with pytest.raises(ValueError, match="Invalid cast config"):
        load_cast_configuration(path)


def test_an_empty_cast_yaml_loads_as_no_columns(tmp_path):
    path = _cast_config(tmp_path, "")

    assert load_cast_configuration(path).columns == []


def test_a_production_shaped_cast_config(spark, service, ctx, tmp_path):
    """The shape real cast files use: decimals with precision, an unquoted date format."""
    config = _cast_config(
        tmp_path,
        "columns:\n"
        "  - name: METRICSEQ\n"
        "    cast:\n"
        "      target_type: decimal(10,2)\n"
        "  - name: YEAR\n"
        "    cast:\n"
        "      target_type: decimal(8,0)\n"
        "  - name: METRICVALUE\n"
        "    cast:\n"
        "      target_type: decimal(10,2)\n"
        "  - name: REFRESHDATE\n"
        "    cast:\n"
        "      target_type: timestamp\n"
        "      format: MM/dd/yyyy HH:mm:ss\n",
    )
    df = spark.createDataFrame(
        [("1234.56", "2024", "99.10", "03/15/2024 14:30:00", "EU")],
        "METRICSEQ string, YEAR string, METRICVALUE string, REFRESHDATE string, REGION string",
    )

    result = service.apply(df, TypingConfig(cast_config=config), ctx)

    assert result.dtypes == [
        ("METRICSEQ", "decimal(10,2)"),
        ("YEAR", "decimal(8,0)"),
        ("METRICVALUE", "decimal(10,2)"),
        ("REFRESHDATE", "timestamp"),
        ("REGION", "string"),
    ]
    assert result.collect()[0]["REFRESHDATE"] == datetime(2024, 3, 15, 14, 30)
    ctx.logger.error.assert_not_called()
