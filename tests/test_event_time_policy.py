"""Tests for the event_time_invalid platform policy (CODE_REVIEW finding 2)."""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from dbx_flame.context.config import DedupConfig, EventTimeConfig, OutputConfig, Verb
from dbx_flame.output.mechanics import compared_times, require_valid_times
from dbx_flame.policies.platform import PlatformPolicyViolation

FORMAT = "yyyy-MM-dd HH:mm:ss"
UPDATED = [("output.event_time.column", EventTimeConfig(column="UPDATED", format=FORMAT))]


@pytest.fixture
def ctx() -> MagicMock:
    return MagicMock()


def _batch(spark, values, kind="string"):
    return spark.createDataFrame(
        [(str(i), value) for i, value in enumerate(values)], f"ID string, UPDATED {kind}"
    )


def test_valid_times_pass(spark, ctx):
    require_valid_times(_batch(spark, ["2026-01-01 00:00:00"]), ctx, UPDATED)

    ctx.logger.error.assert_not_called()


def test_null_and_blank_times_count_as_missing(spark, ctx):
    df = _batch(spark, ["2026-01-01 00:00:00", None, "  "])

    with pytest.raises(PlatformPolicyViolation, match=r"UPDATED: 2 row\(s\) missing; nothing"):
        require_valid_times(df, ctx, UPDATED)


def test_an_unparseable_time_names_the_format_and_an_example(spark, ctx):
    with pytest.raises(
        PlatformPolicyViolation,
        match=r"1 row\(s\) don't parse with format yyyy-MM-dd HH:mm:ss \(e\.g\. soon\)",
    ):
        require_valid_times(_batch(spark, ["soon"]), ctx, UPDATED)


def test_a_typed_timestamp_is_checked_for_null(spark, ctx):
    df = _batch(spark, [datetime(2026, 1, 1), None], kind="timestamp")
    unformatted = [("output.event_time.column", EventTimeConfig(column="UPDATED"))]

    with pytest.raises(PlatformPolicyViolation, match=r"1 row\(s\) missing"):
        require_valid_times(df, ctx, unformatted)


def test_a_time_column_absent_from_the_batch_fails(spark, ctx):
    df = spark.createDataFrame([("1",)], "ID string")

    with pytest.raises(PlatformPolicyViolation, match="UPDATED is not in the batch"):
        require_valid_times(df, ctx, UPDATED)


def test_the_violation_is_audited_with_counts(spark, ctx):
    with pytest.raises(PlatformPolicyViolation):
        require_valid_times(_batch(spark, [None, "soon"]), ctx, UPDATED)

    logged = ctx.logger.error.call_args.kwargs
    assert logged["name"] == "event_time_invalid"
    assert logged["total"] == 2
    assert json.loads(logged["metadata"]) == {
        "output.event_time.column": {"column": "UPDATED", "missing": 1, "unparseable": 1}
    }
    ctx.logger.flush.assert_called_once()


def _ctx_with(output: OutputConfig) -> MagicMock:
    ctx = MagicMock()
    ctx.config.output = output
    return ctx


def test_compared_times_cover_the_event_time_and_the_dedup_order():
    output = OutputConfig(
        verb=Verb.UPSERT,
        schema_name="silver",
        table="TARGET",
        keys=["ID"],
        event_time=EventTimeConfig(column="UPDATED", format=FORMAT),
        dedup=DedupConfig(order_by="LOADED", order_by_format="yyyyMMdd"),
    )

    assert compared_times(_ctx_with(output)) == [
        ("output.event_time.column", EventTimeConfig(column="UPDATED", format=FORMAT)),
        ("output.dedup.order_by", EventTimeConfig(column="LOADED", format="yyyyMMdd")),
    ]


def test_a_disabled_dedup_orders_nothing():
    output = OutputConfig(
        verb=Verb.UPSERT,
        schema_name="silver",
        table="TARGET",
        keys=["ID"],
        dedup=DedupConfig(enabled=False, order_by="LOADED"),
    )

    assert compared_times(_ctx_with(output)) == []
