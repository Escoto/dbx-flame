"""Tests for the policies layer — ruleset loading and the DQX gate."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dbx_flame.context.config import (
    Origin,
    OutputConfig,
    PoliciesConfig,
    Severity,
    SourceConfig,
    TaskConfig,
    TypingConfig,
    Verb,
)
from dbx_flame.context.context import RunIdentity
from dbx_flame.context.loader import ConfigValidationError, build_context
from dbx_flame.policies.base import PolicyViolation
from dbx_flame.policies.checks import ChecksValidationError, load_checks
from dbx_flame.policies.runner import PolicyRunner

RUN = RunIdentity(
    workflow_id="wf-1",
    workflow_run_id="wfrun-1",
    task_key="bronze_to_silver",
    task_run_id="taskrun-1",
)

ID_NOT_NULL = """
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: ID
"""

NAME_NOT_NULL_WARN = """
- criticality: warn
  check:
    function: is_not_null
    arguments:
      column: NAME
"""


def _ruleset(tmp_path, body: str, filename: str = "checks.yml") -> str:
    path = tmp_path / filename
    path.write_text(body, encoding="utf-8")
    return str(path)


def _batch(spark):
    """Two clean rows, one with a null ID, one with a null NAME."""
    return spark.createDataFrame(
        [(1, "a"), (2, "b"), (None, "c"), (3, None)],
        "ID int, NAME string",
    )


@pytest.fixture
def ctx() -> MagicMock:
    """The runner touches only ctx.checks, ctx.logger and ctx.target_table."""
    context = MagicMock()
    context.checks = []
    context.target_table = "`cro_dev_01`.`silver_cro`.`SUBJECTS`"
    return context


@pytest.fixture
def runner() -> PolicyRunner:
    return PolicyRunner()


# ── loading and validating the ruleset ───────────────────────────────────────


def test_load_checks_returns_the_declared_checks(tmp_path):
    checks = load_checks(_ruleset(tmp_path, ID_NOT_NULL))

    assert len(checks) == 1
    assert checks[0]["criticality"] == "error"
    assert checks[0]["check"]["function"] == "is_not_null"


def test_load_checks_rejects_an_unknown_check_function(tmp_path):
    body = """
- criticality: error
  check:
    function: is_definitely_not_a_check
    arguments:
      column: ID
"""
    with pytest.raises(ChecksValidationError) as excinfo:
        load_checks(_ruleset(tmp_path, body))

    assert "is_definitely_not_a_check" in str(excinfo.value)


def test_load_checks_rejects_a_misspelled_argument(tmp_path):
    body = """
- criticality: error
  check:
    function: is_not_null
    arguments:
      colunm: ID
"""
    with pytest.raises(ChecksValidationError):
        load_checks(_ruleset(tmp_path, body))


def test_load_checks_rejects_a_mapping_where_a_list_belongs(tmp_path):
    with pytest.raises(ChecksValidationError, match="must hold a list"):
        load_checks(_ruleset(tmp_path, "criticality: error\n"))


def test_load_checks_reports_a_missing_file(tmp_path):
    with pytest.raises(ChecksValidationError, match="could not be read"):
        load_checks(str(tmp_path / "absent.yml"))


def test_empty_ruleset_is_allowed(tmp_path):
    assert load_checks(_ruleset(tmp_path, "")) == []


# ── the gate ─────────────────────────────────────────────────────────────────


def test_no_configured_checks_is_a_no_op(spark, ctx, runner):
    runner.run(_batch(spark), ctx)

    ctx.logger.info.assert_not_called()
    ctx.logger.warning.assert_not_called()
    ctx.logger.error.assert_not_called()


def test_passing_checks_log_once_and_do_not_raise(spark, ctx, runner, tmp_path):
    ctx.checks = load_checks(_ruleset(tmp_path, ID_NOT_NULL))
    clean = spark.createDataFrame([(1, "a"), (2, "b")], "ID int, NAME string")

    runner.run(clean, ctx)

    ctx.logger.error.assert_not_called()
    assert ctx.logger.info.call_args.kwargs["name"] == "policies_passed"


def test_warn_criticality_logs_a_warning_and_lets_the_batch_through(spark, ctx, runner, tmp_path):
    ctx.checks = load_checks(_ruleset(tmp_path, NAME_NOT_NULL_WARN))

    runner.run(_batch(spark), ctx)

    ctx.logger.error.assert_not_called()
    logged = ctx.logger.warning.call_args.kwargs
    assert logged["source"] == "Policies"
    assert logged["total"] == 1


def test_error_criticality_raises_after_logging(spark, ctx, runner, tmp_path):
    ctx.checks = load_checks(_ruleset(tmp_path, ID_NOT_NULL))

    with pytest.raises(PolicyViolation):
        runner.run(_batch(spark), ctx)

    logged = ctx.logger.error.call_args.kwargs
    assert logged["source"] == "Policies"
    assert logged["total"] == 1


def test_every_check_is_evaluated_before_the_gate_refuses(spark, ctx, runner, tmp_path):
    """A failing error check must not stop the warn check from being reported."""
    ctx.checks = load_checks(_ruleset(tmp_path, ID_NOT_NULL + NAME_NOT_NULL_WARN))

    with pytest.raises(PolicyViolation) as excinfo:
        runner.run(_batch(spark), ctx)

    ctx.logger.warning.assert_called_once()
    ctx.logger.error.assert_called_once()

    # Only the error-criticality check is a violation; the warning is not.
    assert len(excinfo.value.results) == 1
    assert excinfo.value.results[0].severity is Severity.FAIL


def test_a_named_part_of_a_batch_is_named_in_every_audit_row(spark, ctx, runner, tmp_path):
    """A verb gating a backlog per snapshot logs the same names once per snapshot."""
    ctx.checks = load_checks(_ruleset(tmp_path, ID_NOT_NULL + NAME_NOT_NULL_WARN))

    with pytest.raises(PolicyViolation):
        runner.run(_batch(spark), ctx, batch="snapshot 2026-01-01 12:00:00")

    assert ctx.logger.error.call_args.kwargs["description"].endswith(
        "(snapshot 2026-01-01 12:00:00)"
    )
    assert ctx.logger.warning.call_args.kwargs["description"].endswith(
        "(snapshot 2026-01-01 12:00:00)"
    )


def test_the_gate_does_not_alter_the_batch(spark, ctx, runner, tmp_path):
    """DQX's result columns are private to the runner and never reach the writer."""
    ctx.checks = load_checks(_ruleset(tmp_path, NAME_NOT_NULL_WARN))
    batch = _batch(spark)

    runner.run(batch, ctx)

    assert batch.columns == ["ID", "NAME"]


# ── Start-layer wiring ───────────────────────────────────────────────────────


def _config(**overrides) -> TaskConfig:
    defaults: dict[str, Any] = dict(
        catalog="cro",
        env="dev_01",
        metadata_path="/Volumes/meta/",
        source=SourceConfig(origin=Origin.CSV, path="/Volumes/in/", directory="agents"),
        typing=TypingConfig(),
        policies=PoliciesConfig(),
        output=OutputConfig(verb=Verb.APPEND, schema_name="bronze_cro", table="AGENTS"),
    )
    defaults.update(overrides)
    return TaskConfig(**defaults)


@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.catalog.tableExists.return_value = False
    return spark


def test_context_carries_no_checks_when_none_are_configured(mock_spark):
    assert build_context(_config(), mock_spark, RUN).checks == []


def test_context_carries_the_loaded_ruleset(mock_spark, tmp_path):
    config = _config(policies=PoliciesConfig(checks_file=_ruleset(tmp_path, ID_NOT_NULL)))

    ctx = build_context(config, mock_spark, RUN)

    assert len(ctx.checks) == 1
    assert ctx.checks[0]["check"]["function"] == "is_not_null"


def test_a_bad_ruleset_fails_the_task_at_start(mock_spark, tmp_path):
    config = _config(policies=PoliciesConfig(checks_file=str(tmp_path / "absent.yml")))

    with pytest.raises(ConfigValidationError, match="could not be read"):
        build_context(config, mock_spark, RUN)
