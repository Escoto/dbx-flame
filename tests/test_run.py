"""Tests for the wheel entry point — main()."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from dbx_flame.context.loader import ConfigValidationError
from dbx_flame.entrypoints.run import _log_promotion_start, _parse_argv, main


@pytest.fixture(autouse=True)
def stub_pipeline():
    """These tests cover the entrypoint's contract, not the pipeline it drives."""
    with patch("dbx_flame.entrypoints.run.run_pipeline") as stub:
        yield stub


@pytest.fixture(autouse=True)
def stub_version():
    with patch("dbx_flame.entrypoints.run.table_version", return_value=12) as stub:
        yield stub


def test_parse_argv_basic():
    argv = ["--catalog=cro", "--env=dev_01", "--source.origin=csv"]
    result = _parse_argv(argv)
    assert result == {"catalog": "cro", "env": "dev_01", "source.origin": "csv"}


def test_parse_argv_ignores_non_flag_args():
    argv = ["positional", "--key=value", "another"]
    result = _parse_argv(argv)
    assert result == {"key": "value"}


def test_parse_argv_value_with_equals():
    argv = ["--pattern=foo=bar"]
    result = _parse_argv(argv)
    assert result == {"pattern": "foo=bar"}


@patch("dbx_flame.entrypoints.run.SparkSession")
@patch("dbx_flame.entrypoints.run.build_context")
@patch("dbx_flame.entrypoints.run.load_config")
def test_end_to_end_valid_params(mock_load, mock_build, mock_spark_cls):
    mock_config = MagicMock()
    mock_load.return_value = mock_config

    mock_spark = MagicMock()
    mock_spark_cls.getActiveSession.return_value = mock_spark

    mock_ctx = MagicMock()
    mock_build.return_value = mock_ctx

    with patch(
        "sys.argv",
        [
            "run",
            "--catalog=cro",
            "--env=dev_01",
            "--metadata_path=/v/",
            "--source.origin=csv",
            "--source.path=/v/in/",
            "--source.directory=agents",
            "--output.verb=append",
            "--output.schema_name=bronze",
            "--output.table=AGENTS",
        ],
    ):
        main()

    mock_load.assert_called_once()
    mock_build.assert_called_once()
    assert mock_ctx.logger.flush.called


@patch("dbx_flame.entrypoints.run.SparkSession")
@patch("dbx_flame.entrypoints.run.build_context")
@patch("dbx_flame.entrypoints.run.load_config")
def test_logger_flushed_on_success(mock_load, mock_build, mock_spark_cls):
    mock_load.return_value = MagicMock()
    mock_spark_cls.getActiveSession.return_value = MagicMock()
    mock_ctx = MagicMock()
    mock_build.return_value = mock_ctx

    with patch(
        "sys.argv",
        [
            "run",
            "--catalog=cro",
            "--env=dev_01",
            "--metadata_path=/v/",
            "--source.origin=csv",
            "--source.path=/v/in/",
            "--source.directory=agents",
            "--output.verb=append",
            "--output.schema_name=bronze",
            "--output.table=AGENTS",
        ],
    ):
        main()

    # one flush for the eager pipeline_start write, one in the finally
    assert mock_ctx.logger.flush.call_count == 2


@patch("dbx_flame.entrypoints.run.load_config")
def test_config_error_propagates(mock_load):
    mock_load.side_effect = ConfigValidationError(["bad param"])

    with patch("sys.argv", ["run", "--bad=param"]):
        with pytest.raises(ConfigValidationError, match="bad param"):
            main()


@patch("dbx_flame.entrypoints.run.SparkSession")
@patch("dbx_flame.entrypoints.run.build_context")
@patch("dbx_flame.entrypoints.run.load_config")
def test_logger_flushed_on_failure(mock_load, mock_build, mock_spark_cls):
    mock_load.return_value = MagicMock()
    mock_spark_cls.getActiveSession.return_value = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.logger.info.side_effect = [None, None, RuntimeError("boom")]
    mock_build.return_value = mock_ctx

    with patch(
        "sys.argv",
        [
            "run",
            "--catalog=cro",
            "--env=dev_01",
            "--metadata_path=/v/",
            "--source.origin=csv",
            "--source.path=/v/in/",
            "--source.directory=agents",
            "--output.verb=append",
            "--output.schema_name=bronze",
            "--output.table=AGENTS",
        ],
    ):
        with pytest.raises(RuntimeError, match="boom"):
            main()

    mock_ctx.logger.error.assert_called_once()
    assert mock_ctx.logger.flush.called


@patch("dbx_flame.entrypoints.run.SparkSession")
@patch("dbx_flame.entrypoints.run.build_context")
@patch("dbx_flame.entrypoints.run.load_config")
def test_logger_flushed_on_cancellation(mock_load, mock_build, mock_spark_cls):
    """A cancelled run raises BaseException, which never reaches `except Exception`."""
    mock_load.return_value = MagicMock()
    mock_spark_cls.getActiveSession.return_value = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.logger.info.side_effect = [None, None, KeyboardInterrupt()]
    mock_build.return_value = mock_ctx

    with patch("sys.argv", ["run", "--catalog=cro"]):
        with pytest.raises(KeyboardInterrupt):
            main()

    # the second flush is the one in the finally — the rows survived the interrupt
    assert mock_ctx.logger.flush.call_count == 2
    mock_ctx.logger.error.assert_not_called()


@patch("dbx_flame.entrypoints.run.SparkSession")
@patch("dbx_flame.entrypoints.run.build_context")
@patch("dbx_flame.entrypoints.run.load_config")
def test_failure_path_flushes_once(mock_load, mock_build, mock_spark_cls):
    """One flush point, so a failing flush cannot retry and mask the real error."""
    mock_load.return_value = MagicMock()
    mock_spark_cls.getActiveSession.return_value = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.logger.info.side_effect = [None, None, RuntimeError("boom")]
    mock_build.return_value = mock_ctx

    with patch("sys.argv", ["run", "--catalog=cro"]):
        with pytest.raises(RuntimeError, match="boom"):
            main()

    # eager start flush + one in the finally; the old code flushed twice in the
    # handler and masked the real error with the retry
    assert mock_ctx.logger.flush.call_count == 2


@patch("dbx_flame.entrypoints.run.SparkSession")
@patch("dbx_flame.entrypoints.run.build_context")
@patch("dbx_flame.entrypoints.run.load_config")
def test_params_logged_and_flushed_before_any_work(mock_load, mock_build, mock_spark_cls):
    """The params write lands before the pipeline runs, so it can gate the run."""
    mock_load.return_value = MagicMock()
    mock_spark_cls.getActiveSession.return_value = MagicMock()
    mock_ctx = MagicMock()
    mock_build.return_value = mock_ctx

    argv = [
        "run",
        "--catalog=cro",
        "--output.table=AGENTS",
        "--__workflow_id=42",
        "--__workflow_run_id=777",
        "--__task_key=ingest",
        "--__task_run_id=888",
    ]
    with patch("sys.argv", argv):
        main()

    first_info = mock_ctx.logger.info.call_args_list[0]
    assert first_info.kwargs["name"] == "pipeline_start"
    logged = json.loads(first_info.kwargs["metadata"])
    assert logged == {"catalog": "cro", "output.table": "AGENTS"}

    # identity params feed the audit columns; they are not echoed into the blob
    assert not any(key.startswith("__") for key in logged)

    # identity was supplied in full, so nothing is flagged as defaulted
    mock_ctx.logger.warning.assert_not_called()

    # ...and the flush follows it and promotion_start, before anything else
    ordered = [name for name, _, _ in mock_ctx.logger.mock_calls]
    assert ordered[:3] == ["info", "info", "flush"]


@patch("dbx_flame.entrypoints.run.SparkSession")
@patch("dbx_flame.entrypoints.run.build_context")
@patch("dbx_flame.entrypoints.run.load_config")
def test_unwritable_audit_stops_before_work(mock_load, mock_build, mock_spark_cls):
    """If the audit path is broken the task dies up front, having done nothing."""
    mock_load.return_value = MagicMock()
    mock_spark_cls.getActiveSession.return_value = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.logger.flush.side_effect = RuntimeError("audit table unavailable")
    mock_build.return_value = mock_ctx

    with patch("sys.argv", ["run", "--catalog=cro"]):
        with pytest.raises(RuntimeError, match="audit table unavailable"):
            main()

    # only pipeline_start and promotion_start were logged — the pipeline body never ran
    assert mock_ctx.logger.info.call_count == 2
    assert mock_ctx.logger.flush.call_count == 1


@patch("dbx_flame.entrypoints.run.SparkSession")
@patch("dbx_flame.entrypoints.run.build_context")
@patch("dbx_flame.entrypoints.run.load_config")
def test_missing_identity_defaults_but_leaves_a_trace(mock_load, mock_build, mock_spark_cls):
    """Defaulting keeps ad-hoc runs easy; the warning stops it being silent."""
    mock_load.return_value = MagicMock()
    mock_spark_cls.getActiveSession.return_value = MagicMock()
    mock_ctx = MagicMock()
    mock_build.return_value = mock_ctx

    with patch("sys.argv", ["run", "--catalog=cro"]):
        main()

    run = mock_build.call_args.args[2]
    assert run.workflow_id == "local"
    assert run.workflow_run_id == "local"
    assert run.task_key == "local"
    assert run.task_run_id == "local"

    warning = mock_ctx.logger.warning.call_args
    assert warning.kwargs["name"] == "run_identity_missing"
    for name in ("__workflow_id", "__workflow_run_id", "__task_key", "__task_run_id"):
        assert name in warning.kwargs["description"]

    # the warning rides along in the eager start flush, not a later one
    ordered = [name for name, _, _ in mock_ctx.logger.mock_calls]
    assert ordered[:4] == ["warning", "info", "info", "flush"]


@patch("dbx_flame.entrypoints.run.SparkSession")
@patch("dbx_flame.entrypoints.run.build_context")
@patch("dbx_flame.entrypoints.run.load_config")
def test_partial_identity_flags_only_the_missing(mock_load, mock_build, mock_spark_cls):
    mock_load.return_value = MagicMock()
    mock_spark_cls.getActiveSession.return_value = MagicMock()
    mock_ctx = MagicMock()
    mock_build.return_value = mock_ctx

    with patch("sys.argv", ["run", "--catalog=cro", "--__workflow_id=42", "--__task_key=ingest"]):
        main()

    run = mock_build.call_args.args[2]
    assert run.workflow_id == "42"
    assert run.task_key == "ingest"
    assert run.workflow_run_id == "local"

    description = mock_ctx.logger.warning.call_args.kwargs["description"]
    assert "__workflow_run_id" in description
    assert "__task_run_id" in description
    assert "__workflow_id" not in description


@patch("dbx_flame.entrypoints.run.SparkSession")
@patch("dbx_flame.entrypoints.run.build_context")
@patch("dbx_flame.entrypoints.run.load_config")
def test_the_pipeline_runs_between_start_and_complete(
    mock_load, mock_build, mock_spark_cls, stub_pipeline
):
    mock_load.return_value = MagicMock()
    mock_spark_cls.getActiveSession.return_value = MagicMock()
    mock_ctx = MagicMock()
    mock_build.return_value = mock_ctx

    with patch("sys.argv", ["run", "--catalog=cro"]):
        main()

    stub_pipeline.assert_called_once_with(mock_ctx)
    events = [call.kwargs["name"] for call in mock_ctx.logger.info.call_args_list]
    assert events == ["pipeline_start", "promotion_start", "pipeline_complete"]


@patch("dbx_flame.entrypoints.run.SparkSession")
@patch("dbx_flame.entrypoints.run.build_context")
@patch("dbx_flame.entrypoints.run.load_config")
def test_the_target_version_is_recorded_before_any_work(mock_load, mock_build, mock_spark_cls):
    """It is the version to restore when a failed run is beyond what a retry repairs."""
    mock_load.return_value = MagicMock()
    mock_spark_cls.getActiveSession.return_value = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.source_table = "`cro_dev_01`.`bronze`.`SUBJECTS`"
    mock_ctx.target_table = "`cro_dev_01`.`silver`.`SUBJECTS`"
    mock_build.return_value = mock_ctx

    with patch("sys.argv", ["run", "--catalog=cro"]):
        main()

    promotion = mock_ctx.logger.info.call_args_list[1].kwargs
    assert promotion["name"] == "promotion_start"
    assert promotion["description"] == (
        "Starting data promotion from `cro_dev_01`.`bronze`.`SUBJECTS` to "
        "`cro_dev_01`.`silver`.`SUBJECTS`.\nCurrent `cro_dev_01`.`silver`.`SUBJECTS` version #12."
    )
    assert promotion["total"] == 12


def test_a_new_target_is_recorded_as_having_no_version(stub_version):
    stub_version.return_value = None
    ctx = MagicMock(source_table=None, inbound_glob="/v/in/agents/*.csv", target_table="`T`")

    _log_promotion_start(ctx)

    promotion = ctx.logger.info.call_args.kwargs
    assert promotion["description"] == (
        "Starting data promotion from /v/in/agents/*.csv to `T`.\n"
        "Current `T` version: none (table does not exist yet)."
    )
    assert promotion["total"] is None
