"""Tests for context.table_config — the table_config file, read and validated at Start."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dbx_flame.context.config import Origin, OutputConfig, SourceConfig, TaskConfig, Verb
from dbx_flame.context.context import RunIdentity
from dbx_flame.context.loader import ConfigValidationError, build_context
from dbx_flame.context.table_config import TableConfigError, load_table_config

RUN = RunIdentity(
    workflow_id="wf-1", workflow_run_id="wfrun-1", task_key="silver", task_run_id="taskrun-1"
)

FULL = """\
comment: "Study milestones per site."
tags:
  domain: clinical
columns:
  - name: MILESTONESEQ
    cast:
      target_type: decimal(10,2)
    comment: "Expected milestone completion order."
    tags:
      PII: "no"
      category: study_status
  - name: SITE
    comment: "The site's code."
  - name: STARTED
    cast:
      target_type: date
      format: dd/MM/yyyy
"""


def _file(tmp_path, body: str) -> str:
    path = tmp_path / "table.yml"
    path.write_text(body, encoding="utf-8")
    return str(path)


def _errors(tmp_path, body: str) -> str:
    with pytest.raises(TableConfigError) as failure:
        load_table_config(_file(tmp_path, body))
    return "; ".join(failure.value.errors)


def test_a_full_file_loads_every_setting(tmp_path):
    config = load_table_config(_file(tmp_path, FULL))

    assert config.comment == "Study milestones per site."
    assert config.tags == {"domain": "clinical"}
    milestone = config.columns[0]
    assert milestone.comment == "Expected milestone completion order."
    assert milestone.tags == {"PII": "no", "category": "study_status"}


def test_casts_are_the_columns_that_declare_one_in_file_order(tmp_path):
    config = load_table_config(_file(tmp_path, FULL))

    assert list(config.casts) == ["MILESTONESEQ", "STARTED"]
    assert config.casts["STARTED"].format == "dd/MM/yyyy"


@pytest.mark.parametrize(
    "body",
    [
        'comment: "Only a table comment."\n',
        "tags:\n  domain: clinical\n",
        "columns:\n  - name: ID\n    cast:\n      target_type: int\n",
        'columns:\n  - name: ID\n    comment: "Only a column comment."\n',
        'columns:\n  - name: ID\n    tags:\n      PII: "no"\n',
    ],
)
def test_each_setting_is_optional_on_its_own(tmp_path, body):
    load_table_config(_file(tmp_path, body))


@pytest.mark.parametrize("body", ["", "comment: null\n", "tags: {}\ncolumns: []\n"])
def test_a_file_that_sets_nothing_is_rejected(tmp_path, body):
    """It would do nothing, which is easy to mistake for a file that works."""
    assert _errors(tmp_path, body)


def test_a_column_with_only_a_name_is_rejected(tmp_path):
    errors = _errors(tmp_path, "columns:\n  - name: ID\n")

    assert "column 'ID' sets none of cast, comment or tags" in errors


def test_a_column_needs_a_name(tmp_path):
    errors = _errors(tmp_path, "columns:\n  - comment: x\n")

    assert "columns.0.name" in errors


@pytest.mark.parametrize(
    "tags",
    ["PII: no", "PII: yes", "PII: off", "RANK: 1", "yes: PII"],
)
def test_a_tag_yaml_reads_as_something_other_than_a_string_is_rejected(tmp_path, tags):
    """YAML reads an unquoted no as false; turning it back into text would guess."""
    errors = _errors(tmp_path, f"columns:\n  - name: ID\n    tags:\n      {tags}\n")

    assert "must be quoted strings" in errors


def test_an_unquoted_table_tag_is_rejected_too(tmp_path):
    assert "must be quoted strings" in _errors(tmp_path, "tags:\n  PII: no\n")


def test_a_framework_column_is_rejected(tmp_path):
    """The framework owns its columns' types and the comments it keeps on them."""
    errors = _errors(tmp_path, 'columns:\n  - name: __EXPORT_DATE\n    comment: "x"\n')

    assert "'__EXPORT_DATE' is a framework column" in errors


def test_a_column_listed_twice_is_rejected(tmp_path):
    """Which entry would win is a guess either way."""
    body = 'columns:\n  - name: ID\n    comment: "a"\n  - name: ID\n    comment: "b"\n'

    assert "columns are listed more than once: ID" in _errors(tmp_path, body)


@pytest.mark.parametrize("body", ['comment: "  "\n', 'columns:\n  - name: ID\n    comment: ""\n'])
def test_a_blank_comment_is_rejected(tmp_path, body):
    assert "comment must not be empty" in _errors(tmp_path, body)


@pytest.mark.parametrize(
    "body",
    [
        'comments: "typo"\n',
        'columns:\n  - name: ID\n    coment: "typo"\n',
        "columns:\n  - name: ID\n    cast:\n      target: int\n",
    ],
)
def test_an_unknown_key_is_rejected(tmp_path, body):
    """A typo that silently does nothing is worse than a failed run."""
    assert "Extra inputs are not permitted" in _errors(tmp_path, body)


def test_every_problem_is_reported_with_the_file_and_its_location(tmp_path):
    body = "tags:\n  PII: no\ncolumns:\n  - name: ID\n  - name: __SOURCE\n    comment: x\n"
    path = _file(tmp_path, body)

    with pytest.raises(TableConfigError) as failure:
        load_table_config(path)

    assert len(failure.value.errors) == 3
    assert all(error.startswith(f"table_config '{path}': ") for error in failure.value.errors)


def test_an_unreadable_file_is_rejected(tmp_path):
    with pytest.raises(TableConfigError, match="could not be read"):
        load_table_config(str(tmp_path / "absent.yml"))


def test_invalid_yaml_is_rejected(tmp_path):
    assert "could not be read" in _errors(tmp_path, "columns: [\n")


def test_a_file_that_is_not_a_mapping_is_rejected(tmp_path):
    assert "must hold a mapping, got list" in _errors(tmp_path, "- name: ID\n")


def test_an_empty_file_says_so(tmp_path):
    assert "is empty" in _errors(tmp_path, "")


def _task(table_config: str | None) -> TaskConfig:
    return TaskConfig(
        catalog="cro",
        env="dev_01",
        metadata_path="/Volumes/cro_dev_01/meta/",
        table_config=table_config,
        source=SourceConfig(origin=Origin.CSV, path="/Volumes/in/", directory="agents"),
        output=OutputConfig(verb=Verb.APPEND, schema_name="silver_cro", table="AGENTS"),
    )


def test_the_context_carries_the_file_read_at_start(tmp_path):
    ctx = build_context(_task(_file(tmp_path, FULL)), MagicMock(), RUN)

    assert ctx.table_config is not None
    assert ctx.table_config.comment == "Study milestones per site."


def test_a_task_without_a_file_carries_none(tmp_path):
    ctx = build_context(_task(None), MagicMock(), RUN)

    assert ctx.table_config is None


def test_a_bad_file_fails_the_task_at_start(tmp_path):
    with pytest.raises(ConfigValidationError, match="sets none of cast, comment or tags"):
        build_context(_task(_file(tmp_path, "columns:\n  - name: ID\n")), MagicMock(), RUN)
