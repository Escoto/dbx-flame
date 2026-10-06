"""Tests for the pre-processor registry — resolution, ordering, unknown names."""

from __future__ import annotations

from typing import ClassVar
from unittest.mock import MagicMock

import pytest

from dbx_flame.context.config import (
    Origin,
    OutputConfig,
    SourceConfig,
    TaskConfig,
    Verb,
)
from dbx_flame.context.loader import ConfigValidationError, validate_requirements
from dbx_flame.pipelines import preprocessors
from dbx_flame.pipelines.preprocessors import (
    PREPROCESSORS,
    RecordEnvelope,
    apply_preprocessors,
    resolve,
)


def _config(names: list[str], fields: list[str] | None = None) -> TaskConfig:
    if fields is None:
        fields = ["uri"] if RecordEnvelope.name in names else []
    return TaskConfig(
        catalog="cro",
        env="dev_01",
        metadata_path="/Volumes/cro_dev_01/meta/",
        source=SourceConfig(
            origin=Origin.JSON,
            path="/Volumes/in/",
            directory="agents",
            preprocessors=names,
            envelope_fields=fields,
        ),
        output=OutputConfig(verb=Verb.APPEND, schema_name="bronze_cro", table="AGENTS"),
    )


def test_only_the_envelope_is_registered():
    assert PREPROCESSORS == {"record_envelope": RecordEnvelope}


def test_resolve_instantiates_the_named_preprocessors():
    resolved = resolve(["record_envelope"])

    assert [type(item) for item in resolved] == [RecordEnvelope]


def test_resolve_with_no_names_is_empty():
    assert resolve([]) == []


def test_flatten_nested_is_rejected_at_start():
    """It is deliberately not provided, so asking for it must fail rather than do nothing."""
    errors = validate_requirements(_config(["flatten_nested"]))

    assert any("flatten_nested" in error for error in errors)


def test_an_unknown_name_is_rejected_at_start():
    errors = validate_requirements(_config(["record_envelope", "nonsense"]))

    assert len(errors) == 1
    assert "nonsense" in errors[0]
    assert "record_envelope" in errors[0]  # the registered names are listed


def test_registered_names_pass_validation():
    assert validate_requirements(_config(["record_envelope"])) == []


def test_build_context_refuses_an_unknown_preprocessor():
    from dbx_flame.context.context import RunIdentity
    from dbx_flame.context.loader import build_context

    run = RunIdentity("wf", "wfrun", "task", "taskrun")

    with pytest.raises(ConfigValidationError, match="nonsense"):
        build_context(_config(["nonsense"]), MagicMock(), run)


def test_preprocessors_are_applied_in_order(monkeypatch):
    calls: list[str] = []

    class First:
        name: ClassVar[str] = "first"

        def apply(self, df, ctx):
            calls.append("first")
            return df

    class Second:
        name: ClassVar[str] = "second"

        def apply(self, df, ctx):
            calls.append("second")
            return df

    monkeypatch.setitem(preprocessors.PREPROCESSORS, "first", First)
    monkeypatch.setitem(preprocessors.PREPROCESSORS, "second", Second)
    ctx = MagicMock()
    ctx.config.source.preprocessors = ["second", "first"]

    apply_preprocessors(MagicMock(), ctx)

    assert calls == ["second", "first"]


def test_no_preprocessors_returns_the_input_untouched():
    ctx = MagicMock()
    ctx.config.source.preprocessors = []
    df = MagicMock()

    assert apply_preprocessors(df, ctx) is df
