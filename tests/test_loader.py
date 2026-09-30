"""Tests for load_config — flat params to validated TaskConfig."""

from __future__ import annotations

import pytest

from dbx_flame.context.config import IncrementStrategy, Origin, TaskConfig, Verb
from dbx_flame.context.loader import (
    ConfigValidationError,
    load_config,
    validate_requirements,
)

MINIMAL_PARAMS = {
    "catalog": "cro",
    "env": "dev_01",
    "metadata_path": "/Volumes/meta/",
    "source.origin": "csv",
    "source.path": "/Volumes/inbound/",
    "source.directory": "agents",
    "output.verb": "append",
    "output.schema_name": "bronze_cro",
    "output.table": "AGENTS",
}


def test_happy_path_minimal():
    config = load_config(MINIMAL_PARAMS)
    assert isinstance(config, TaskConfig)
    assert config.catalog == "cro"
    assert config.env == "dev_01"
    assert config.metadata_path == "/Volumes/meta/"
    assert config.source.origin == Origin.CSV
    assert config.source.path == "/Volumes/inbound/"
    assert config.source.directory == "agents"
    assert config.output.verb == Verb.APPEND
    assert config.output.schema_name == "bronze_cro"
    assert config.output.table == "AGENTS"


def test_happy_path_full():
    params = {
        **MINIMAL_PARAMS,
        "source.options.header": "false",
        "source.options.delimiter": "|",
        "source.options.quote": "'",
        "source.options.escape": "\\",
        "source.options.multiline": "false",
        "source.file_extension": ".csv",
        "schema_evolution": "add_new_columns",
        "source.snapshot_time_pattern": "iso",
        "source.preprocessors": "trim, upper",
        "source.rename_patterns": "_OLD$=_NEW, ^FOO_=",
        "source.schema_name": "bronze_cro",
        "source.table": "AGENTS",
        "output.keys": "ID, NAME",
        "output.event_time.column": "UPDATED_AT",
        "output.event_time.format": "yyyy-MM-dd",
        "output.snapshot_scope": "full",
        "output.dedup.enabled": "true",
        "output.dedup.columns": "ID",
        "output.dedup.order_by": "UPDATED_AT",
        "output.dedup.order_by_format": "yyyy-MM-dd",
        "output.deletes.keys": "ID, NAME",
        "output.deletes.event_time.column": "DELETED_AT",
        "output.deletes.event_time.format": "yyyy-MM-dd HH:mm:ss",
        "typing.cast_config": "/Volumes/casts/agents.yml",
        "typing.validate_casts": "false",
        "policies.checks_file": "/Volumes/meta/checks/AGENTS.yml",
    }
    config = load_config(params)
    assert config.source.options.header is False
    assert config.source.options.delimiter == "|"
    assert config.source.options.multiline is False
    assert config.source.file_extension == ".csv"
    assert config.schema_evolution.value == "add_new_columns"
    assert config.source.snapshot_time_pattern.value == "iso"
    assert config.source.preprocessors == ["trim", "upper"]
    assert config.source.rename_patterns == ["_OLD$=_NEW", "^FOO_="]
    assert config.output.keys == ["ID", "NAME"]
    assert config.output.event_time.column == "UPDATED_AT"
    assert config.output.event_time.format == "yyyy-MM-dd"
    assert config.output.snapshot_scope.value == "full"
    assert config.output.dedup.enabled is True
    assert config.output.dedup.columns == ["ID"]
    assert config.output.dedup.order_by == "UPDATED_AT"
    assert config.output.dedup.order_by_format == "yyyy-MM-dd"
    assert config.output.deletes.keys == ["ID", "NAME"]
    assert config.output.deletes.event_time.column == "DELETED_AT"
    assert config.output.deletes.event_time.format == "yyyy-MM-dd HH:mm:ss"
    assert config.typing.cast_config == "/Volumes/casts/agents.yml"
    assert config.typing.validate_casts is False
    assert config.policies.checks_file == "/Volumes/meta/checks/AGENTS.yml"


def test_dotted_key_splitting():
    config = load_config(MINIMAL_PARAMS)
    assert config.source.origin == Origin.CSV
    assert config.output.verb == Verb.APPEND
    assert config.output.schema_name == "bronze_cro"


@pytest.mark.parametrize("value", ["true", "True", "TRUE"])
def test_boolean_coercion_true_variants(value):
    params = {**MINIMAL_PARAMS, "output.dedup.enabled": value, "output.dedup.order_by": "DT"}
    config = load_config(params)
    assert config.output.dedup.enabled is True


@pytest.mark.parametrize("value", ["false", "False", "FALSE"])
def test_boolean_coercion_false_variants(value):
    params = {**MINIMAL_PARAMS, "source.options.header": value}
    config = load_config(params)
    assert config.source.options.header is False


def test_list_coercion_comma_separated():
    params = {**MINIMAL_PARAMS, "output.keys": "ID, NAME"}
    config = load_config(params)
    assert config.output.keys == ["ID", "NAME"]


def test_list_coercion_empty_string():
    params = {**MINIMAL_PARAMS, "output.keys": ""}
    config = load_config(params)
    assert config.output.keys == []


def test_enum_coercion_case_insensitive():
    for origin_str in ["CSV", "Csv", "csv"]:
        params = {**MINIMAL_PARAMS, "source.origin": origin_str}
        config = load_config(params)
        assert config.source.origin == Origin.CSV


def test_unknown_key_rejected():
    params = {**MINIMAL_PARAMS, "unknown_key": "value"}
    with pytest.raises(ConfigValidationError, match="unknown_key"):
        load_config(params)


def test_retired_param_source_type():
    params = {**MINIMAL_PARAMS, "source_type": "csv"}
    with pytest.raises(ConfigValidationError, match="source_type") as exc_info:
        load_config(params)
    assert "Retired parameter" in str(exc_info.value)
    assert "no longer supported" in str(exc_info.value)


def test_retired_param_update_columns():
    params = {**MINIMAL_PARAMS, "update_columns": "col1,col2"}
    with pytest.raises(ConfigValidationError, match="update_columns") as exc_info:
        load_config(params)
    assert "Retired parameter" in str(exc_info.value)


def test_missing_required_field():
    params = {k: v for k, v in MINIMAL_PARAMS.items() if k != "catalog"}
    with pytest.raises(ConfigValidationError, match="catalog"):
        load_config(params)


def test_aggregated_errors():
    params = {
        "source_type": "csv",
        "unknown_key": "value",
        "env": "dev_01",
    }
    with pytest.raises(ConfigValidationError) as exc_info:
        load_config(params)
    assert len(exc_info.value.errors) >= 2
    error_text = str(exc_info.value)
    assert "source_type" in error_text
    assert "unknown_key" in error_text


def test_nested_model_defaults():
    config = load_config(MINIMAL_PARAMS)
    assert config.source.options.header is True
    assert config.source.options.delimiter == ","
    assert config.source.options.multiline is True
    assert config.source.preprocessors == []
    assert config.output.dedup.enabled is True
    assert config.output.dedup.columns == []
    assert config.typing.validate_casts is True
    assert config.policies.checks_file is None
    assert config.schema_evolution.value == "fail_on_new_columns"


def test_dedup_config():
    params = {
        **MINIMAL_PARAMS,
        "output.dedup.enabled": "true",
        "output.dedup.columns": "ID, NAME",
        "output.dedup.order_by": "UPDATED_AT",
        "output.dedup.order_by_format": "yyyy-MM-dd",
    }
    config = load_config(params)
    assert config.output.dedup.enabled is True
    assert config.output.dedup.columns == ["ID", "NAME"]
    assert config.output.dedup.order_by == "UPDATED_AT"
    assert config.output.dedup.order_by_format == "yyyy-MM-dd"


def test_event_time_config():
    params = {
        **MINIMAL_PARAMS,
        "output.event_time.column": "UPDATED_AT",
        "output.event_time.format": "yyyy-MM-dd",
    }
    config = load_config(params)
    assert config.output.event_time is not None
    assert config.output.event_time.column == "UPDATED_AT"
    assert config.output.event_time.format == "yyyy-MM-dd"


def test_policies_config():
    """The task config names a ruleset; the rules themselves live in it."""
    params = {
        **MINIMAL_PARAMS,
        "policies.checks_file": "/Volumes/meta/checks/AGENTS.yml",
    }
    config = load_config(params)
    assert config.policies.checks_file == "/Volumes/meta/checks/AGENTS.yml"


def test_policies_default_to_no_ruleset():
    config = load_config(MINIMAL_PARAMS)
    assert config.policies.checks_file is None


@pytest.mark.parametrize("value", ["yes", "1", "no", "tru", ""])
def test_invalid_boolean_rejected(value):
    """Anything that is not true/false is an error, never a silent False."""
    params = {**MINIMAL_PARAMS, "output.dedup.enabled": value}
    with pytest.raises(ConfigValidationError) as exc_info:
        load_config(params)
    assert "output.dedup.enabled: expected true or false" in str(exc_info.value)


def test_boolean_tolerates_surrounding_whitespace():
    params = {**MINIMAL_PARAMS, "output.dedup.enabled": " True ", "output.dedup.order_by": "DT"}
    config = load_config(params)
    assert config.output.dedup.enabled is True


def test_increment_strategy_is_parsed_as_an_enum():
    """It became a parameter once SCD2 needed to opt out of streaming checkpoints.

    Which verbs accept which strategy is a separate question, answered by
    validate_requirements against what the writer declares.
    """
    config = load_config({**MINIMAL_PARAMS, "source.increment_strategy": "watermark"})

    assert config.source.increment_strategy == IncrementStrategy.WATERMARK


def test_no_increment_strategy_is_chosen_by_default():
    """Absent means "use the verb's default", not a strategy in its own right."""
    assert load_config(MINIMAL_PARAMS).source.increment_strategy is None


def test_anchor_dt_is_parsed_with_its_format():
    params = {
        **MINIMAL_PARAMS,
        "source.anchor_dt.column": "UPDATED_DATE",
        "source.anchor_dt.format": "yyyy-MM-dd HH:mm:ss",
    }

    anchor = load_config(params).source.anchor_dt

    assert anchor.column == "UPDATED_DATE"
    assert anchor.format == "yyyy-MM-dd HH:mm:ss"


def test_increment_anchor_is_a_switch():
    """The column and its format belong to Bronze; Silver only reads __ANCHOR_DT."""
    assert load_config({**DELTA_PARAMS, "source.increment_anchor": "true"}).source.increment_anchor
    assert not load_config(DELTA_PARAMS).source.increment_anchor


DELTA_PARAMS = {
    "catalog": "cro",
    "env": "dev_01",
    "metadata_path": "/Volumes/meta/",
    "source.origin": "delta",
    "source.schema_name": "bronze_cro",
    "source.table": "AGENTS_UPDATES",
    "output.verb": "scd2",
    "output.schema_name": "silver_cro",
    "output.table": "AGENTS",
    "output.keys": "ID",
    "output.event_time.column": "MODIFIED",
}


def test_deletes_table_parsed_on_delta_origin():
    config = load_config({**DELTA_PARAMS, "source.deletes_table": "AGENTS_DELETES"})
    assert config.source.deletes_table == "AGENTS_DELETES"


# --- field-level validation ---------------------------------------------------


@pytest.mark.parametrize("missing", ["source.path", "source.directory"])
def test_file_origin_requires_path_and_directory(missing):
    params = {k: v for k, v in MINIMAL_PARAMS.items() if k != missing}
    with pytest.raises(ConfigValidationError, match=missing):
        load_config(params)


@pytest.mark.parametrize("missing", ["source.schema_name", "source.table"])
def test_delta_origin_requires_schema_and_table(missing):
    params = {k: v for k, v in DELTA_PARAMS.items() if k != missing}
    with pytest.raises(ConfigValidationError, match=missing):
        load_config(params)


def test_missing_origin_fields_are_reported_together():
    params = {
        k: v for k, v in MINIMAL_PARAMS.items() if k not in ("source.path", "source.directory")
    }
    with pytest.raises(ConfigValidationError) as exc_info:
        load_config(params)
    message = str(exc_info.value)
    assert "source.path" in message and "source.directory" in message


def test_deletes_table_rejected_on_file_origin():
    params = {**MINIMAL_PARAMS, "source.deletes_table": "AGENTS_DELETES"}
    with pytest.raises(ConfigValidationError, match="requires source.origin=delta"):
        load_config(params)


def test_anchor_dt_rejected_on_delta_origin():
    """A delta origin reads the __ANCHOR_DT its Bronze already wrote."""
    params = {**DELTA_PARAMS, "source.anchor_dt.column": "UPDATED_DATE"}
    with pytest.raises(ConfigValidationError, match="source.anchor_dt applies to file origins"):
        load_config(params)


_UPSERT_PARAMS = {**MINIMAL_PARAMS, "output.verb": "upsert", "output.keys": "ID"}


def test_default_dedup_needs_an_order_on_a_keyed_verb():
    errors = validate_requirements(load_config(_UPSERT_PARAMS))
    assert any("output.dedup.order_by" in error for error in errors)


def test_default_dedup_orders_by_event_time():
    params = {**_UPSERT_PARAMS, "output.event_time.column": "UPDATED_AT"}
    assert validate_requirements(load_config(params)) == []


def test_dedup_order_not_required_when_disabled():
    params = {**_UPSERT_PARAMS, "output.dedup.enabled": "false"}
    assert validate_requirements(load_config(params)) == []


def test_default_dedup_ignored_by_unkeyed_verbs():
    assert validate_requirements(load_config(MINIMAL_PARAMS)) == []


@pytest.mark.parametrize("table", ["agents", "Agents", "AGENTS_v2"])
def test_output_table_must_be_uppercase(table):
    with pytest.raises(ConfigValidationError, match="UPPERCASE"):
        load_config({**MINIMAL_PARAMS, "output.table": table})


@pytest.mark.parametrize("table", ["AGENTS", "AGENTS_V2", "AGENTS_2024"])
def test_uppercase_table_names_accepted(table):
    config = load_config({**MINIMAL_PARAMS, "output.table": table})
    assert config.output.table == table


def test_rename_pattern_without_equals_is_rejected():
    """Without the check, a bare regex would silently mean 'delete every match'."""
    params = {**MINIMAL_PARAMS, "source.rename_patterns": "__[Vv]$"}
    with pytest.raises(ConfigValidationError, match="missing '='"):
        load_config(params)


def test_rename_pattern_with_an_invalid_regex_is_rejected():
    params = {**MINIMAL_PARAMS, "source.rename_patterns": "[unclosed="}
    with pytest.raises(ConfigValidationError, match="not a valid regex"):
        load_config(params)


def test_valid_rename_patterns_are_parsed_as_a_list():
    params = {**MINIMAL_PARAMS, "source.rename_patterns": "__[Vv]$=,^PREFIX_="}

    config = load_config(params)

    assert config.source.rename_patterns == ["__[Vv]$=", "^PREFIX_="]
