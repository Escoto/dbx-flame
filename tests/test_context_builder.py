"""Tests for build_context — TaskConfig + Spark → frozen Context."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dbx_flame.context.config import (
    DeletesConfig,
    EventTimeConfig,
    IncrementStrategy,
    Origin,
    OutputConfig,
    PoliciesConfig,
    SnapshotScope,
    SnapshotTimePattern,
    SourceConfig,
    TaskConfig,
    TypingConfig,
    Verb,
)
from dbx_flame.context.context import RunIdentity
from dbx_flame.context.loader import (
    ConfigValidationError,
    build_context,
    validate_requirements,
)
from dbx_flame.output.registry import VERB_REQUIREMENTS, WRITERS

RUN = RunIdentity(
    workflow_id="wf-1",
    workflow_run_id="wfrun-1",
    task_key="bronze_to_silver",
    task_run_id="taskrun-1",
)


def _make_config(**overrides) -> TaskConfig:
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
    spark.createDataFrame.return_value = MagicMock()
    spark.createDataFrame.return_value.write = MagicMock()
    return spark


def test_catalog_resolution(mock_spark):
    config = _make_config(catalog="cro", env="dev_01")
    ctx = build_context(config, mock_spark, RUN)
    assert ctx.catalog == "cro_dev_01"


def test_target_table_fqn(mock_spark):
    config = _make_config(
        output=OutputConfig(verb=Verb.APPEND, schema_name="silver_cro", table="SUBJECTS"),
    )
    ctx = build_context(config, mock_spark, RUN)
    assert ctx.target_table == "`cro_dev_01`.`silver_cro`.`SUBJECTS`"


def test_source_table_for_delta_origin(mock_spark):
    config = _make_config(
        source=SourceConfig(
            origin=Origin.DELTA,
            schema_name="bronze_cro",
            table="SUBJECTS_UPDATES",
        ),
    )
    ctx = build_context(config, mock_spark, RUN)
    assert ctx.source_table == "`cro_dev_01`.`bronze_cro`.`SUBJECTS_UPDATES`"


def test_source_table_none_for_file_origin(mock_spark):
    config = _make_config()
    ctx = build_context(config, mock_spark, RUN)
    assert ctx.source_table is None


def test_deletes_table_set(mock_spark):
    config = _make_config(
        source=SourceConfig(
            origin=Origin.DELTA,
            schema_name="bronze_cro",
            table="SUBJECTS_UPDATES",
            deletes_table="SUBJECTS_DELETES",
        ),
        # a deletes feed is only valid for a verb that supports one
        output=OutputConfig(
            verb=Verb.COMPLETE_DELTA,
            schema_name="silver_cro",
            table="SUBJECTS",
            keys=["ID"],
            event_time=EventTimeConfig(column="MODIFIED"),
            deletes=DeletesConfig(keys=["ID"], event_time=EventTimeConfig(column="DELETED_AT")),
        ),
    )
    ctx = build_context(config, mock_spark, RUN)
    assert ctx.deletes_table == "`cro_dev_01`.`bronze_cro`.`SUBJECTS_DELETES`"


def test_deletes_table_none_when_absent(mock_spark):
    config = _make_config()
    ctx = build_context(config, mock_spark, RUN)
    assert ctx.deletes_table is None


def test_inbound_glob_csv(mock_spark):
    config = _make_config(
        source=SourceConfig(origin=Origin.CSV, path="/Volumes/in", directory="agents"),
    )
    ctx = build_context(config, mock_spark, RUN)
    assert ctx.inbound_glob == "/Volumes/in/agents/*.csv"


def test_inbound_glob_with_file_extension_override(mock_spark):
    config = _make_config(
        source=SourceConfig(
            origin=Origin.CSV,
            path="/Volumes/in",
            directory="agents",
            file_extension="txt",
        ),
    )
    ctx = build_context(config, mock_spark, RUN)
    assert ctx.inbound_glob == "/Volumes/in/agents/*.txt"


def test_inbound_glob_none_for_delta(mock_spark):
    config = _make_config(
        source=SourceConfig(
            origin=Origin.DELTA,
            schema_name="bronze_cro",
            table="SUBJECTS_UPDATES",
        ),
    )
    ctx = build_context(config, mock_spark, RUN)
    assert ctx.inbound_glob is None


def test_checkpoint_location(mock_spark):
    config = _make_config(metadata_path="/Volumes/meta/")
    ctx = build_context(config, mock_spark, RUN)
    assert ctx.checkpoint_location == "/Volumes/meta/cro_dev_01/bronze_cro/AGENTS/_checkpoint/"


def test_schema_hints_location(mock_spark):
    config = _make_config(metadata_path="/Volumes/meta/")
    ctx = build_context(config, mock_spark, RUN)
    assert ctx.schema_hints_location == "/Volumes/meta/cro_dev_01/bronze_cro/AGENTS/_schema_hints/"


def test_run_identity(mock_spark):
    run = RunIdentity(
        workflow_id="42",
        workflow_run_id="run-777",
        task_key="inbound_to_bronze",
        task_run_id="taskrun-888",
    )
    ctx = build_context(_make_config(), mock_spark, run)
    assert ctx.run.workflow_id == "42"
    assert ctx.run.workflow_run_id == "run-777"
    assert ctx.run.task_key == "inbound_to_bronze"
    assert ctx.run.task_run_id == "taskrun-888"


def test_verb_requires_keys_for_upsert(mock_spark):
    config = _make_config(
        output=OutputConfig(verb=Verb.UPSERT, schema_name="gold", table="AGG"),
    )
    with pytest.raises(ConfigValidationError, match="output.keys"):
        build_context(config, mock_spark, RUN)


def test_verb_requires_event_time_for_scd2(mock_spark):
    config = _make_config(
        output=OutputConfig(verb=Verb.SCD2, schema_name="silver", table="T", keys=["ID"]),
    )
    with pytest.raises(ConfigValidationError, match="output.event_time"):
        build_context(config, mock_spark, RUN)


def test_snapshot_time_pattern_is_refused_on_a_delta_origin():
    """A delta origin reads the __EXPORT_DATE its Bronze already parsed."""
    with pytest.raises(ValueError, match="snapshot_time_pattern applies to file origins"):
        SourceConfig(
            origin=Origin.DELTA,
            schema_name="bronze",
            table="T_UPDATES",
            snapshot_time_pattern=SnapshotTimePattern.DATETIME,
        )


def test_verb_append_no_extra_requirements(mock_spark):
    config = _make_config(
        output=OutputConfig(verb=Verb.APPEND, schema_name="bronze_cro", table="AGENTS"),
    )
    ctx = build_context(config, mock_spark, RUN)
    assert ctx.config.output.verb == Verb.APPEND


def test_verb_full_no_extra_requirements(mock_spark):
    config = _make_config(
        output=OutputConfig(verb=Verb.FULL, schema_name="bronze_cro", table="AGENTS"),
    )
    ctx = build_context(config, mock_spark, RUN)
    assert ctx.config.output.verb == Verb.FULL


def test_scd2_with_all_requirements(mock_spark):
    config = _make_config(
        source=SourceConfig(
            origin=Origin.DELTA,
            schema_name="bronze",
            table="T_UPDATES",
        ),
        output=OutputConfig(
            verb=Verb.SCD2,
            schema_name="silver",
            table="T",
            keys=["ID"],
            event_time=EventTimeConfig(column="MODIFIED"),
        ),
    )
    ctx = build_context(config, mock_spark, RUN)
    assert ctx.config.output.verb == Verb.SCD2


def test_complete_delta_with_all_requirements(mock_spark):
    config = _make_config(
        source=SourceConfig(
            origin=Origin.DELTA,
            schema_name="bronze",
            table="T_UPDATES",
        ),
        output=OutputConfig(
            verb=Verb.COMPLETE_DELTA,
            schema_name="silver",
            table="T",
            keys=["ID"],
            event_time=EventTimeConfig(column="DT"),
        ),
    )
    ctx = build_context(config, mock_spark, RUN)
    assert ctx.config.output.verb == Verb.COMPLETE_DELTA


# --- origin x verb requirements ------------------------------------------------


def _complete_delta_output() -> OutputConfig:
    return OutputConfig(
        verb=Verb.COMPLETE_DELTA,
        schema_name="silver",
        table="T",
        keys=["ID"],
        event_time=EventTimeConfig(column="DT"),
    )


def test_complete_delta_rejects_file_origin(mock_spark):
    config = _make_config(
        source=SourceConfig(
            origin=Origin.CSV,
            path="/Volumes/in/",
            directory="agents",
            snapshot_time_pattern=SnapshotTimePattern.DATETIME,
        ),
        output=_complete_delta_output(),
    )
    with pytest.raises(ConfigValidationError, match="does not support source.origin=csv"):
        build_context(config, mock_spark, RUN)


def test_complete_delta_accepts_delta_origin():
    config = _make_config(
        source=SourceConfig(
            origin=Origin.DELTA,
            schema_name="bronze",
            table="T_UPDATES",
        ),
        output=_complete_delta_output(),
    )
    assert validate_requirements(config) == []


def test_error_names_the_supported_origins():
    config = _make_config(
        source=SourceConfig(
            origin=Origin.SAS,
            path="/Volumes/in/",
            directory="agents",
            snapshot_time_pattern=SnapshotTimePattern.DATETIME,
        ),
        output=_complete_delta_output(),
    )
    errors = validate_requirements(config)
    assert len(errors) == 1
    assert "supported: delta" in errors[0]


def test_every_other_verb_accepts_any_origin():
    for verb in (Verb.APPEND, Verb.FULL, Verb.UPSERT, Verb.SCD2):
        for origin in Origin:
            assert origin in VERB_REQUIREMENTS[verb].origins


def test_deletes_feed_rejected_for_append(mock_spark):
    config = _make_config(
        source=SourceConfig(
            origin=Origin.DELTA,
            schema_name="bronze",
            table="T_UPDATES",
            deletes_table="T_DELETES",
        ),
    )
    with pytest.raises(ConfigValidationError, match="does not support a deletes feed"):
        build_context(config, mock_spark, RUN)


def test_output_deletes_rejected_for_upsert():
    config = _make_config(
        output=OutputConfig(
            verb=Verb.UPSERT,
            schema_name="gold",
            table="AGG",
            keys=["ID"],
            deletes=DeletesConfig(keys=["ID"]),
        ),
    )
    errors = validate_requirements(config)
    assert any("deletes feed" in e for e in errors)


def test_deletes_feed_rejected_for_scd2():
    """SCD2 has no deletes feed, because a stream has no way to cut one at the
    same point as its updates. Deletes stay a COMPLETE_DELTA feature."""
    config = _make_config(
        source=SourceConfig(
            origin=Origin.DELTA,
            schema_name="bronze",
            table="T_UPDATES",
            deletes_table="T_DELETES",
        ),
        output=OutputConfig(
            verb=Verb.SCD2,
            schema_name="silver",
            table="T",
            keys=["ID"],
            event_time=EventTimeConfig(column="MODIFIED"),
        ),
    )
    errors = validate_requirements(config)
    assert any("does not support a deletes feed" in error for error in errors)


def test_deletes_feed_allowed_for_complete_delta():
    config = _make_config(
        source=SourceConfig(
            origin=Origin.DELTA,
            schema_name="bronze",
            table="T_UPDATES",
            deletes_table="T_DELETES",
        ),
        output=OutputConfig(
            verb=Verb.COMPLETE_DELTA,
            schema_name="silver",
            table="T",
            keys=["ID"],
            event_time=EventTimeConfig(column="MODIFIED"),
            deletes=DeletesConfig(keys=["ID"], event_time=EventTimeConfig(column="DELETED_AT")),
        ),
    )
    assert validate_requirements(config) == []


def test_half_a_deletes_feed_is_rejected():
    """A deletes table with no keys retires nothing and still reports success."""
    config = _make_config(
        source=SourceConfig(
            origin=Origin.DELTA,
            schema_name="bronze",
            table="T_UPDATES",
            deletes_table="T_DELETES",
        ),
        output=OutputConfig(
            verb=Verb.COMPLETE_DELTA,
            schema_name="silver",
            table="T",
            keys=["ID"],
            event_time=EventTimeConfig(column="MODIFIED"),
        ),
    )
    errors = validate_requirements(config)
    assert any("output.deletes.keys" in error for error in errors)


def test_snapshot_scope_full_rejected_for_append(mock_spark):
    config = _make_config(
        output=OutputConfig(
            verb=Verb.APPEND,
            schema_name="bronze_cro",
            table="AGENTS",
            snapshot_scope=SnapshotScope.FULL,
        ),
    )
    with pytest.raises(ConfigValidationError, match="snapshot_scope=full"):
        build_context(config, mock_spark, RUN)


def test_snapshot_scope_full_rejected_for_scd2():
    """SCD2 takes changes only; a full-snapshot source belongs on FULL or COMPLETE_DELTA."""
    config = _make_config(
        source=SourceConfig(origin=Origin.DELTA, schema_name="bronze", table="T_UPDATES"),
        output=OutputConfig(
            verb=Verb.SCD2,
            schema_name="silver",
            table="T",
            keys=["ID"],
            event_time=EventTimeConfig(column="MODIFIED"),
            snapshot_scope=SnapshotScope.FULL,
        ),
    )
    errors = validate_requirements(config)
    assert errors == ["output.verb=scd2 does not support output.snapshot_scope=full"]


def test_requirement_errors_aggregate():
    """A config wrong in several ways reports every problem in one pass."""
    config = _make_config(
        source=SourceConfig(origin=Origin.CSV, path="/Volumes/in/", directory="agents"),
        output=OutputConfig(
            verb=Verb.COMPLETE_DELTA,
            schema_name="silver",
            table="T",
            snapshot_scope=SnapshotScope.FULL,
        ),
    )
    errors = validate_requirements(config)
    assert len(errors) == 3
    joined = " | ".join(errors)
    assert "source.origin=csv" in joined
    assert "output.keys" in joined
    assert "output.event_time.column" in joined


def test_verb_requirements_derived_from_writers():
    """The registry must stay derived — no second hardcoded copy to drift."""
    assert len(WRITERS) == len(Verb)
    for writer in WRITERS:
        assert VERB_REQUIREMENTS[writer.verb] is writer.requires


def test_increment_strategy_declared_by_verb(mock_spark):
    """Not a parameter: the verb decides, and Context carries the resolved value."""
    delta_source = SourceConfig(origin=Origin.DELTA, schema_name="bronze", table="T_UPDATES")

    scd2 = _make_config(
        source=delta_source,
        output=OutputConfig(
            verb=Verb.SCD2,
            schema_name="silver",
            table="T",
            keys=["ID"],
            event_time=EventTimeConfig(column="MODIFIED"),
        ),
    )
    ctx = build_context(scd2, mock_spark, RUN)
    assert ctx.increment_strategy is IncrementStrategy.CHECKPOINT

    complete_delta = _make_config(
        source=SourceConfig(
            origin=Origin.DELTA,
            schema_name="bronze",
            table="T_UPDATES",
        ),
        output=_complete_delta_output(),
    )
    ctx = build_context(complete_delta, mock_spark, RUN)
    assert ctx.increment_strategy is IncrementStrategy.WATERMARK


def test_metadata_path_without_a_trailing_slash(mock_spark):
    """The separator belongs to the join, not to how the operator typed the parameter."""
    config = _make_config(metadata_path="/Volumes/meta")

    ctx = build_context(config, mock_spark, RUN)

    assert ctx.checkpoint_location == "/Volumes/meta/cro_dev_01/bronze_cro/AGENTS/_checkpoint/"
    assert ctx.schema_hints_location == "/Volumes/meta/cro_dev_01/bronze_cro/AGENTS/_schema_hints/"


def _scd2(**source_overrides) -> TaskConfig:
    source: dict[str, Any] = dict(
        origin=Origin.DELTA, schema_name="bronze_cro", table="SUBJECTS_UPDATES"
    )
    source.update(source_overrides)
    return _make_config(
        source=SourceConfig(**source),
        output=OutputConfig(
            verb=Verb.SCD2,
            schema_name="silver_cro",
            table="SUBJECTS",
            keys=["ID"],
            event_time=EventTimeConfig(column="UPDATED_DATE"),
        ),
    )


def test_scd2_can_opt_into_the_watermark_strategy():
    config = _scd2(increment_strategy=IncrementStrategy.WATERMARK)

    assert validate_requirements(config) == []


def test_a_verb_refuses_a_strategy_it_does_not_declare():
    config = _make_config(
        source=SourceConfig(
            origin=Origin.CSV,
            path="/v/",
            directory="d",
            increment_strategy=IncrementStrategy.WATERMARK,
        )
    )

    errors = validate_requirements(config)

    assert len(errors) == 1
    assert "does not support source.increment_strategy=watermark" in errors[0]
    assert "checkpoint" in errors[0]  # the error lists what the verb does support


def test_an_anchor_is_refused_outside_the_watermark_strategy():
    """A checkpoint stream has no watermark to anchor, so the parameter would do nothing."""
    config = _scd2(increment_anchor=True)

    errors = validate_requirements(config)

    assert any("source.increment_anchor only applies" in error for error in errors)


def _anchored_complete_delta(scope: SnapshotScope) -> TaskConfig:
    output = _complete_delta_output().model_copy(update={"snapshot_scope": scope})
    return _make_config(
        source=SourceConfig(
            origin=Origin.DELTA,
            schema_name="bronze",
            table="T_UPDATES",
            increment_anchor=True,
        ),
        output=output,
    )


def test_an_anchor_is_refused_under_full_scope():
    """GH #25: the anchor would hand full scope only part of the export to re-assert."""
    errors = validate_requirements(_anchored_complete_delta(SnapshotScope.FULL))

    assert errors == [
        "source.increment_anchor cannot be combined with output.snapshot_scope=full: "
        "a full export must be read whole, or the records it still carries are expired"
    ]


def test_an_anchor_is_accepted_under_delta_scope():
    assert validate_requirements(_anchored_complete_delta(SnapshotScope.DELTA)) == []


def test_the_configured_strategy_wins_over_the_verb_default(mock_spark):
    config = _scd2(increment_strategy=IncrementStrategy.WATERMARK)

    ctx = build_context(config, mock_spark, RUN)

    assert ctx.increment_strategy is IncrementStrategy.WATERMARK


def test_the_verb_default_applies_when_nothing_is_configured(mock_spark):
    ctx = build_context(_scd2(), mock_spark, RUN)

    assert ctx.increment_strategy is IncrementStrategy.CHECKPOINT
