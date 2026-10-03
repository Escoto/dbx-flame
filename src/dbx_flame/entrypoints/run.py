"""Single wheel entry point: params → Context → Pipeline → Typing → Policies → Output."""

from __future__ import annotations

import json
import sys
from typing import TYPE_CHECKING

from pyspark.sql import SparkSession

from dbx_flame.context.context import RunIdentity
from dbx_flame.context.loader import build_context, load_config
from dbx_flame.entrypoints.pipeline import run_pipeline
from dbx_flame.output.mechanics import table_version

if TYPE_CHECKING:
    from dbx_flame.context.context import Context

# Supplied by the workflow as Databricks dynamic values; see docs/03_config_schema.md.
LOCAL = "local"
IDENTITY_PARAMS = (
    "__workflow_id",
    "__workflow_run_id",
    "__task_key",
    "__task_run_id",
)


def _parse_argv(argv: list[str]) -> dict[str, str]:
    params: dict[str, str] = {}
    for arg in argv:
        if arg.startswith("--") and "=" in arg:
            key, value = arg[2:].split("=", 1)
            params[key] = value
    return params


def _run_identity(params: dict[str, str]) -> tuple[RunIdentity, list[str]]:
    """Pull the identity parameters out of params, defaulting any that are absent.

    Returns the identity plus the names that fell back, so the caller can record
    that the run is not fully attributable rather than silently pretending it is.
    """
    defaulted = [name for name in IDENTITY_PARAMS if name not in params]
    values = [params.pop(name, LOCAL) for name in IDENTITY_PARAMS]
    return RunIdentity(*values), defaulted


def _log_promotion_start(ctx: Context) -> None:
    """Record the target's version before any data moves.

    A failure that no retry repairs is undone by restoring the target to this version.
    """
    source = ctx.source_table or ctx.inbound_glob
    version = table_version(ctx)
    current = (
        f"Current {ctx.target_table} version #{version}."
        if version is not None
        else f"Current {ctx.target_table} version: none (table does not exist yet)."
    )
    ctx.logger.info(
        name="promotion_start",
        source="run",
        description=f"Starting data promotion from {source} to {ctx.target_table}.\n{current}",
        total=version,
    )


def main() -> None:
    """Entry point for python_wheel_task. Reads named parameters and executes the pipeline."""
    params = _parse_argv(sys.argv[1:])

    run, defaulted = _run_identity(params)

    config = load_config(params)

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    ctx = build_context(config, spark, run)

    if defaulted:
        ctx.logger.warning(
            name="run_identity_missing",
            source="run",
            description=(
                f"Run identity not supplied; defaulted to '{LOCAL}': " + ", ".join(defaulted)
            ),
        )

    # Deliberately unguarded: this is the first write of the run, so it doubles as a
    # smoke test of the audit path (table, schema, permissions). If it cannot land,
    # the task fails here — before any pipeline work — rather than doing the work and
    # losing its audit trail at the end.
    ctx.logger.info(
        name="pipeline_start",
        source="run",
        description=f"Pipeline dispatched with {len(params)} parameters",
        metadata=json.dumps(params, sort_keys=True),
    )
    _log_promotion_start(ctx)
    ctx.logger.flush()

    try:
        run_pipeline(ctx)
        ctx.logger.info(
            name="pipeline_complete",
            source="run",
            description=(
                f"{config.source.origin.value} → {config.output.verb.value} "
                f"→ {ctx.target_table}"
            ),
        )
    except Exception as exc:
        ctx.logger.error(name="pipeline_failure", source="run", description=str(exc))
        raise
    finally:
        # in finally, not per-branch: a cancelled run raises BaseException, which
        # never reaches `except Exception`, and its buffered rows would be lost.
        ctx.logger.flush()


if __name__ == "__main__":
    sys.exit(main() or 0)
