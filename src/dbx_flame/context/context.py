"""The Context object: validated config + Spark session + run identity + logger."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from dbx_flame.context.config import IncrementStrategy, TaskConfig
    from dbx_flame.observability.audit_logger import AuditLogger


@dataclass(frozen=True)
class RunIdentity:
    """Databricks identifiers for one task execution.

    Two stable ids (which workflow, which task) and two run ids (which execution
    of each), so audit rows can be both trended and pinned to a single run.
    """

    workflow_id: str
    workflow_run_id: str
    task_key: str
    task_run_id: str


@dataclass(frozen=True)
class Context:
    config: TaskConfig
    spark: SparkSession
    run: RunIdentity
    logger: AuditLogger
    catalog: str
    source_table: Optional[str]
    deletes_table: Optional[str]
    target_table: str
    inbound_glob: Optional[str]
    checkpoint_location: str
    schema_hints_location: str
    increment_strategy: IncrementStrategy

    # The DQX ruleset, read and validated at Start. It rides on the Context because the
    # runner executes inside foreachBatch, where DQX cannot reach a workspace to load it.
    checks: list[dict[str, Any]] = field(default_factory=list)

    # When a full read stamps a table we didn't create. Taken once per run, so the updates
    # and deletes reads share one snapshot, and every action re-evaluating a frame gets the
    # same value, which current_timestamp() would not guarantee.
    read_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
