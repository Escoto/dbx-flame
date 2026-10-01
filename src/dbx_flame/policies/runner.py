"""Policy runner — the data quality gate between Typing and the write."""

from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

from databricks.labs.dqx.engine import DefaultColumnNames, DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

from dbx_flame.context.config import Severity
from dbx_flame.policies.base import PolicyResult, PolicyViolation

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from dbx_flame.context.context import Context

_SOURCE = "Policies"

# DQX records a failure by which column it lands in rather than by a field on the
# struct, so the column is how severity is read back.
_RESULT_COLUMNS: tuple[tuple[str, Severity], ...] = (
    (DefaultColumnNames.ERRORS.value, Severity.FAIL),
    (DefaultColumnNames.WARNINGS.value, Severity.WARN),
)


def _engine() -> DQEngine:
    """A DQX engine that holds no workspace connection.

    DQX authenticates with a Databricks WorkspaceClient, which is not available inside
    foreachBatch — and evaluation needs none, because it is pure Spark. A stub client
    satisfies the constructor; this is DQX's own documented workaround. Everything that
    genuinely needs the workspace, namely reading the ruleset, happens at Start instead.
    """
    return DQEngine(MagicMock(spec=WorkspaceClient))


class PolicyRunner:
    """Evaluate the configured ruleset, log every result, refuse the batch on error.

    run() hands nothing back and the caller writes the DataFrame it already had. DQX
    evaluates by appending its result columns, and keeping that DataFrame private to
    this class is what guarantees they can never reach the target table.
    """

    def run(self, df: DataFrame, ctx: Context) -> None:
        if not ctx.checks:
            return

        results = self._evaluate(df, ctx)
        self._log(results, ctx)

        # Logged first, and every check evaluated, so one run reports everything the
        # batch broke rather than stopping at the first rule to fail.
        failures = [result for result in results if result.severity is Severity.FAIL]
        if failures:
            raise PolicyViolation(failures)

    def _evaluate(self, df: DataFrame, ctx: Context) -> list[PolicyResult]:
        """Per-check counts for everything that failed, in a single Spark job."""
        checked = _engine().apply_checks_by_metadata(df, list(ctx.checks))

        # Tagging by source column counts both severities in one job. Passing rows
        # carry NULL, which explode drops.
        tagged = [
            checked.select(
                F.lit(severity.value).alias("severity"),
                F.explode(F.col(column)).alias("result"),
            )
            for column, severity in _RESULT_COLUMNS
        ]
        counts = (
            reduce(lambda left, right: left.unionByName(right), tagged)
            .groupBy(
                "severity",
                F.col("result.name").alias("name"),
                F.col("result.message").alias("message"),
            )
            .count()
            .collect()
        )

        return [
            PolicyResult(
                policy=row["name"],
                severity=Severity(row["severity"]),
                failed_count=row["count"],
                details=row["message"],
            )
            for row in counts
        ]

    def _log(self, results: list[PolicyResult], ctx: Context) -> None:
        if not results:
            ctx.logger.info(
                name="policies_passed",
                source=_SOURCE,
                description=f"Every configured check passed on {ctx.target_table}",
                total=len(ctx.checks),
            )
            return

        for result in results:
            record = ctx.logger.error if result.severity is Severity.FAIL else ctx.logger.warning
            record(
                name=result.policy,
                source=_SOURCE,
                description=result.details,
                total=result.failed_count,
            )
