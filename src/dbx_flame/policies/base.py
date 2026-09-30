"""Severity, the result shape the audit log is fed from, and the gate's exception."""

from __future__ import annotations

from dataclasses import dataclass

from dbx_flame.context.config import Severity


@dataclass(frozen=True)
class PolicyResult:
    """One check's verdict, in the shape the audit row needs.

    Deliberately no sample rows. DQX already reports which check failed and why, and
    copying offending source rows into the audit table would put data under a set of
    grants that has nothing to do with the table it came from.
    """

    policy: str
    passed: bool
    severity: Severity
    failed_count: int = 0
    details: str = ""


class PolicyViolation(Exception):
    """Raised when one or more fail-severity policies are violated."""

    def __init__(self, results: list[PolicyResult]):
        self.results = results
        names = [result.policy for result in results]
        super().__init__(f"Policy violations: {', '.join(names)}")
