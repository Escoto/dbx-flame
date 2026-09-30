"""The DQX ruleset — read the detached YAML and validate it before anything runs."""

from __future__ import annotations

import yaml
from databricks.labs.dqx.engine import DQEngine


class ChecksValidationError(Exception):
    """Raised when the ruleset cannot be read, or DQX rejects what it contains."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("; ".join(errors))


def load_checks(path: str) -> list[dict]:
    """Read a DQX ruleset and hand back the checks DQX's metadata API expects.

    Volume and Workspace paths are ordinary files on Databricks, so a plain open() is
    enough — the same reason the cast config is read this way. Going through DQX's own
    storage layer would mean constructing an engine, and an engine wants a workspace
    client that reading a file has no reason to hold.

    Validation happens here, at Start, rather than at the first batch: a misspelled
    check function should fail the task before it reads any data.
    """
    try:
        with open(path, encoding="utf-8") as handle:
            checks = yaml.safe_load(handle) or []
    except (OSError, yaml.YAMLError) as exc:
        raise ChecksValidationError(
            [f"policies.checks_file '{path}' could not be read: {exc}"]
        ) from exc

    if not isinstance(checks, list):
        raise ChecksValidationError(
            [
                f"policies.checks_file '{path}' must hold a list of checks, got {type(checks).__name__}"
            ]
        )

    # Static on DQEngine, so this catches bad functions and arguments with no
    # workspace client.
    status = DQEngine.validate_checks(checks)
    if status.has_errors:
        raise ChecksValidationError(
            [f"policies.checks_file '{path}': {error}" for error in status.errors]
        )

    return checks
