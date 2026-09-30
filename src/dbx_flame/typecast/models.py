"""Cast configuration models — loaded from the detached per-table YAML file."""

from __future__ import annotations

from typing import Optional

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError


class CastSpec(BaseModel):
    """A column's target type, plus the parse format for date/timestamp targets."""

    model_config = ConfigDict(extra="forbid")

    target_type: str
    format: Optional[str] = None


class ColumnCastConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    cast: CastSpec


class CastConfiguration(BaseModel):
    """The detached YAML: column types and nothing else.

    Whether casts are validated is a workflow-level switch (typing.validate_casts)
    rather than a property of the type list, so it stays in the task config. Extra
    keys are rejected here for the same reason they are in the task config: a typo
    that silently does nothing is worse than a failed run.
    """

    model_config = ConfigDict(extra="forbid")

    columns: list[ColumnCastConfig] = []


def load_cast_configuration(path: str) -> CastConfiguration:
    """Read the cast YAML.

    Workspace and Volume paths are ordinary files on Databricks, so a plain open()
    is all this needs.
    """
    with open(path, encoding="utf-8") as handle:
        try:
            return CastConfiguration(**(yaml.safe_load(handle) or {}))
        except (yaml.YAMLError, ValidationError) as exc:
            raise ValueError(f"Invalid cast config '{path}': {exc}") from exc
