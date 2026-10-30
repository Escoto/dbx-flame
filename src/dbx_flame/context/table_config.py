"""The table_config file: a target table's comment and tags, and its columns' casts,
comments and tags. Read and validated at Start."""

from __future__ import annotations

from typing import Annotated, Any, Optional

import yaml
from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    ValidationError,
    field_validator,
    model_validator,
)

from dbx_flame.mechanics.columns import is_framework_column


class TableConfigError(Exception):
    """Raised when the table_config file cannot be read, or what it holds is invalid."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("; ".join(errors))


def _tags_are_strings(tags: Any) -> Any:
    """YAML reads an unquoted no, yes, on or off as a boolean, which a tag can't carry."""
    if isinstance(tags, dict):
        bad = [
            f"{name}: {value}"
            for name, value in tags.items()
            if not (isinstance(name, str) and isinstance(value, str))
        ]
        if bad:
            raise ValueError(
                'tag names and values must be quoted strings, e.g. PII: "no"; '
                f"got {', '.join(bad)}"
            )
    return tags


def _comment_has_text(comment: Optional[str]) -> Optional[str]:
    if comment is not None and not comment.strip():
        raise ValueError("comment must not be empty; leave it out instead")
    return comment


Tags = Annotated[dict[str, str], BeforeValidator(_tags_are_strings)]
Comment = Annotated[Optional[str], AfterValidator(_comment_has_text)]


class CastSpec(BaseModel):
    """A column's target type, plus the parse format for date/timestamp targets."""

    model_config = ConfigDict(extra="forbid")

    target_type: str
    format: Optional[str] = None


class ColumnConfig(BaseModel):
    """One source column: its cast, comment and tags, each optional but not all absent."""

    model_config = ConfigDict(extra="forbid")

    name: str
    cast: Optional[CastSpec] = None
    comment: Comment = None
    tags: Tags = {}

    @field_validator("name")
    @classmethod
    def _name_is_a_source_column(cls, value: str) -> str:
        # The framework owns its columns: their types, and the comments it keeps on them.
        if is_framework_column(value):
            raise ValueError(f"'{value}' is a framework column and can't be configured")
        return value

    @model_validator(mode="after")
    def _configures_something(self) -> "ColumnConfig":
        if self.cast is None and self.comment is None and not self.tags:
            raise ValueError(f"column '{self.name}' sets none of cast, comment or tags")
        return self


class TableConfig(BaseModel):
    """The detached YAML a task names in table_config.

    Every key is optional, but a file that sets nothing would do nothing, so it is
    rejected. Extra keys are rejected too: a typo that silently does nothing is worse
    than a failed run.
    """

    model_config = ConfigDict(extra="forbid")

    comment: Comment = None
    tags: Tags = {}
    columns: list[ColumnConfig] = []

    @model_validator(mode="after")
    def _configures_something(self) -> "TableConfig":
        if self.comment is None and not self.tags and not self.columns:
            raise ValueError("sets none of comment, tags or columns")
        return self

    @model_validator(mode="after")
    def _names_each_column_once(self) -> "TableConfig":
        names = [column.name for column in self.columns]
        repeated = sorted({name for name in names if names.count(name) > 1})
        if repeated:
            raise ValueError(f"columns are listed more than once: {', '.join(repeated)}")
        return self

    @property
    def casts(self) -> dict[str, CastSpec]:
        """The declared casts, in YAML order."""
        return {column.name: column.cast for column in self.columns if column.cast}


def load_table_config(path: str) -> TableConfig:
    """Read and validate the table_config file.

    Workspace and Volume paths are ordinary files on Databricks, so a plain open() is
    all this needs.
    """
    try:
        with open(path, encoding="utf-8") as handle:
            raw = yaml.safe_load(handle)
    except (OSError, yaml.YAMLError) as exc:
        raise TableConfigError([f"table_config '{path}' could not be read: {exc}"]) from exc

    if raw is None:
        raise TableConfigError([f"table_config '{path}' is empty"])
    if not isinstance(raw, dict):
        raise TableConfigError(
            [f"table_config '{path}' must hold a mapping, got {type(raw).__name__}"]
        )

    try:
        return TableConfig(**raw)
    except ValidationError as exc:
        errors = []
        for err in exc.errors():
            loc = ".".join(str(part) for part in err["loc"])
            msg = err["msg"].removeprefix("Value error, ")
            errors.append(f"table_config '{path}': {loc + ': ' if loc else ''}{msg}")
        raise TableConfigError(errors) from exc
