"""CastService — apply column types with single-pass silent-NULL validation."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from dbx_flame.policies.platform import PlatformPolicy, violate
from dbx_flame.typecast.models import CastSpec, load_cast_configuration

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.sql import Column, DataFrame

    from dbx_flame.context.config import TypingConfig
    from dbx_flame.context.context import Context

_STRING = "string"
_METADATA_PREFIX = "__"
_ORIGINAL_PREFIX = "_orig_"
_FORMATTED_TYPES: dict[str, Callable[..., Column]] = {
    "date": F.to_date,
    "timestamp": F.to_timestamp,
}
_SOURCE = "typecast"


class MissingColumnException(Exception):
    """Raised when the cast config names a column the DataFrame does not have."""


def _is_metadata(column: str) -> bool:
    """Framework columns (__EXPORT_DATE, __FILEPATH, ...) are exempt from user casts."""
    return column.startswith(_METADATA_PREFIX)


def _quoted(column: str) -> Column:
    """Backticked: casting may run before names have been sanitized."""
    return F.col(f"`{column}`")


def _cast(column: str, spec: CastSpec) -> Column:
    target = spec.target_type.strip().lower()
    formatter = _FORMATTED_TYPES.get(target)
    if formatter and spec.format:
        return formatter(_quoted(column), spec.format).alias(column)
    return _quoted(column).cast(spec.target_type).alias(column)


def _became_null(column: str) -> Column:
    """A value that was present before the cast and is NULL after it."""
    original = _quoted(f"{_ORIGINAL_PREFIX}{column}")
    return original.isNotNull() & (F.trim(original) != "") & _quoted(column).isNull()


def _order(df: DataFrame, casts: dict[str, CastSpec]) -> list[str]:
    """Declared columns first, in YAML order, then the rest, then framework metadata.

    The config doubles as a column ordering, which is why it is worth honouring even
    for the columns it only names rather than retypes.
    """
    untouched = [c for c in df.columns if c not in casts and not _is_metadata(c)]
    metadata = [c for c in df.columns if _is_metadata(c)]
    return list(casts) + untouched + metadata


class CastService:
    """Apply the casts the detached config declares, and only those.

    A column the config does not name keeps the type it arrived with. Coming out of
    Auto Loader that is string, so bronze is unaffected; promoting an already-typed
    table is where it matters, because forcing the undeclared columns to string there
    would quietly undo the types the previous layer established.
    """

    def apply(self, df: DataFrame, cfg: TypingConfig, ctx: Context) -> DataFrame:
        casts = self._resolve(df, cfg, ctx)
        order = _order(df, casts)

        # Casting to string cannot turn a present value into NULL, so a config that
        # only declares strings has nothing to check and is spared the extra pass.
        checked = [
            column for column, spec in casts.items() if spec.target_type.strip().lower() != _STRING
        ]
        validating = bool(cfg.validate_casts and checked)

        projection = [
            _cast(column, casts[column]) if column in casts else _quoted(column)
            for column in order
        ]
        if validating:
            projection += [
                _quoted(column).cast(_STRING).alias(f"{_ORIGINAL_PREFIX}{column}")
                for column in checked
            ]

        casted = df.select(*projection)
        if not validating:
            return casted

        self._reject_silent_nulls(casted, checked, ctx)
        return casted.drop(*[f"{_ORIGINAL_PREFIX}{column}" for column in checked])

    def _resolve(self, df: DataFrame, cfg: TypingConfig, ctx: Context) -> dict[str, CastSpec]:
        """The casts the config declares, in YAML order. Nothing is added to this."""
        casts: dict[str, CastSpec] = {}

        if cfg.cast_config:
            configuration = load_cast_configuration(cfg.cast_config)
            available = set(df.columns)
            missing: list[str] = []
            exempt: list[str] = []
            for configured in configuration.columns:
                if _is_metadata(configured.name):
                    exempt.append(configured.name)
                elif configured.name not in available:
                    missing.append(configured.name)
                else:
                    casts[configured.name] = configured.cast  # a repeated column: last wins
            if missing:
                raise MissingColumnException(
                    f"{cfg.cast_config} configures columns the source does not have: "
                    + ", ".join(missing)
                )
            if exempt:
                ctx.logger.warning(
                    name="cast_config_ignored_metadata",
                    source=_SOURCE,
                    description=(
                        "Framework metadata columns are not user-castable and were "
                        "left as they are: " + ", ".join(exempt)
                    ),
                    total=len(exempt),
                )

        return casts

    def _reject_silent_nulls(self, casted: DataFrame, columns: list[str], ctx: Context) -> None:
        """One bounded pass over the batch — no second stream, no /tmp checkpoints.

        A single bad value condemns the column, so this keeps at most one example per
        column rather than counting or sampling. That makes it one aggregate whose
        memory does not grow with the batch, and — unlike a capped row scan — every
        checked column is represented no matter how many rows failed in its neighbours.
        """
        checks = {column: _became_null(column) for column in columns}
        example = casted.agg(
            *[
                F.first(
                    F.when(check, _quoted(f"{_ORIGINAL_PREFIX}{column}")), ignorenulls=True
                ).alias(column)
                for column, check in checks.items()
            ]
        ).collect()[0]

        offenders = {column: example[column] for column in checks if example[column] is not None}
        if not offenders:
            return

        detail = ", ".join(
            f"{column} (e.g. {value})" for column, value in sorted(offenders.items())
        )
        message = f"Values became NULL after casting: {detail}"
        violate(
            ctx,
            PlatformPolicy.CAST_SILENT_NULL,
            _SOURCE,
            message,
            # columns affected; the row count is deliberately not gathered
            total=len(offenders),
            metadata=json.dumps(offenders, sort_keys=True),
        )
