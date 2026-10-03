"""Pre-processor registry — named DataFrame transforms applied after reading."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Protocol, runtime_checkable

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from dbx_flame.context.context import Context


@runtime_checkable
class PreProcessor(Protocol):
    """A named, config-selected DataFrame transform."""

    name: ClassVar[str]

    def apply(self, df: DataFrame, ctx: Context) -> DataFrame: ...


# The envelope's own columns, and what the batch carries once it is unwrapped.
_METADATA = "metadata"
_ARRAY = "data"
_PAYLOAD = "DATA"
_EXPORT_DATE = "EXPORT_DATE"

# The export stamp is 14 digits, sometimes followed by a timezone letter the format
# string cannot parse — take the digits rather than failing the batch on the suffix.
_STAMP = r"(\d{14})"
_STAMP_FORMAT = "yyyyMMddHHmmss"


def _as_json_string(df: DataFrame, column: str) -> Column:
    """The column as JSON text, whether the reader typed it or handed back a string.

    Schema hints keep both envelope columns as strings; without them Auto Loader infers
    a struct. Normalising here is what keeps the reader and this transform independent.
    """
    if dict(df.dtypes).get(column) == "string":
        return F.col(column)
    return F.to_json(F.col(column))


class RecordEnvelope:
    """Common vendor JSON envelope: metadata:export_date + data[] → URI/DATA/EXPORT_DATE."""

    name: ClassVar[str] = "record_envelope"

    def apply(self, df: DataFrame, ctx: Context) -> DataFrame:
        stamp = F.get_json_object(_as_json_string(df, _METADATA), "$.export_date")
        export_date = F.to_timestamp(F.regexp_extract(stamp, _STAMP, 1), _STAMP_FORMAT)

        # from_json to array<string> keeps each item as its own raw JSON text, so the
        # payload survives whatever the vendor changes inside it.
        items = (
            df.withColumn(_EXPORT_DATE, export_date)
            .withColumn(_PAYLOAD, F.from_json(_as_json_string(df, _ARRAY), "array<string>"))
            .withColumn(_PAYLOAD, F.explode(_PAYLOAD))
        )

        fields = ctx.config.source.envelope_fields
        lifted = [F.get_json_object(_PAYLOAD, f"$.{field}").alias(field) for field in fields]

        # Everything the reader added stays: provenance is attached upstream of this and
        # COMPLETE_DELTA cuts its snapshots from __FILEPATH.
        carried = [column for column in df.columns if column not in (_METADATA, _ARRAY)]
        return items.select(*lifted, _PAYLOAD, _EXPORT_DATE, *carried)


# No generic flattener on purpose: exploding nested arrays multiplies rows and denormalizes
# the data, so how to flatten a payload is the consumer's decision, not the framework's.
PREPROCESSORS: dict[str, type[PreProcessor]] = {
    "record_envelope": RecordEnvelope,
}


def resolve(names: list[str]) -> list[PreProcessor]:
    """Instantiate the named pre-processors, in configured order.

    Unknown names are rejected at Start (loader.validate_requirements), so by the
    time the pipeline runs every name here is registered.
    """
    return [PREPROCESSORS[name]() for name in names]


def apply_preprocessors(df: DataFrame, ctx: Context) -> DataFrame:
    """Run the configured pre-processors over the batch, in order."""
    for preprocessor in resolve(ctx.config.source.preprocessors):
        df = preprocessor.apply(df, ctx)
    return df
