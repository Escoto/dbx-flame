"""Origin → source reader registry.

Adding an origin means adding a reader and one entry here; nothing in the Start
layer or the entrypoint needs a matching edit.
"""

from __future__ import annotations

from dbx_flame.context.config import Origin
from dbx_flame.pipelines.base import SourcePipeline
from dbx_flame.pipelines.csv_source import CsvSource
from dbx_flame.pipelines.table_source import TableSource
from dbx_flame.pipelines.json_source import JsonSource
from dbx_flame.pipelines.sas_source import SasSource

# json and sas are registered but still raise on read (P6): an origin the config
# accepts should fail where it is unimplemented, not look unknown at dispatch.
SOURCES: dict[Origin, type[SourcePipeline]] = {
    Origin.CSV: CsvSource,
    Origin.JSON: JsonSource,
    Origin.SAS: SasSource,
    Origin.TABLE: TableSource,
}
