"""Step 1 — write the week of Elluminate snapshots the pipeline never picked up.

Seven daily exports, each carrying every study whether or not anything changed —
that is what a full-snapshot feed looks like, and it is why TST_ST_111 appears
identically seven times.

Each file name carries the 14-digit stamp that __EXPORT_DATE and the snapshot id are
both derived from. UPDATEDATE is something else entirely: the record's own edit time,
in Elluminate's MM/dd/yyyy HH:mm:ss, and the column the validity windows are built from
once the cast config has turned it into a real timestamp.
"""

import os
import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    FILE_PREFIX,
    HEADER,
    INBOUND,
    SNAPSHOTS,
    SOURCE_DIRECTORY,
    export_stamp,
    snapshot_rows,
)

directory = f"{INBOUND}/{SOURCE_DIRECTORY}"
os.makedirs(directory, exist_ok=True)

for offset in range(SNAPSHOTS):
    path = f"{directory}/{FILE_PREFIX}_{export_stamp(offset)}.csv"
    rows = snapshot_rows(offset)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("\n".join([HEADER, *rows]) + "\n")
    print(f"wrote {len(rows)} rows to {path}")
