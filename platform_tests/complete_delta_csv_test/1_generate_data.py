"""Step 1 — drop this round's CSV exports into the inbound directory.

Round 1 writes three exports at once. That backlog is the whole point: they all reach
bronze before silver runs, and COMPLETE_DELTA has to split them apart again and replay
them in order. Plain SCD2 given the same input would keep only A's newest version and
the v1→v2→v3 chain would never exist.

File names carry the iso stamp (2026-09-30T10:35:00) Bronze parses __EXPORT_DATE from,
two seconds apart so the order is unambiguous. This is the suite's one iso source; the
rest use datetime. CHANGE_TS is something else entirely: the source's own event time,
and what the validity windows are built from.
"""

import datetime
import os
import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import HEADER, INBOUND, ROUNDS, SOURCE_DIRECTORY  # noqa: E402

STAMP = "%Y-%m-%dT%H:%M:%S"

round_number = sys.argv[4]
exports = ROUNDS[round_number]

directory = f"{INBOUND}/{SOURCE_DIRECTORY}"
os.makedirs(directory, exist_ok=True)

now = datetime.datetime.now()
for offset, rows in enumerate(exports):
    name = (now + datetime.timedelta(seconds=offset * 2)).strftime(STAMP)
    path = f"{directory}/{name}.csv"
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("\n".join([HEADER, *rows]) + "\n")
    print(f"wrote {path}")
