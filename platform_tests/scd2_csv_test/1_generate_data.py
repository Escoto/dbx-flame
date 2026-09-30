"""Step 1 — drop one CSV export into the inbound directory.

Which export is chosen by the round number the workflow passes as the second
parameter. One export per round is the point: SCD2 collapses to the latest version
per key *within* an increment, so landing both at once would dedup ID 1 and leave no
history to observe. Two rounds is what makes the version chain appear.

The file name carries the 14-digit stamp __EXPORT_DATE is read from. It has nothing
to do with UPDATE_DATE, which is the source's own event time and the column SCD2
actually builds validity windows from.
"""

import datetime
import os
import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import EXPORTS, HEADER, INBOUND, SOURCE_DIRECTORY  # noqa: E402

STAMP = "%Y%m%d%H%M%S"

round_number = sys.argv[2]
rows = EXPORTS[round_number]

directory = f"{INBOUND}/{SOURCE_DIRECTORY}"
os.makedirs(directory, exist_ok=True)

name = datetime.datetime.now().strftime(STAMP)
path = f"{directory}/{name}.csv"
with open(path, "w", encoding="utf-8") as handle:
    handle.write("\n".join([HEADER, *rows]) + "\n")

print(f"wrote round {round_number} to {path}")
