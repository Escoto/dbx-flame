"""Step 1 — drop this round's CSV exports into the inbound directory.

Round 1 sends two exports, round 2 a third. File names carry the 14-digit stamp the
framework reads __EXPORT_DATE from, two seconds apart so each is unambiguously newer.
"""

import datetime
import os
import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import INBOUND, SOURCE_DIRECTORY  # noqa: E402

STAMP = "%Y%m%d%H%M%S"
HEADER = "ID,NAME,SOURCE_SYSTEM"

EXPORTS = {
    "1": [
        ["1,Alice,Source_A", "2,Bob,Source_A", "3,Charlie,Source_A"],
        ["4,David,Source_A", "5,Elise,Source_A", "6,Farah,Source_A"],
    ],
    "2": [
        ["7,Gus,Source_B", "8,Hana,Source_B"],
    ],
}

directory = f"{INBOUND}/{SOURCE_DIRECTORY}"
os.makedirs(directory, exist_ok=True)

now = datetime.datetime.now()
for offset, rows in enumerate(EXPORTS[sys.argv[5]]):
    name = (now + datetime.timedelta(seconds=2 * offset)).strftime(STAMP)
    path = f"{directory}/{name}.csv"
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("\n".join([HEADER, *rows]) + "\n")
    print(f"wrote {path}")
