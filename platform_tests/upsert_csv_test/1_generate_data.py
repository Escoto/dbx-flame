"""Step 1 — drop two CSV exports into the inbound directory.

File names carry the 14-digit stamp the framework reads __EXPORT_DATE from, two
seconds apart so the second export is unambiguously the newer one. ID 1.0 appears
in both, which is what makes the dedup and newer-wins rules observable downstream.
"""

import datetime
import os
import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import INBOUND, SOURCE_DIRECTORY  # noqa: E402

STAMP = "%Y%m%d%H%M%S"
HEADER = "ID,NAME,SOURCE_SYSTEM,CREATED_DATE"

FIRST_EXPORT = [
    "1.0,Alice,Source_A,a",
    "2.0,Bob,Source_A,b",
    "3.0,Charlie,Source_A,c",
]
SECOND_EXPORT = [
    "1.0,Alice,Source_B,a",
    "4.0,David,Source_B,b",
    "5.0,Elise,Source_B,c",
]

directory = f"{INBOUND}/{SOURCE_DIRECTORY}"
os.makedirs(directory, exist_ok=True)

now = datetime.datetime.now()
for offset, rows in ((0, FIRST_EXPORT), (2, SECOND_EXPORT)):
    name = (now + datetime.timedelta(seconds=offset)).strftime(STAMP)
    path = f"{directory}/{name}.csv"
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("\n".join([HEADER, *rows]) + "\n")
    print(f"wrote {path}")
