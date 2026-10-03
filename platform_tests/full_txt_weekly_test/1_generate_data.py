"""Step 1 — land what the provider sent since the previous scheduled run.

The round number, passed as the fourth parameter, picks the week. Round 3 lands two
files: Wednesday's accidental push and Monday 3's export, since the pipeline never
ran in between.
"""

import os
import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import HEADER, INBOUND, LANDED, SOURCE_DIRECTORY  # noqa: E402

round_number = sys.argv[4]

directory = f"{INBOUND}/{SOURCE_DIRECTORY}"
os.makedirs(directory, exist_ok=True)

for export in LANDED[round_number]:
    lines = ["|".join(HEADER), *("|".join(row) for row in export["rows"])]
    path = f"{directory}/{export['file']}"
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")
    print(f"round {round_number}: landed {path}")
