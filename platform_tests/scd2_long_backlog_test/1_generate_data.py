"""Step 1 — land all three committed exports in inbound before the pipeline runs.

The fixtures are committed CSVs under sample_data/, copied here rather than built —
see _shared.py. All three land in one step, up front: the case this test covers
only shows up when Auto Loader's availableNow trigger picks up more than one snapshot
file in the same micro-batch, which happens whenever a backlog accumulates before a
run, since neither reader sets maxFilesPerTrigger. Landing the exports one run at a
time (as scd2_csv_test does) would avoid the very condition being tested.
"""

import os
import shutil
import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import EXPORTS, INBOUND, SOURCE_DIRECTORY  # noqa: E402

samples = f"{sys.argv[1]}/sample_data"
directory = f"{INBOUND}/{SOURCE_DIRECTORY}"
os.makedirs(directory, exist_ok=True)

for export in EXPORTS:
    shutil.copyfile(f"{samples}/{export}", f"{directory}/{export}")
    print(f"landed {export} in {directory}")
