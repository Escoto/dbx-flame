"""Step 1 — land the two committed exports in their own inbound directories.

The fixtures are real MDM-shaped files under sample_data/ rather than rows built
here: the envelope is what this test exercises, so the exports have to keep their
nesting, their absent keys and the timezone suffix on the export stamp.

Which export goes where is the point — a full feed and a delta feed are tracked as
separate directories into separate bronze tables, and only merge in silver.
"""

import os
import shutil
import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    DELTA_DIRECTORY,
    DELTA_EXPORT,
    FULL_DIRECTORY,
    FULL_EXPORT,
    INBOUND,
)

samples = f"{sys.argv[1]}/sample_data"

for export, directory in ((FULL_EXPORT, FULL_DIRECTORY), (DELTA_EXPORT, DELTA_DIRECTORY)):
    target = f"{INBOUND}/{directory}"
    os.makedirs(target, exist_ok=True)

    # The file name is carried across unchanged: __EXPORT_DATE and the snapshot split
    # are both cut from the 14-digit stamp in it.
    shutil.copyfile(f"{samples}/{export}", f"{target}/{export}")
    print(f"landed {export} in {target}")
