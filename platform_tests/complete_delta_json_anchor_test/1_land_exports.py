"""Step 1 — land one round of the committed exports in their inbound directories.

The second parameter names the round: "steady" lands the pipeline working as usual,
"backlog" lands everything the upstream system sent after it.
"""

import os
import shutil
import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import INBOUND, ROUNDS  # noqa: E402

samples = f"{sys.argv[1]}/sample_data"

for export, directory in ROUNDS[sys.argv[2]]:
    target = f"{INBOUND}/{directory}"
    os.makedirs(target, exist_ok=True)

    # The file name is carried across unchanged: __EXPORT_DATE and the snapshot split
    # are both cut from the 14-digit stamp in it.
    shutil.copyfile(f"{samples}/{export}", f"{target}/{export}")
    print(f"landed {export} in {target}")
