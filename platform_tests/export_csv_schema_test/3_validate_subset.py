"""Step 3 — a subset named in mixed case, stamped with the date in Europe/Berlin.

The run clears the schema's folder first, so the whole export's files must be gone.
"""

import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import expect_contact, expect_site, exported_files  # noqa: E402

files = exported_files()

# The export ran moments ago, possibly just before midnight in Berlin.
today = datetime.now(ZoneInfo("Europe/Berlin")).date()
candidates = [
    [f"CONTACT_{day:%Y-%m-%d}.csv", f"SITE_{day:%Y-%m-%d}.csv"]
    for day in (today, today - timedelta(days=1))
]
assert files in candidates, f"expected one of {candidates}, found {files}"

contact, site = files
expect_site(site)
expect_contact(contact)

print("subset export validated")
