"""Step 3 — a subset named in mixed case, stamped with the date in Europe/Berlin.

The run clears the schema's folder first, so the whole export's files must be gone. It
keeps framework columns, renames SITE_ID in SITE and leaves EMAIL out of CONTACT; the
setting for SITE_DE, which isn't exported, is skipped.
"""

import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    CONTACT_COLUMNS,
    SITE_COLUMNS,
    data_files,
    expect_contact,
    expect_manifest,
    expect_site,
    exported_files,
)

files = exported_files()

# The export ran moments ago, possibly just before midnight in Berlin.
today = datetime.now(ZoneInfo("Europe/Berlin")).date()
candidates = [
    [f"CONTACT_{day:%Y-%m-%d}.csv", f"SITE_{day:%Y-%m-%d}.csv"]
    for day in (today, today - timedelta(days=1))
]
data = data_files(files)
assert data in candidates, f"expected one of {candidates}, found {files}"

contact, site = data
expect_site(site, header=["ID", *SITE_COLUMNS[1:], "__SOURCE"])
expect_contact(contact, header=CONTACT_COLUMNS[:2])
expect_manifest(
    files,
    "md5",
    {"CONTACT": ("MANAGED", True, 2, contact), "SITE": ("MANAGED", True, 3, site)},
)

print("subset export validated")
