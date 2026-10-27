"""Step 3 — a subset named in mixed case, stamped with the date in Europe/Berlin.

The run clears the export's folder first, so the whole export's files must be gone. It
keeps framework columns, renames SITE_ID in SITE and leaves EMAIL out of CONTACT; the
setting for SITE_DE, which isn't exported, is skipped. With no destination, it delivers
one stamped zip and its checksum to the default deliveries folder.
"""

import hashlib
import os
import sys
import zipfile
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    CONTACT_COLUMNS,
    DEFAULT_DELIVERY,
    SITE_COLUMNS,
    FOLDER,
    data_files,
    expect_audited,
    expect_built,
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

# The zip carries the run's stamp, as the data files do.
stem = f"PLATFORM_TEST_{contact[len('CONTACT_'):-len('.csv')]}"
delivered = sorted(os.listdir(DEFAULT_DELIVERY))
assert delivered == [f"{stem}.md5", f"{stem}.zip"], f"delivered {delivered}"
zip_path = os.path.join(DEFAULT_DELIVERY, f"{stem}.zip")
expect_built(zip_path, f"{stem}.zip")

# Every file built sits at the zip's root, the manifest and its checksum included.
with zipfile.ZipFile(zip_path) as package:
    zipped = sorted(package.namelist())
built = [name for name in files if not name.startswith(f"{stem}.")]
assert zipped == built, f"zip holds {zipped}, built {built}"

with open(zip_path, "rb") as handle:
    digest = hashlib.md5(handle.read()).hexdigest()
with open(os.path.join(DEFAULT_DELIVERY, f"{stem}.md5"), encoding="ascii") as handle:
    assert handle.read() == digest, "the zip's md5 file differs from its hash"

# The zip and every CSV in it are on record; a file inside the zip is located by the zip.
expect_audited(
    "export_subset",
    "md5",
    {
        f"{stem}.zip": (zip_path, zip_path),
        **{
            name: (zip_path, os.path.join(FOLDER, name))
            for name in zipped
            if name.endswith(".csv")
        },
    },
)

print("subset export validated")
