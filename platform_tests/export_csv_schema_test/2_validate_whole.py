"""Step 2 — the whole schema, tables and view, exported as one unstamped CSV each and
delivered as loose files to the round's destination."""

import os
import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    DESTINATION,
    data_files,
    expect_built,
    expect_contact,
    expect_manifest,
    expect_site,
    expect_site_view,
    exported_files,
)

files = exported_files()
# Uppercase although Unity Catalog lists them in lowercase.
data = data_files(files)
assert data == ["CONTACT.csv", "SITE.csv", "SITE_DE.csv"], f"unexpected files: {files}"
# A checksum beside each, plus the manifest and its own: no staging folder left behind.
assert len(files) == 8, f"unexpected files: {files}"

expect_site("SITE.csv")
expect_contact("CONTACT.csv")
expect_site_view("SITE_DE.csv")
expect_manifest(
    files,
    "sha256",
    {
        "CONTACT": ("MANAGED", True, 2, "CONTACT.csv"),
        "SITE": ("MANAGED", True, 3, "SITE.csv"),
        "SITE_DE": ("VIEW", False, 1, "SITE_DE.csv"),
    },
)

# package: files delivers every file built, the manifest and its checksum included.
delivered = sorted(os.listdir(DESTINATION))
assert delivered == files, f"delivered {delivered}, built {files}"
for name in delivered:
    expect_built(os.path.join(DESTINATION, name), name)

print("whole-schema export validated")
