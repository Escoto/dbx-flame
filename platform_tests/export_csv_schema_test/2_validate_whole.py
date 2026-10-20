"""Step 2 — the whole schema, tables and view, exported as one unstamped CSV each."""

import sys

# Databricks exec()s a workspace file, so __file__ is never defined here;
# the workflow passes this script's directory as the first parameter.
sys.path.append(sys.argv[1])

from _shared import (  # noqa: E402
    expect_contact,
    expect_site,
    expect_site_view,
    exported_files,
)

files = exported_files()
# Uppercase although Unity Catalog lists them in lowercase; no staging folder left behind.
assert files == ["CONTACT.csv", "SITE.csv", "SITE_DE.csv"], f"unexpected files: {files}"

expect_site("SITE.csv")
expect_contact("CONTACT.csv")
expect_site_view("SITE_DE.csv")

print("whole-schema export validated")
