# Library-based examples

Each folder is one scenario that uses dbx-flame as a library from PyPI.
Each folder has a job you can copy into your bundle, a diagram, and a README with the steps to reproduce.

- [daily_snapshot](daily_snapshot/README.md): a daily full CSV export goes to Bronze, which keeps all exports, and to Silver, which keeps the latest export.
- [full_and_delta](full_and_delta/README.md): JSON full exports and daily delta exports go to one Bronze table for each type, and to one Silver table that keeps the history of each record.
