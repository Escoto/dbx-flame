# CLAUDE.md

Remember to keep your answers short and concise. An accurate summary is more meaningful than a lengthy and verbose explanation. 

## Project Overview

This is a configuration-driven Data Processing Framework for ingesting data into Databricks.
The framework supports a multi-layer medallion architecture (Source → Inbound → Bronze → Silver → Gold) and 
is deployed via Databricks Declarative Automation Bundles (Previously Databricks Asset Bundles).

**Tech Stack**: Python 3.11, PySpark 15.4.x-scala2.12, Databricks, Delta Lake, DQX, Pandas

## Running anything

Targets native Linux. Run pytest, black, flake8, mypy, ruff and `databricks` directly:

```bash
poetry run pytest
```

Python is 3.11 — matching Databricks Runtime 15.4 LTS. Invoke it as `python`, never
`python3`. Needs a JDK (PySpark) and Poetry with a 3.11 interpreter; see [README.md](README.md#development).

**If this Claude session is running on Windows**, run everything (including `databricks`
commands, which read the CLI profile from there) through WSL instead:

```bash
wsl -d Ubuntu-24.04 -- bash -lc 'cd /mnt/c/repos/escoto/dbx-flame && poetry run pytest'
```

## Conventions

- **All Delta table names are UPPERCASE.** This is validated, not just convention.
- Keep implementations as simple as the problem allows.
- Comments explain *why*, not *what*. A comment that restates the code earns its deletion.
- This is a greenfield project. It has no predecessor to stay compatible with, so
  "parity" is never a reason to do (or not to do) something.

## Architecture

Five layers, each reaching the next only through a typed `Context` and a DataFrame:
Start (`context/`) → Pipeline (`pipelines/`) → Typing (`typecast/`) → Policies
(`policies/`) → Output (`output/`). Cross-cutting: `observability/`, `entrypoints/`.

A layer never reads raw task parameters — only the validated `Context`. Write verbs declare
their own `Requirements`, and the Start layer validates each origin × verb combination
before anything runs.

Full detail in [docs/](docs/00_overview.md); start with `00_overview.md`.

## Testing

```bash
make test    # unit tests, local Spark + Delta, 70% coverage gate
```

Platform tests are real Databricks jobs under `platform_tests/`, with their workflow YAML in
`workflows/platform_tests/`. Each generates its own fixtures and asserts the resulting
tables. Deploy and run them with:

```bash
databricks bundle deploy -t dev_01 -p <profile>
databricks bundle run integration_test_suite -t dev_01 -p <profile>
```

A platform test must clean only its own directories and tables. The suite runs its jobs in
parallel, so a wholesale cleanup of the shared inbound or metadata roots would destroy a
neighbour's checkpoints mid-run.

## Editing files from Windows

Bash heredocs mangle `\n` escapes in this environment. When a patch contains them, write the
patch script to the scratchpad with the Write tool and run it by path, rather than piping a
heredoc into `python -`.
