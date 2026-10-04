# CLAUDE.md

Remember to keep your answers short and concise. An accurate summary is more meaningful than a lengthy and verbose explanation. 

## Project Overview

This is a configuration-driven Data Processing Framework for ingesting data into Databricks.
The framework supports a multi-layer medallion architecture (Source → Inbound → Bronze → Silver → Gold) and 
is deployed via Databricks Declarative Automation Bundles (Previously Databricks Asset Bundles).

**Tech Stack**: Python 3.11, Databricks Runtime 15.4 LTS (PySpark 3.5), Delta Lake, DQX, Pandas

## Running anything

Targets native Linux. Run `poetry install` once, then every tool through Poetry;
`databricks` and `gh` run directly:

```bash
poetry run pytest tests/test_history_writers.py   # while iterating: only the files you touch
make test                                          # before handing over: full suite, ~7 min
poetry run python <script>                         # there is no bare `python` on PATH
```

Python is 3.11, matching Databricks Runtime 15.4 LTS. Needs a JDK (PySpark) and Poetry with a
3.11 interpreter; see [README.md](README.md#development).

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
- Every catalog is `{catalog}_{env}`: `catalog: mdm` with `env: dev` resolves to `mdm_dev`.
  The audit table is `monitoring_{env}.audit.logs`.
- **Classic job clusters only.** Serverless is not supported yet. It rejects or breaks three
  things the framework does: SCD2 caches a DataFrame, UPSERT sets an autoMerge Spark conf,
  and `foreachBatch` captures the `Context`.

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
databricks bundle deploy -t dev -p <profile>
databricks bundle run integration_test_suite -t dev -p <profile>
```

A platform test must clean only its own directories and tables. The suite runs its jobs in
parallel, so a wholesale cleanup of the shared inbound or metadata roots would destroy a
neighbour's checkpoints mid-run.

A new workspace needs its schemas and volume created once, and a failed run is traced
through the CLI; both are in [05_testing.md](docs/05_testing.md#workspace-prerequisites).

## Skills

- `flame-data-engineer`: before writing or reviewing any code, config, workflow YAML or docs.
- `flame-pm`: choosing, evaluating, writing and labelling GitHub issues.
- `flame-data-setup`: onboarding a dataset, from raw files to Silver.
- `pharma-regulations`: GxP and ALCOA+ guidance for regulated data.

## Planning

All work is tracked in GitHub issues; there is no TODO file. See the `flame-pm` skill.

## Editing files from Windows

Bash heredocs mangle `\n` escapes in this environment. When a patch contains them, write the
patch script to the scratchpad with the Write tool and run it by path, rather than piping a
heredoc into `python -`.
