# CLAUDE.md

Keep answers short and concise: an accurate summary beats a lengthy explanation.

## Project Overview

A configuration-driven framework for ingesting data into Databricks through a medallion
architecture (Source → Inbound → Bronze → Silver → Gold), deployed via Databricks
Declarative Automation Bundles (previously Asset Bundles).

**Tech Stack**: Python 3.11, Databricks Runtime 15.4 LTS (PySpark 3.5), Delta Lake, DQX, Pandas

## Running anything

Targets native Linux. Run `poetry install` once (needs a JDK and a 3.11 interpreter; see
[README.md](README.md#development)), then every tool through Poetry; `databricks` and `gh`
run directly:

```bash
poetry run pytest tests/test_history_writers.py   # while iterating: only the files you touch
make test                                          # before handing over: full suite, ~7 min
poetry run python <script>                         # there is no bare `python` on PATH
```

Format with black (`make qa`, line length 99), never `ruff format`: it rewrites files
black leaves alone.

## Conventions

- **All Delta table names are UPPERCASE.** This is validated, not just convention.
- Keep implementations as simple as the problem allows.
- Comments explain *why*, not *what*. A comment that restates the code earns its deletion.
- Greenfield: no predecessor to stay compatible with, so "parity" is never a reason.
- Every catalog is `{catalog}_{env}`: `catalog: mdm` with `env: dev` resolves to `mdm_dev`.
  The audit table is `monitoring_{env}.audit.logs`.
- **Provenance is written once.** `__SOURCE` and `__EXPORT_DATE` are set where data enters
  the framework and never overwritten on promotion, so every Silver and Gold row traces to
  its exact origin (ALCOA+). `__EXPORT_DATE` is the only source of truth for time.
- **Classic job clusters only.** Serverless rejects or breaks three things the framework
  does: SCD2 caches a DataFrame, UPSERT sets an autoMerge Spark conf, and `foreachBatch`
  captures the `Context`.

## Architecture

Five layers, each reaching the next only through a typed `Context` and a DataFrame:
Start (`context/`) → Pipeline (`pipelines/`) → Typing (`typecast/`) → Policies
(`policies/`) → Output (`output/`). Cross-cutting: `observability/`, `entrypoints/`.

A layer never reads raw task parameters, only the validated `Context`. Write verbs declare
their `Requirements` and readers their increment strategies; Start validates each
origin × verb × strategy combination before anything runs.

Full detail in [docs/](docs/00_overview.md); start with `00_overview.md`.

## Testing

`make test`: unit tests, local Spark + Delta, 70% coverage gate.

Platform tests are real Databricks jobs under `platform_tests/`, with their workflow YAML in
`workflows/platform_tests/`. Each generates its own fixtures and asserts the resulting tables:

```bash
databricks bundle deploy -t dev -p <profile>
databricks bundle run integration_test_suite -t dev -p <profile>
```

A platform test cleans only its own directories and tables: the suite runs its jobs in
parallel, so a wholesale cleanup of the shared inbound or metadata roots would destroy a
neighbour's checkpoints mid-run. Workspace prerequisites and tracing a failed run:
[05_testing.md](docs/05_testing.md#workspace-prerequisites).

## Skills

- `flame-data-engineer`: before writing or reviewing any code, config, workflow YAML or docs.
- `flame-pm`: choosing, evaluating, writing and labelling GitHub issues.
- `flame-data-setup`: onboarding a dataset, from raw files or a table we didn't create to Silver.
- `pharma-regulations`: GxP and ALCOA+ guidance for regulated data.

## Planning

All work is tracked in GitHub issues; there is no TODO file. See the `flame-pm` skill.

## Running from Windows

When this session runs on Windows, run everything through WSL (`databricks` reads its CLI
profile there too):

```bash
wsl -d Ubuntu-24.04 -- bash -lc 'cd /mnt/c/repos/escoto/dbx-flame && poetry run pytest'
```

Anything beyond a plain command gets mangled on the way in: heredocs lose `\n` escapes,
`$VAR` and `$(...)` are dropped or expanded too early, and Git Bash rewrites `/mnt/...`
paths. Write the script to the scratchpad with the Write tool and run it by path:

```bash
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash -lc "tr -d '\r' < /mnt/c/<path>/script.sh | bash"
```
