# CLAUDE.md

Keep answers short. An accurate summary beats a long explanation.

## Project

dbx-flame is a configuration-driven framework that ingests data into Databricks, from Inbound
to Bronze to Silver. Gold is SQL views over Silver, and a separate export task delivers files
to consumers. A Databricks Declarative Automation Bundle (previously Asset Bundle) deploys it
as a Python wheel. Python 3.11, Databricks Runtime 15.4 LTS. See the [README](README.md) and
[00_overview.md](docs/00_overview.md).

## Running anything

The project targets Linux. Run `poetry install` once (it needs a JDK and Python 3.11, see
[README.md](README.md#development)). Then run every tool through Poetry. Run `databricks`
and `gh` directly.

```bash
poetry run pytest tests/test_history_writers.py   # while iterating: only the files you touch
make test                                          # before handing over: full suite, ~7 min
poetry run python <script>                         # there is no bare `python` on PATH
```

Format with black (`make qa`, line length 99). Never use `ruff format`: it rewrites files
that black leaves unchanged.

### On Windows

Run everything through WSL. The `databricks` CLI reads its profile there too.

```bash
wsl -d Ubuntu-24.04 -- bash -lc 'cd /mnt/c/repos/escoto/dbx-flame && poetry run pytest'
```

A command that is more than a plain command breaks on its way into WSL:

- Heredocs lose their `\n` escapes.
- `$VAR` and `$(...)` are dropped or expanded too early.
- Git Bash rewrites `/mnt/...` paths.

Write the script to the scratchpad with the Write tool, then run it by path:

```bash
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash -lc "tr -d '\r' < /mnt/c/<path>/script.sh | bash"
```

## Conventions

- **Never commit or push.** The project runs in a regulated environment. The user creates
  branches, commits and deploys. You can monitor runs with the CLI.
- **All Delta table names are UPPERCASE.** Start validates this.
- **Every catalog is `{catalog}_{env}`.** `catalog: mdm` with `env: dev` resolves to
  `mdm_dev`. The audit table is `monitoring_{env}.audit.logs`.
- **Provenance is written once.** The framework sets `__SOURCE` and `__EXPORT_DATE` where
  data enters it, and never overwrites them on promotion. Every Silver and Gold row therefore
  traces to its exact origin (ALCOA+). `__EXPORT_DATE` is the only source of truth for time.
- **Classic job clusters only.** Serverless rejects or breaks three things the framework
  does: SCD2 caches a DataFrame, UPSERT sets an autoMerge Spark conf, and `foreachBatch`
  captures the `Context`.

## Architecture

Five layers. Each layer reaches the next only through a typed `Context` and a DataFrame:
Start (`context/`) → Pipeline (`pipelines/`) → Typing (`typecast/`) → Policies
(`policies/`) → Output (`output/`). Cross-cutting: `observability/`, `mechanics/` and
`entrypoints/`, which also holds the export task.

Start validates every origin × verb × increment strategy combination before it reads any
data. Detail: [00_overview.md](docs/00_overview.md) and
[01_architecture.md](docs/01_architecture.md).

## Platform tests

Platform tests are real Databricks jobs. Their scripts are in `platform_tests/` and their
workflow YAML is in `workflows/platform_tests/`. Each target sets its CLI profile in
[databricks.yml](databricks.yml).

```bash
databricks bundle deploy -t dev
databricks bundle run export_test_suite -t dev
```

| Suite | Run it for a change to |
|---|---|
| `data_processing_test_suite` | Ingest: Source → Bronze → Silver |
| `export_test_suite` | The export task |
| `integration_test_suite` | Shared code, and before a release. It runs both suites. |

Register each test in exactly one suite. Prerequisites, test rules and how to trace a
failed run: [05_testing.md](docs/05_testing.md#2-platform-tests-databricks).

## Skills

- `flame-data-engineer`: coding standards and working agreements. Load it before you write
  or review code, config, workflow YAML or docs.
- `flame-pm`: choose, evaluate, write and label GitHub issues. All work is tracked in
  issues. There is no TODO file.
- `flame-data-setup`: onboard a dataset, from raw files or a table we didn't create to
  Silver.
- `pharma-regulations`: GxP and ALCOA+ guidance for regulated data.
- `flame-feature-state`: a sample config for a feature in progress. It marks what works now
  and which issue adds the rest.
