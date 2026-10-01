# dbx-flame

A configuration-driven data engineering framework for Databricks. Onboarding a new dataset
means writing a workflow YAML — never Python.

[![Databricks Runtime][DBR]][DBR-url]
[![python][Python]][Python-url]
[![Apache Spark][Spark]][Spark-url]
[![DQX][DQX]][DQX-url]
[![Poetry][Poetry]][Poetry-url]
[![Code style: black][Black]][Black-url]
[![CI][CI]][CI-url]
[![Release][Release]][Release-url]
![Status][Status]
[![License][License]][License-url]

## Why

Most Databricks pipelines grow one notebook per dataset. This framework inverts that: a
single typed engine reads a workflow YAML and carries every dataset through the same five
layers — Start, Pipeline, Typing, Policies, Output. A new source is a new YAML file, not a
new code path.

The project is **alpha** — see [Status](#status) for what's implemented today.

## Key Capabilities

- **Config-driven onboarding** — declare an origin, a write verb and a target; the framework
  validates the combination and runs it. No per-dataset Python.
- **Five typed layers** — Start → Pipeline → Typing → Policies → Output, each reachable only
  through a typed `Context` and a DataFrame, so every layer is independently testable.
- **Five write verbs** — `APPEND`, `FULL`, `UPSERT`, `SCD2`, `COMPLETE_DELTA` — layer-agnostic,
  so the same verb serves Inbound→Bronze or Bronze→Silver.
- **Gold is SQL** — each Gold table is a materialized view over Silver, in its own `.sql` file.
  Silver's MERGEs never block it. See [Gold](docs/00_overview.md#gold).
- **A data quality gate, not a bolt-on** — every batch is checked against a
  [Databricks DQX](https://databrickslabs.github.io/dqx/) ruleset before it's written;
  `error` refuses the batch, `warn` logs and lets it through.
- **Fail fast, fail loud** — invalid config, unknown origins, and failed checks stop the run
  with one aggregated error report. Nothing logs an error and reports success.
- **Ships as a wheel** — src-layout package deployed via Databricks Asset Bundles and run
  through `python_wheel_task` entry points. No notebook logic, no `sys.path` hacks.

## Quick Look

Two tasks, two YAML blocks — CSV into Bronze, then Bronze into Silver with SCD Type 1
upsert on `CLAIM_ID`:

```yaml
# Inbound → Bronze
source.origin: csv
source.path: /Volumes/dev/source_data/inbound/
source.directory: CLAIMS
output.verb: append
output.schema_name: claims
output.table: CLAIMS_BRONZE

# Bronze → Silver
source.origin: delta
source.schema_name: claims
source.table: CLAIMS_BRONZE
output.verb: upsert
output.schema_name: claims
output.table: CLAIMS_SILVER
output.keys: CLAIM_ID
output.event_time.column: __EXPORT_DATE
```

No code changes for either step — both are entries in a workflow YAML deployed through the
bundle. See [03_write_verbs.md](docs/03_write_verbs.md) for the full verb matrix.

## Documentation

1. [00_overview.md](docs/00_overview.md) — goals, principles, glossary, layer diagram
2. [01_architecture.md](docs/01_architecture.md) — layers, protocols, execution flow, extension points
3. [02_config_schema.md](docs/02_config_schema.md) — the typed config schema, parameter by parameter
4. [03_write_verbs.md](docs/03_write_verbs.md) — verb semantics with worked examples
5. [04_policies.md](docs/04_policies.md) — the data quality gate, driven by a DQX ruleset
6. [05_testing.md](docs/05_testing.md) — unit and platform testing
7. [06_roadmap.md](docs/06_roadmap.md) — phased implementation plan and current status

## Development

Targets Linux (native or WSL). Dependencies and the virtualenv are managed with
[Poetry](https://python-poetry.org/) (2.x).

**Prerequisites**: a JDK (11 or 17 — required by PySpark) and Python **3.11** exactly (matches
Databricks Runtime 15.4 LTS). If your system Python isn't 3.11, install one (e.g. via
[pyenv](https://github.com/pyenv/pyenv) or a standalone build) and point Poetry at it.

```bash
poetry env use python3.11   # once, to pin the interpreter (path to a 3.11 binary if not on PATH)
make install                # runtime + dev dependencies
make test                   # unit tests with coverage
```

Poetry keeps the virtualenv outside the project (`~/.cache/pypoetry/virtualenvs`), so there is
no `.venv/` directory to get out of sync with the interpreter actually running the tests.

### Make targets

| Target | What it does |
|---|---|
| `make install` | `poetry install` — create the venv and install everything |
| `make test` | `poetry run pytest` — unit tests with coverage (70% gate) |
| `make qa` | `black .`, then `flake8`, then `yamllint .` |
| `make format` | import sort (`ruff --select I --fix`) + `black .` |
| `make build` | `poetry build` — wheel + sdist into `dist/` |
| `make clean` | remove `dist/`, caches, `__pycache__` |

`make` with no target prints this list.

Note that `make qa` **rewrites files** — `black .` formats in place rather than checking.
Use `poetry run black --check .` for a read-only pass.

### Tool configuration

All tool config lives in `pyproject.toml` except flake8, which cannot read it — flake8's
settings are in `.flake8`.

`mypy` and `ruff` are installed and configured but are not part of `make qa` yet.

### Testing

`make test` runs unit tests against a local Spark + Delta session. Platform tests are real
Databricks jobs under [platform_tests/](platform_tests/) — each one generates its own
fixtures and asserts the resulting tables:

```bash
databricks bundle deploy -t dev_01 -p <profile>
databricks bundle run integration_test_suite -t dev_01 -p <profile>
```

See [05_testing.md](docs/05_testing.md) for the full strategy.

### On Windows (WSL)

Spark doesn't run natively on Windows, so do all of the above inside WSL (e.g. Ubuntu 24.04),
not PowerShell/cmd — the Databricks CLI profile lives there too:

```bash
wsl -d Ubuntu-24.04 -- bash -lc 'cd /path/to/dbx-flame && poetry run pytest'
```

## Deploying to Databricks

The bundle in [databricks.yml](databricks.yml) is configured to work with a Python wheel: it
builds the wheel, then deploys the bundle. Authenticate with a CLI profile or with
`DATABRICKS_HOST` / `DATABRICKS_TOKEN`:

```bash
databricks bundle deploy
```

## Traceability

Every run writes to an audit log — not vendor telemetry, a trail in your own Databricks
catalog (`monitoring_{env}.audit.logs`). It's not a bolt-on: `Context` carries the logger as a
required field, and the audit write is deliberately the first thing a task does — if it can't
land, the run fails before touching any data, rather than processing a batch it can't account
for.

Each row ties a log level and an event to the exact job, run and task that produced it:

| | Columns |
|---|---|
| What ran | `name`, `source` |
| When | `__workflow_id` / `__workflow_run_id`, `__task_key` / `__task_run_id`, `time_stamp` |
| Where | `catalog`, `schema`, `table` |
| Outcome | `type` (INFO / WARNING / ERROR), `total`, `description`, `metadata` |

Nothing leaves the workspace, and there's no flag to turn it off. Full contract:
[00_overview.md](docs/00_overview.md#glossary).

## Status

Start, Pipeline (CSV + JSON + Delta), Typing, Policies (DQX), and all five write verbs are
implemented and tested. The SAS origin and the Gold example view are not implemented yet.
Full phase-by-phase status:
[06_roadmap.md](docs/06_roadmap.md).

## License

Apache 2.0 — see [LICENSE](LICENSE).

<!-- MARKDOWN LINKS & IMAGES -->

[DBR]: https://img.shields.io/badge/Databricks%20Runtime-15.4--LTS-%231B3139
[DBR-url]: https://docs.databricks.com/en/release-notes/runtime/15.4lts.html

[Python]: https://img.shields.io/badge/python-3.11-g
[Python-url]: https://www.python.org/

[Spark]: https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark&logoColor=white
[Spark-url]: https://spark.apache.org/

[DQX]: https://img.shields.io/badge/DQX-0.16-FF3621
[DQX-url]: https://databrickslabs.github.io/dqx/

[Poetry]: https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json
[Poetry-url]: https://python-poetry.org/

[Black]: https://img.shields.io/badge/code%20style-black-000000.svg
[Black-url]: https://github.com/psf/black

[CI]: https://github.com/Escoto/dbx-flame/actions/workflows/on_push.yml/badge.svg
[CI-url]: https://github.com/Escoto/dbx-flame/actions/workflows/on_push.yml

[Release]: https://img.shields.io/github/v/tag/Escoto/dbx-flame?label=release
[Release-url]: https://github.com/Escoto/dbx-flame/tags

[Status]: https://img.shields.io/badge/status-alpha-orange

[License]: https://img.shields.io/badge/license-Apache%202.0-blue
[License-url]: LICENSE
