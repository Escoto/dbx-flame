# dbx-flame

A configuration-driven data engineering framework for Databricks. Onboarding a new dataset
means writing a workflow YAML — never Python.


[![PyPI][PyPI]][PyPI-url]
![Status][Status]
[![CI][CI]][CI-url]
[![python][Python]][Python-url]
[![Databricks Runtime][DBR]][DBR-url]
[![DQX][DQX]][DQX-url]
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
- **Gold is SQL (planned)** — each Gold table will be a materialized view over Silver, in its
  own `.sql` file, so Silver's MERGEs never block it. See [Gold](docs/00_overview.md#gold).
- **A data quality gate, not a bolt-on** — every batch is checked against a
  [Databricks DQX](https://databrickslabs.github.io/dqx/) ruleset before it's written;
  `error` refuses the batch, `warn` logs and lets it through.
- **Fail fast, fail loud** — invalid config and failed checks stop the run with one aggregated
  error report. Nothing logs an error and reports success.
- **Ships as a wheel** — src-layout package deployed via Databricks Declarative Automation
  Bundles and run through `python_wheel_task` entry points. No notebook logic, no `sys.path` hacks.

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

Targets Linux; on Windows, work inside WSL, since Spark doesn't run natively there. Needs a JDK
(11 or 17, for PySpark), Python **3.11** (matches Databricks Runtime 15.4 LTS) and
[Poetry](https://python-poetry.org/) 2.x.

```bash
poetry env use python3.11   # once, to pin the interpreter
make install                # runtime + dev dependencies
make test                   # unit tests with coverage
```

`make` with no target lists the rest. Platform tests are real Databricks jobs under
[platform_tests/](platform_tests/); see [05_testing.md](docs/05_testing.md).

## Deploying to Databricks

The bundle in [databricks.yml](databricks.yml) is configured to work with a Python wheel: it
builds the wheel, then deploys the bundle. Authenticate with a CLI profile or with
`DATABRICKS_HOST` / `DATABRICKS_TOKEN`:

```bash
databricks bundle deploy
```

## Traceability

Every run that passes config validation writes an audit trail to your own catalog
(`monitoring_{env}.audit.logs`), not to vendor telemetry. Each row names the job, run and task
that produced it. The audit write comes first, so a run that can't log fails before touching
any data. Schema: [00_overview.md](docs/00_overview.md#glossary).

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

[DQX]: https://img.shields.io/badge/DQX-0.16-FF3621
[DQX-url]: https://databrickslabs.github.io/dqx/

[CI]: https://img.shields.io/github/actions/workflow/status/Escoto/dbx-flame/on_push.yml?branch=main&label=CI
[CI-url]: https://github.com/Escoto/dbx-flame/actions/workflows/on_push.yml

[PyPI]: https://img.shields.io/pypi/v/dbx-flame?color=blue
[PyPI-url]: https://pypi.org/project/dbx-flame/

[Status]: https://img.shields.io/badge/status-alpha-blue

[License]: https://img.shields.io/badge/license-Apache%202.0-blue
[License-url]: LICENSE
