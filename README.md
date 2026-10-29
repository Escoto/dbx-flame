# dbx-flame

A configuration-driven framework that loads governed, traceable data into Databricks. You
onboard a dataset with a workflow YAML, not with Python.


[![PyPI][PyPI]][PyPI-url]
![Status][Status]
[![CI][CI]][CI-url]
[![python][Python]][Python-url]
[![Databricks Runtime][DBR]][DBR-url]
[![DQX][DQX]][DQX-url]
[![License][License]][License-url]

## Why

dbx-flame builds the data foundation that the "data intelligence" sits on: governed,
traceable Bronze and Silver tables in Unity Catalog. Genie, agents and AI functions only work
as well as the tables and metadata under them.

Most Databricks pipelines grow one notebook per dataset. dbx-flame uses one typed engine
instead. It reads a workflow YAML and moves each dataset through the same five layers: Start,
Pipeline, Typing, Policies, Output.

The project is **beta**. See [Status](#status).

## Quick Look

Two tasks: CSV files into Bronze, then Bronze into Silver with an SCD Type 1 upsert on
`CLAIM_ID`.

```yaml
# Inbound → Bronze
source.origin: csv
source.path: /Volumes/dev/source_data/inbound/
source.directory: CLAIMS
output.verb: append
output.schema_name: claims
output.table: CLAIMS_BRONZE

# Bronze → Silver
source.origin: table
source.schema_name: claims
source.table: CLAIMS_BRONZE
output.verb: upsert
output.schema_name: claims
output.table: CLAIMS_SILVER
output.keys: CLAIM_ID
output.event_time.column: __EXPORT_DATE
```

Both tasks are entries in a workflow YAML that the bundle deploys. All verbs:
[03_write_verbs.md](docs/03_write_verbs.md).

## What Every Table Gets

- **Checked configuration.** Start checks each task before it reads data. An invalid setting,
  or one with no effect, stops the run.
- **Quality gates.** A [Databricks DQX](https://databrickslabs.github.io/dqx/) ruleset checks
  each batch before the framework writes it. An `error` rule stops the batch. A `warn` rule
  logs the failure and lets the batch continue. A run never logs an error and reports success.
- **Row-level provenance.** The framework sets `__SOURCE` and `__EXPORT_DATE` when data
  enters, and never changes them. Each Silver row traces back to the export it came from.
- **An audit trail in your catalog.** Each run that passes Start writes to
  `monitoring_{env}.audit.flame_logs`. Each row names its job, run and task. A run that
  cannot log stops before it touches data. Schema: [00_overview.md](docs/00_overview.md#glossary).
  Each audit event also prints to the driver's stdout. If the cluster sends stdout to
  CloudWatch (for example, with the CloudWatch agent in an init script), the events also
  appear there.
- **One set of write verbs.** `APPEND`, `FULL`, `UPSERT`, `SCD2` and `COMPLETE_DELTA` work the
  same for `Inbound → Bronze` and `Bronze → Silver`.

## Recommended Usage

The source, increment strategy (`source.increment_strategy`) and verb for each step:

| Step | Source | Strategy | Verb | Why |
|---|---|---|---|---|
| Inbound → Bronze | Files (csv, json) | `checkpoint` | APPEND | Bronze keeps each export exactly as it arrived |
| External table → Bronze | A table we didn't create (e.g. a federated one) | `full_read` | APPEND | One complete snapshot per run. Sees deletes |
| | | `delta_read` | APPEND | Only the rows changed since the last run, for a table too large to read whole. Misses deletes |
| Bronze → Silver | A table we stamped | `checkpoint` | FULL | The newest export is the current state |
| | | `checkpoint` | UPSERT | Latest version per key (SCD1) |
| | | `checkpoint` · `watermark` | SCD2 | History of changes |
| | | `watermark` | COMPLETE_DELTA, `snapshot_scope: delta` | Replays each export in order. A deletes feed retires keys |

Comprehensive list of combinations:
* [08_usage.md](docs/08_usage.md).
* Combinations cheat sheet [examples](docs/examples/README.md)

## Documentation

Start with the [quick start](docs/README.md#quick-start): the catalogs, volumes and naming
rules that the framework needs. [docs/README.md](docs/README.md) lists every document.

## Development

The project targets Linux. On Windows, use WSL, because Spark does not run natively there. You
need a JDK (11 or 17, for PySpark), Python **3.11** (the version
in Databricks Runtime 15.4 LTS) and [Poetry](https://python-poetry.org/) 2.x.

```bash
poetry env use python3.11   # once, to pin the interpreter
make install                # runtime + dev dependencies
make test                   # unit tests with coverage
```

The [Makefile](Makefile) lists the other targets. Platform tests are real Databricks jobs
that run real-life scenarios: [platform_tests/](platform_tests/) and
[05_testing.md](docs/05_testing.md).

## Deploying to Databricks

The bundle in [databricks.yml](databricks.yml) builds the Python wheel, then deploys it.
Authenticate with a CLI profile or with `DATABRICKS_HOST` / `DATABRICKS_TOKEN`. A new
workspace first needs the setup in the [quick start](docs/README.md#quick-start).

```bash
databricks bundle deploy
```

## Status

Implemented and tested: Start, Pipeline (CSV, JSON and table), Typing, Policies (DQX) and all
five write verbs. The export task writes a schema's objects to CSV files, with a checksum per
file and a manifest ([09_export.md](docs/09_export.md)). Not implemented yet: the SAS origin
and the Gold example view. Status per phase: [06_roadmap.md](docs/06_roadmap.md).

## License

Apache 2.0. See [LICENSE](LICENSE).

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

[Status]: https://img.shields.io/badge/status-beta-blue

[License]: https://img.shields.io/badge/license-Apache%202.0-blue
[License-url]: LICENSE
