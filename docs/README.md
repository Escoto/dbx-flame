# dbx-flame documentation

## Quick start

What the framework itself needs to run. Your data, the compute and the permissions to reach
both are yours to provide; the step-by-step recipe, with grants, is
[07_workspace_setup.md](07_workspace_setup.md).

### Naming rules

These are required, not conventions: a name that breaks them isn't found, or is rejected at
Start before any data is read.

- **Every catalog is `{catalog}_{env}`.** A task passes `catalog` and `env` separately and
  never a full catalog name: `catalog: mdm` with `env: dev` reads and writes in `mdm_dev`.
- **`env` is the bundle target.** Workflows pass `env: ${bundle.target}`, so deploying with
  `-t prod` means `_prod` everywhere.
- **`monitoring` is the only fixed name.** Every environment logs to
  `monitoring_{env}.audit.logs`. Name your data catalogs however you like.
- **One catalog per task.** A task writes, and keeps its progress, in its own
  `{catalog}_{env}`. Only a table source may live in another catalog (`source.catalog`).
- **Table names are UPPERCASE**, validated at Start. Keep catalog, schema and volume names
  lowercase.

### Infrastructure

The framework creates tables only. It never creates a catalog, schema or volume.

| What | Where | Created by |
|---|---|---|
| Audit catalog and schema | `monitoring_{env}.audit`, once per environment | You |
| Audit table | `monitoring_{env}.audit.logs` | The framework, on the first run. Don't create it |
| Metadata volume | Any volume in each target catalog `{catalog}_{env}`, passed as `metadata_path`. Every ingest task needs one; the tasks of a catalog can share it | You. The framework keeps one folder per table under it: checkpoints and schema hints |
| Output schemas | One per `output.schema_name` | You. Its tables are created on first write |
| Export volume | A folder inside any volume of the task's catalog, passed as `export.preparation_path`. Export tasks only | You |

A missing `monitoring_{env}.audit` fails every run before it reads any data: the audit
write comes first. The metadata volume holds the framework's own state; losing it makes a
checkpointed source read its files again.

For `catalog: mdm` on target `prod`:

```sql
-- Audit (required name: monitoring_{env})
CREATE CATALOG IF NOT EXISTS monitoring_prod;
CREATE SCHEMA  IF NOT EXISTS monitoring_prod.audit;

-- Your catalog (required name: {catalog}_{env}), one schema per output.schema_name
CREATE CATALOG IF NOT EXISTS mdm_prod;
CREATE SCHEMA  IF NOT EXISTS mdm_prod.bronze;
CREATE SCHEMA  IF NOT EXISTS mdm_prod.silver;

-- Raw files (recommended layout; any volume works)
-- source.path: /Volumes/mdm_prod/raw/files/
-- source.directory: site
-- reads as: /Volumes/mdm_prod/raw/files/site/site_20261025141700.json
CREATE SCHEMA  IF NOT EXISTS mdm_prod.raw;
CREATE VOLUME  IF NOT EXISTS mdm_prod.raw.files;

-- Framework state (recommended layout; any volume in your destination catalog works)
-- metadata_path: /Volumes/mdm_prod/metadata/state/
CREATE SCHEMA  IF NOT EXISTS mdm_prod.metadata;
CREATE VOLUME  IF NOT EXISTS mdm_prod.metadata.state;
```

Jobs run on classic job clusters (Databricks Runtime 15.4 LTS); serverless is
[not supported](../CLAUDE.md#conventions).

## Documents

| Document | What it covers |
|---|---|
| [00_overview.md](00_overview.md) | Goals, principles, glossary, layer diagram |
| [01_architecture.md](01_architecture.md) | Layers, protocols, execution flow, extension points |
| [02_config_schema.md](02_config_schema.md) | The typed config schema, parameter by parameter |
| [03_write_verbs.md](03_write_verbs.md) | Verb semantics with worked examples |
| [04_policies.md](04_policies.md) | The data quality gate, driven by a DQX ruleset |
| [05_testing.md](05_testing.md) | Unit and platform testing |
| [06_roadmap.md](06_roadmap.md) | Phased implementation plan and current status |
| [07_workspace_setup.md](07_workspace_setup.md) | Workspace setup recipe: catalogs, schemas, volumes, grants |
| [08_usage.md](08_usage.md) | Which source, strategy and verb to combine, and the traps |
| [09_export.md](09_export.md) | The export task: file naming and the CSV format |
