# 07 — Workspace Setup

A recipe to prepare a Databricks workspace for dbx-flame. It sets up the **platform tests**
step by step, because they are the quickest way to see the framework work end to end.

> **The platform tests are optional.** You don't need them to run your own pipelines, but
> running them once is the recommended way to learn how the framework behaves. Steps 1–4
> are the same for your own catalogs: swap the test names for yours (see
> [Your own pipelines](#your-own-pipelines)).

The framework creates **tables only**: the audit table and every target table. It never
creates catalogs, schemas or volumes, so you create those once, by hand or any automation you may already have in place.

## The naming convention

This is required, not just a convention. The framework builds every Unity Catalog name from
two task parameters, and it never accepts a full catalog name. A task with

```yaml
catalog: mdm
env: dev
```

reads and writes in catalog `mdm_dev` (`{catalog}_{env}`), and logs to
`monitoring_dev.audit.logs` (`monitoring_{env}`).

- **`monitoring` is the only fixed name.** Every environment needs a `monitoring_{env}`
  catalog with an `audit` schema. Name your data catalogs (`mdm`, `sales`, `dbx_flame`, ...)
  however you like, as long as each one ends in `_{env}`.
- **`env` is the bundle target.** Workflows pass `env: ${bundle.target}`, so deploying with
  `-t dev` means `_dev` everywhere. A target named `dev_01` needs `monitoring_dev_01`.
- **One catalog per task.** A task reads and writes in the same `{catalog}_{env}`: a
  `delta` source (`source.schema_name`, `source.table`) is looked up there too. Bronze and
  Silver of a dataset therefore live in the same catalog, usually in different schemas.
- **Table names are UPPERCASE**, which is validated at Start. Catalog, schema and volume
  names have no such rule; keep them lowercase.

A catalog that doesn't match the pattern isn't found, and a missing `monitoring_{env}`
fails every run before it reads any data, because the audit write comes first.

## Requirements

- A workspace with Unity Catalog, and the right to create catalogs (or an admin to run
  steps 2–3 for you).
- The Databricks CLI with a profile for that workspace.
- Poetry, to build the wheel the bundle deploys (see the [README](../README.md#development)).

## 1 · Pick your target

Choose the bundle target to deploy to; it becomes `env`. The test catalog comes from the
`platform_tests_catalog` variable in [databricks.yml](../databricks.yml), default `dbx_flame`.

For target `dev` that gives:

| Object | Name |
|---|---|
| Audit catalog and schema | `monitoring_dev.audit` |
| Test catalog | `dbx_flame_dev` |
| Test schema | `dbx_flame_dev.functional_testing` |
| Test volume | `dbx_flame_dev.functional_testing.source_data` |

The rest of this guide uses `dev`. Substitute your own target throughout.

> **Note:** `dbx_flame_dev` is only the example. If you already have a test catalog, set
> `platform_tests_catalog` to its prefix instead, as long as the full name follows
> `{catalog}_{target}`: an existing `sandbox_dev` means `platform_tests_catalog: sandbox`
> on target `dev`. Set the target to match too. The bundle defines two, `dev` and `dev_01`,
> because we run more than one test environment, and each sets its own
> `platform_tests_catalog` (`dbx_flame` and `testing`, so `dbx_flame_dev` and
> `testing_dev_01`).

## 2 · Create the audit location

```sql
CREATE CATALOG IF NOT EXISTS monitoring_dev;
CREATE SCHEMA  IF NOT EXISTS monitoring_dev.audit;
```

Don't create `logs`: the first run creates it with the right columns.

## 3 · Create the data catalog, schema and volume

```sql
CREATE CATALOG IF NOT EXISTS dbx_flame_dev;
CREATE SCHEMA  IF NOT EXISTS dbx_flame_dev.functional_testing;
CREATE VOLUME  IF NOT EXISTS dbx_flame_dev.functional_testing.source_data;
```

The tests use one volume for two roots, and create the folders under them themselves:

| Path | Parameter | Holds |
|---|---|---|
| `/Volumes/dbx_flame_dev/functional_testing/source_data/inbound/` | `source.path` | the generated input files |
| `/Volumes/dbx_flame_dev/functional_testing/source_data/metadata/` | `metadata_path` | checkpoints and schema hints |

## 4 · Grant access

Jobs deployed in development mode run as the person who deploys them. If that person
created the objects in steps 2–3, they own them and can skip this step. Otherwise, grant the
job's identity (a user, group or service principal):

```sql
GRANT USE CATALOG ON CATALOG monitoring_dev TO `<principal>`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA monitoring_dev.audit TO `<principal>`;

GRANT USE CATALOG ON CATALOG dbx_flame_dev TO `<principal>`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY, APPLY TAG, READ VOLUME, WRITE VOLUME
  ON SCHEMA dbx_flame_dev.functional_testing TO `<principal>`;
```

`APPLY TAG` is there because the framework tags every table it writes. It also sets table
properties, and only the table owner can do that, so keep each table's tasks running under
one identity.

## 5 · Configure the bundle

In [databricks.yml](../databricks.yml), set the target's `workspace.profile` to your CLI
profile, and `platform_tests_catalog` if you don't want `dbx_flame`. Note that `dev_01` is
marked `default: true`, so a command without `-t` deploys there.

The test jobs run on classic single-node job clusters with `node_type_id: m7gd.large`, an
AWS type. On Azure or GCP, change it in each file under
[workflows/platform_tests/](../workflows/platform_tests/) to an equivalent. Don't switch
them to serverless; it [isn't supported](../CLAUDE.md#conventions).

## 6 · Deploy and run

```bash
databricks bundle validate -t dev -p <profile>
databricks bundle deploy -t dev -p <profile>
databricks bundle run integration_test_suite -t dev -p <profile>
```

The suite runs every test job in parallel. To trace a failure, see
[Debugging a failed run](05_testing.md#debugging-a-failed-run).

## 7 · Check it worked

```sql
SHOW TABLES IN dbx_flame_dev.functional_testing;   -- the *_BRONZE_1 / *_SILVER_1 tables

SELECT type, name, catalog, schema, table, description
FROM monitoring_dev.audit.logs
ORDER BY time_stamp DESC;
```

Every run leaves a `pipeline_start` and `pipeline_complete` row.

## Your own pipelines

Do steps 2–4 for each environment, with your names. For `catalog: mdm` on target `prod`,
writing to `bronze` and `silver`:

```sql
CREATE CATALOG IF NOT EXISTS monitoring_prod;
CREATE SCHEMA  IF NOT EXISTS monitoring_prod.audit;

CREATE CATALOG IF NOT EXISTS mdm_prod;
CREATE SCHEMA  IF NOT EXISTS mdm_prod.bronze;
CREATE SCHEMA  IF NOT EXISTS mdm_prod.silver;
CREATE SCHEMA  IF NOT EXISTS mdm_prod.landing;
CREATE VOLUME  IF NOT EXISTS mdm_prod.landing.files;
```

Then:

- **One schema for each `output.schema_name`** you use. A table is created on first write; its
  schema is not.
- **A volume for `source.path` and `metadata_path`.** They can share one, as the tests do.
  Under `metadata_path` the framework keeps one folder per table:
  `{metadata_path}/{catalog}_{env}/{schema}/{TABLE}/_checkpoint/`.
- **One `monitoring_{env}` per target**, shared by every catalog in that environment.
- **The grants from step 4** on each schema and volume the job touches.

Every task parameter is described in [02_config_schema.md](02_config_schema.md), and the
`flame-data-setup` skill walks you through onboarding a dataset.
