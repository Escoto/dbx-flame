# 05 — Testing Strategy

Two levels: **unit tests** (local, fast, per-layer) and **platform tests** (real Databricks
workflows over generated fixtures). The first proves the logic; the second proves the logic
survives contact with Auto Loader, Unity Catalog and the Delta merge engine, which no local
test can.

## 1. Unit tests (pytest, local)

A local SparkSession with Delta (`delta-spark`), plus `chispa` for DataFrame equality.
The layers are unit-testable *because* they only touch Context + DataFrame:

| Layer | What is tested |
|---|---|
| context | coercion (`"true"`/`true`, comma lists, enums), unknown-key rejection, aggregated errors, origin × verb × increment strategy matrix, resolved names/paths |
| pipelines | provenance columns (incl. the `__EXPORT_DATE` regex for both file-name patterns), full- and delta-read stamping, the delta read's filter, column types and pushdown, the stamped / unstamped / incomplete table rule, sanitization rules, rename patterns, pre-processor registry, SAS empty-file rules |
| typecast | YAML parsing, ordering/append semantics, missing-column error, date/timestamp formats, pass-through of undeclared columns, silent-NULL detection incl. sample capture |
| policies | ruleset parsing and DQX validation errors, result aggregation and counts, the batch handed on unchanged, warn-continues / error-raises-after-all |
| output | every verb against in-memory Delta tables: creation path, anti-filter idempotence (re-run = no-op), close-and-insert chains, `snapshot_scope: full` expiry, deletes double-merge, UPSERT newer-wins, FULL empty-skip, watermark boundaries |
| observability | audit row schema/partitioning, buffering + flush-on-failure, KPI event names |

Coverage gate: **≥70%**, enforced by `pytest` itself (`--cov-fail-under`) and run in
pre-commit/CI.

```bash
make test        # poetry run pytest, with coverage
```

On Windows, run this from WSL — Spark does not run natively on Windows.

## 2. Platform tests (Databricks)

Each platform test is a deployed job that builds its own fixtures from nothing, runs the
framework over them, and asserts the result. They live in two halves: the workflow YAML in
`workflows/platform_tests/` and the scripts in `platform_tests/<test_name>/`.

Every one follows the same shape:

```
0_cleanup  →  1_generate_data  →  inbound_to_bronze  →  2_validate_bronze
                                →  bronze_to_silver  →  3_validate_silver  →  4_…
```

| Test | Covers |
|---|---|
| `upsert_csv_test` | UPSERT (SCD Type 1): newer-wins, and a second round proving rows refresh in place instead of accumulating |
| `scd2_csv_test` | SCD2 with a string anchor date, two successive loads, history chains and validity windows, and one `__LAST_MODIFIED_DT` per run that the second load moves only on the rows it wrote or closed |
| `scd2_long_backlog_test` | SCD2 with three exports in one batch: one current row per key at its newest version, and keys the last export omits left untouched |
| `full_csv_long_backlog_test` | FULL with three exports in one batch: the target holds only the newest export |
| `full_txt_weekly_test` | Weekly pipe-delimited `.txt` snapshots into FULL over three scheduled runs: an unscheduled export arriving between runs lands in Bronze but never reaches Silver |
| `complete_delta_csv_test` | COMPLETE_DELTA: a multi-snapshot backlog replayed in order, plus a watermark re-run that must be a no-op |
| `complete_delta_csv_typecasting_test` | A week of daily full exports with a real table config: type survival through promotion, a 7-column composite key, genuinely empty fields |
| `complete_delta_csv_typecasting_full_test` | The same fixtures under `snapshot_scope: full`: only the newest export is replayed, with the *identical* current state |
| `append_table_full_read_test` | `full_read` of a plain table with no framework columns, standing in for a foreign one: two reads land in Bronze as two snapshots with `TABLE:` provenance, and Silver (FULL) matches the source after it changed in place, the deleted row included |
| `append_table_delta_read_test` | `delta_read` of a plain table on its `MODIFIED_AT`: the first read lands every row, the second only the updated and added ones, each under its own `TABLE:` stamp; Silver (UPSERT) holds the newest state per key, each row naming the read that brought it in, and keeps the row deleted at the source |
| `append_table_cross_catalog_test` | APPEND from a Bronze in one catalog into a Silver in another (`source.catalog`): Silver's checkpoint lives in the consumer's catalog, and a second round receives only the new export |
| `complete_delta_json_anchor_test` | Full + delta feeds after a pause: a backlog with two full exports resyncs Silver to the newest, and the delta feed, anchored on the `__ANCHOR_DT` both Bronze feeds stamp, applies only what came after it |
| `export_csv_schema_test` | The export task over a schema of its own: the whole schema (two tables and a view) as `<OBJECT>.csv` without the framework column, then a subset named in mixed case as `<OBJECT>_<date>.csv` dated in Europe/Berlin, replacing the first export, keeping the framework column, renaming one column and excluding another, and skipping the setting of a view it doesn't export. Asserts the exact headers, values, quotes, line breaks, NULL against `""`, ISO dates, timestamps to the microsecond and plain decimals; every file's checksum (sha256, then md5) and the manifest's types, versions and counts; the whole schema delivered as loose files to a destination of its own, the subset as a stamped zip with its md5 to the default `deliveries/PLATFORM_TEST`, each byte for byte as built; and a `file_delivered` audit row for every file delivered, holding the hash of what arrived, the manifest's version and its location |

Validation scripts assert on **data** — row counts, history chains, flags, validity windows,
and the actual typed values — and on **schema**, including that no internal helper column
leaked into the target.

Each test cleans only its own directories and tables, so every test can run in parallel.
Each test is registered in exactly one of two suites; run only the one a change can reach:

| Suite | Runs | When |
|---|---|---|
| `data_processing_test_suite` | Every ingest test: Source → Bronze → Silver, every verb and reader | A change to ingest: `context/`, `pipelines/`, `typecast/`, `policies/`, `output/` |
| `export_test_suite` | Every export test (`export_*`) | A change to the export: `entrypoints/export.py` and its config |
| `integration_test_suite` | Both suites, in parallel | A change to shared code (`mechanics/`, `observability/`, config loading), and before a release |

```bash
databricks bundle deploy -t dev -p <profile>
databricks bundle run export_test_suite -t dev -p <profile>
```

A new platform test goes in [data_processing_test_suite.yml](../workflows/data_processing_test_suite.yml)
or [export_test_suite.yml](../workflows/export_test_suite.yml); the integration suite picks
it up from there.

### Workspace prerequisites

The framework names every catalog `{catalog}_{env}`; for the tests that is
`${var.platform_tests_catalog}_${bundle.target}` (`dbx_flame_dev`). Scripts cannot create
Unity Catalog objects, so a new workspace needs these once. The step-by-step recipe, with
SQL and grants, is [07_workspace_setup.md](07_workspace_setup.md).

| Object | Used for |
|---|---|
| `<catalog>_<target>.functional_testing` (schema) | test tables |
| `<catalog>_<target>.functional_testing.source_data` (volume) | inbound files and checkpoints |
| `monitoring_<target>.audit` (schema) | the audit table, created on first run |
| `<consumer_catalog>_<target>.functional_testing` and its `source_data` volume | `append_table_cross_catalog_test`'s Silver and checkpoints |
| `<catalog>_<target>.export_testing` (schema) | `export_csv_schema_test`'s tables and view, alone, so a whole-schema export sees nothing else |

Every test script takes `[base, catalog, env, ...]` and builds the catalog the same way.

### Debugging a failed run

The suite run only says which test failed. To reach the error:

```bash
databricks jobs get-run <suite-run-id>          # each test task's run_job_task.job_id
databricks jobs list-runs --job-id <job-id> --limit 1
databricks jobs get-run <test-run-id>           # the failed task and its run_id
databricks jobs get-run-output <task-run-id>    # error and error_trace
```

A cluster that never started shows its reason in the task's `state_message` instead.

### Why fixtures are generated, never committed

The tests build their own CSVs at run time from a `_shared.py` module that also holds the
expected outcomes. Fixture and assertion therefore cannot drift apart, and a test that
changes its data has to change its expectations in the same file.

## 3. What is explicitly NOT tested

- Databricks platform behavior — Auto Loader internals, the Delta merge engine.
