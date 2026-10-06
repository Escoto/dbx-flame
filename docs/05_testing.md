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
| context | coercion (`"true"`/`true`, comma lists, enums), unknown-key rejection, aggregated errors, origin × verb requirements matrix, resolved names/paths |
| pipelines | provenance columns (incl. the `__EXPORT_DATE` regex for both file-name patterns), sanitization rules, rename patterns, pre-processor registry, SAS empty-file rules |
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
| `scd2_csv_test` | SCD2 with a string anchor date, two successive loads, history chains and validity windows |
| `scd2_long_backlog_test` | SCD2 with three exports in one batch: one current row per key at its newest version, and keys the last export omits left untouched |
| `full_csv_long_backlog_test` | FULL with three exports in one batch: the target holds only the newest export |
| `full_txt_weekly_test` | Weekly pipe-delimited `.txt` snapshots into FULL over three scheduled runs: an unscheduled export arriving between runs lands in Bronze but never reaches Silver |
| `complete_delta_csv_test` | COMPLETE_DELTA: a multi-snapshot backlog replayed in order, plus a watermark re-run that must be a no-op |
| `complete_delta_csv_typecasting_test` | A week of daily full exports with a real cast config: type survival through promotion, a 7-column composite key, genuinely empty fields |
| `complete_delta_csv_typecasting_full_test` | The same fixtures under `snapshot_scope: full`: only the newest export is replayed, with the *identical* current state |
| `append_delta_cross_catalog_test` | APPEND from a Bronze in one catalog into a Silver in another (`source.catalog`): Silver's checkpoint lives in the consumer's catalog, and a second round receives only the new export |
| `complete_delta_json_anchor_test` | Full + delta feeds after a pause: a backlog with two full exports resyncs Silver to the newest, and the delta feed, anchored on the `__ANCHOR_DT` both Bronze feeds stamp, applies only what came after it |

Validation scripts assert on **data** — row counts, history chains, flags, validity windows,
and the actual typed values — and on **schema**, including that no internal helper column
leaked into the target.

Each test cleans only its own directories and tables, so the whole suite can run in parallel:

```bash
databricks bundle deploy -t dev -p <profile>
databricks bundle run integration_test_suite -t dev -p <profile>
```

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
| `<consumer_catalog>_<target>.functional_testing` and its `source_data` volume | `append_delta_cross_catalog_test`'s Silver and checkpoints (`platform_tests_consumer_catalog`; for now the same catalog) |

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
- File-writer implementations: `files.py` is an interface, and its tests arrive with it.
