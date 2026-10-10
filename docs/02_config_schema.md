# 02 — Configuration Schema

## 1. How parameters travel

Workflow YAMLs keep passing **flat `key: value` task parameters** (now as `python_wheel_task.named_parameters`). YAML anchors and shared blocks keep working exactly as today. Dotted keys express nesting; the Start layer splits them into a nested dict and validates the result with pydantic.

```
flat params ──split on "."──▶ nested dict ──pydantic──▶ TaskConfig ──▶ Context
```

Rules applied by the loader:

- **Coercion**: booleans accept `true`/`false` in any casing, quoted or not; lists accept comma-separated strings (`"ID, NAME"` → `["ID", "NAME"]`); enums are case-insensitive.
- **Unknown keys are rejected**, so a typo in a parameter name fails the task instead of being silently ignored.
- **All errors aggregate** into one `ConfigValidationError` listing every problem, so a misconfigured workflow is fixed in one iteration, not one error at a time. It is raised before the audit logger exists, so it reaches the task's driver log and run output, not the audit table.

## 2. Schema (pydantic model tree)

### Run identity (reserved `__` parameters)

Four parameters carry Databricks traceability into every audit row. They are consumed by
the entry point before validation, so they never appear in `TaskConfig`, and they are the
only parameters allowed to start with `__`:

| Parameter | Dynamic value | Audit column | Stable across runs |
|---|---|---|---|
| `__workflow_id` | `{{job.id}}` | `__workflow_id` | yes |
| `__workflow_run_id` | `{{job.run_id}}` | `__workflow_run_id` | no |
| `__task_key` | `{{task.name}}` | `__task_key` | yes |
| `__task_run_id` | `{{task.run_id}}` | `__task_run_id` | no |

Any that are absent default to the literal string `local`, and the run logs a
`run_identity_missing` WARNING naming them — so ad-hoc local runs stay frictionless while a
workflow that forgets them is visible in the audit table rather than silently untraceable.

```yaml
# ── identity ─────────────────────────────────────────────
catalog: clinical                 # str, required
env: dev                          # str, required   (always ${bundle.target})
metadata_path: /Volumes/clinical_dev/.../metadata/   # str, required; a Volume in {catalog}_{env}

# ── source (Layer 2: Pipeline) ───────────────────────────
source.origin: csv                # enum: csv | json | sas | table   (required)

# file origins (csv/json/sas):
source.path: /Volumes/.../inbound/      # base volume path
source.directory: Subjects                # subdirectory
source.file_extension: txt              # optional; defaults to origin (csv reads *.csv)
                                        #   any case matches (.txt, .TXT); a leading dot is dropped

source.anchor_dt.column: LAST_MODIFIED_DT  # optional; copied into __ANCHOR_DT as a timestamp
source.anchor_dt.format: "yyyy-MM-dd HH:mm:ss"  # optional; only when the column is a string

source.snapshot_time_pattern: datetime  # enum: datetime (default, 20260930103500) | iso (2026-09-30T10:35:00)
source.preprocessors: ""                # ordered list of registered names, e.g. "record_envelope"
source.envelope_fields: "uri, status"   # list; paired with the record_envelope pre-processor
source.rename_patterns: ""              # list of regex=replacement pairs, e.g. "__[Vv]$="
source.options.header: true             # csv only (default true)
source.options.delimiter: ","           # csv only (default ",")
source.options.quote: '"'               # csv only (default "")
source.options.escape: "\\"             # csv/json (default "\")
source.options.multiline: true          # csv/json (default true)
source.options.schema_hints: "ID STRING"  # json only; optional Auto Loader hints

# table origin:
source.catalog: raw                    # optional; the source's catalog, as {catalog}_{env}
                                       #   unset → the task's catalog
source.schema_name: bronze_main          # schema of the source table
source.table: SUBJECTS_UPDATES          # source table
source.deletes_table: SUBJECTS_DELETES  # optional deletes feed
source.increment_strategy: full_read   # optional: checkpoint | watermark | full_read; unset → the verb's default
source.increment_anchor: false         # bool; watermark on __ANCHOR_DT instead of __EXPORT_DATE

schema_evolution: fail_on_new_columns    # what happens when a new column shows up:
                                        #   add_new_columns | add_new_columns_with_type_widening
                                        #   | fail_on_new_columns (default) | none
                                        # one knob, read and write together — see §4

# ── typing (Layer 3) ─────────────────────────────────────
typing.cast_config: /Workspace/.../subjects_cast.yml   # optional; absent → types pass through
typing.validate_casts: true             # bool, default true (silent-NULL detection)

# ── policies (Layer 4) ───────────────────────────────────
policies.checks_file: /Volumes/.../subjects_checks.yml  # optional; DQX ruleset

# ── output (Layer 5) ─────────────────────────────────────
output.verb: scd2                 # enum: append | full | upsert | scd2 | complete_delta (required)
output.schema_name: silver_main    # str, required
output.table: SUBJECTS            # str, required, UPPERCASE
output.keys: "ID"                 # list; required by upsert/scd2/complete_delta
output.event_time.column: MODIFIEDDATE      # required by scd2/complete_delta
output.event_time.format: "M/d/yyyy h:mm:ss a"  # optional; only when the column is a string (…_fmt)
output.snapshot_scope: delta      # enum: delta | full (default delta); complete_delta only
output.dedup.enabled: true        # bool, default true
output.dedup.columns: "ID"        # list; empty → output.keys
output.dedup.order_by: MODIFIEDDATE     # empty → output.event_time.column
output.dedup.order_by_format: "M/d/yyyy h:mm:ss a"  # optional
output.deletes.keys: "ID"               # deletes feed join keys
output.deletes.event_time.column: DATE_DELETED
output.deletes.event_time.format: yyyyMMddHHmmss
output.tags.project: dbx-flame          # Unity Catalog tags on the target, one key per tag
```

Notes:

- `source.catalog` lets a table-origin task read its source and deletes tables from another
  catalog in the same metastore, so each layer can have a catalog of its own. The consumer
  tracks its own consumption: the target, its checkpoints and the audit row all stay in the
  task's `catalog`, which is why `metadata_path` must be a Volume there. The job's identity
  needs `SELECT` on the source catalog. Pointing an existing task at another source catalog
  means a new source table, so its checkpoint must be reset.
- Each verb declares the increment strategies it supports and its default (`checkpoint` for
  append/full/upsert/scd2, `watermark` for complete_delta), and each origin declares the ones
  it can read with: file origins only `checkpoint` (Auto Loader resumes from nothing else), a
  table `checkpoint`, `watermark` and `full_read`. `source.increment_strategy` picks another
  only where both the verb and the origin allow it; any other choice is rejected at Start.
  The Start layer resolves the result onto the `Context`.
- `source.increment_strategy: full_read` reads the whole source table as one batch on every
  run, with no checkpoint. It is for a table we didn't create (a federated one, say), which
  has no stream to resume and no `__EXPORT_DATE` to follow, and every verb accepts it. Each
  read lands as one snapshot: `__EXPORT_DATE` and `__BRONZE_LAST_MODIFIED_DT` are the run's
  read time, taken once, and `__SOURCE` is `TABLE:<catalog>.<schema>.<table>@<read time>`. A
  deletes table read in the same run shares that stamp, so both land in one snapshot. Every
  run reads the whole table from the source system, so mind its size and the load on it.
  Land it in Bronze with APPEND, one snapshot per run, and promote from there.
- Whether a table is ours is decided by its columns, `__SOURCE` and `__EXPORT_DATE`, before
  anything is read. A violation fails the run as a [platform policy](04_policies.md#9-platform-policies):

  | The source table carries | `checkpoint` · `watermark` | `full_read` |
  |---|---|---|
  | both (a table we stamped) | read as is; provenance travels unchanged | `stamped_full_read`: it would load every export it holds again |
  | neither (a table we didn't create) | `unstamped_table`: an increment can't be stamped as a snapshot | stamped with the read time |
  | only one | `malformed_table` (incomplete metadata) | `malformed_table` |
- `source.snapshot_time_pattern` is the export stamp agreed with the source, and Bronze
  parses `__EXPORT_DATE` from it. Only the configured pattern is read. A file whose name
  doesn't carry it fails the run before anything from its batch is written, so it never
  lands in Bronze; remove or rename it to continue. File origins only: a table origin
  reads the `__EXPORT_DATE` its Bronze already parsed.
- `source.anchor_dt` names a per-record date, typically a modification date. The Bronze
  task copies it, after typing, into `__ANCHOR_DT` as a timestamp, leaving the original
  untouched. A value that doesn't parse fails the batch; a NULL is logged
  (`anchor_missing`) and passes. File origins only.
- `source.increment_anchor: true` makes the watermark compare `__ANCHOR_DT` in place of
  `__EXPORT_DATE`: only rows whose anchor is later than the target's highest are read, so
  a delta feed applies only the changes made after what Silver already holds. Because
  `__ANCHOR_DT` is a real timestamp column, the filter reaches the scan rather than
  parsing a string per row. Every table it reads (source, deletes feed, target) must
  carry `__ANCHOR_DT`, so **every Bronze feeding an anchored Silver sets
  `source.anchor_dt`**, the full feed included; a missing column fails the run. It
  requires `source.increment_strategy: watermark`, and is rejected with
  `output.snapshot_scope: full`: the anchor returns only the records that moved, and
  full scope would expire every record the export still carries unchanged.
- Dedup is **on by default** for keyed verbs (upsert/scd2/complete_delta): the latest row per
  `output.keys`, ordered by `output.event_time`, ties broken by the newer `__EXPORT_DATE`.
  An upsert without `output.event_time` must set `output.dedup.order_by` or
  `output.dedup.enabled: false`. It runs before the user policies, so they judge the
  deduplicated batch; COMPLETE_DELTA dedups each snapshot on its own.
- `record_envelope` unwraps a vendor JSON envelope — `metadata.export_date` plus a `data`
  array — into one row per item. The keys named in `source.envelope_fields` are lifted into
  columns of their own and the whole item stays under `DATA` as JSON text, so only those
  keys have to stay stable when the vendor reshapes a payload.
- `source.envelope_fields` and `source.preprocessors: record_envelope` are validated as a
  pair. Either half alone is a silent no-op — fields with no envelope are never read, and
  an envelope with no fields leaves the batch without a key column — so both are rejected
  at Start.
- `source.options.multiline` defaults to `true`, which reads each JSON file as one document,
  as the `record_envelope` export is. A JSON Lines source (one object per line) **must** set
  it to `false`: under the default, Spark reads only the file's first object and silently
  drops the rest. For CSV, `true` only costs parallelism, because a file is then read by a
  single task.
- There is no single overloaded "mode" parameter: Bronze ingestion uses `output.verb: append|full`, Silver promotion uses `output.verb: scd2|complete_delta|upsert`. The verb alone determines how the write behaves.
- The physical catalog is `{catalog}_{env}`, so one config serves every environment. Table names are UPPERCASE by convention, and that convention is validated.

## 3. Origin × verb requirements matrix

Verbs are layer-agnostic; the Start layer enforces this matrix (each writer *declares* its requirements — the matrix is derived, not hardcoded):

| | `append` | `full` | `upsert` | `scd2` | `complete_delta` |
|---|---|---|---|---|---|
| **any origin** | `output.*` target | `output.*` target | + `output.keys` | + `output.keys`, `output.event_time.column` | — |
| **file origins** (csv/json/sas) | typical Inbound→Bronze | supported | supported | supported | not supported (needs a Delta updates table) |
| **table origin** | supported | supported | supported | typical Bronze→Silver | optional `source.deletes_table` (with `output.deletes.*`) |
| **deletes feed** | — | — | — | — | optional |
| **`snapshot_scope: full`** | — | — | — | — | allowed |

Validation failures name the missing/conflicting keys, e.g.:
`output.verb=scd2 requires: output.keys, output.event_time.column — missing: output.event_time.column`.

## 4. Schema evolution

`schema_evolution` sits at the task level rather than under `source` or `output`, because
it is one decision with effects in both places:

| It sets | Where | Effect |
|---|---|---|
| `cloudFiles.schemaEvolutionMode` | reader (file origins) | how Auto Loader reacts to a new column |
| `mergeSchema` | every write | whether the target may gain a column |
| `spark.databricks.delta.schema.autoMerge.enabled` | UPSERT's merge | the same, for a MERGE, which has no write option |

Splitting it into a read setting and a write setting would let them disagree, and the
disagreement is always a broken task: a reader told to add new columns feeding a write
told to refuse them fails the moment the source changes. One value makes that
unrepresentable.

The values are Auto Loader's own modes; see the [Databricks
documentation](https://docs.databricks.com/ingestion/auto-loader/schema.html) for what each
does on the read. On the write they reduce to two outcomes: `add_new_columns` and
`add_new_columns_with_type_widening` let the target grow, the rest refuse.

Any mode is valid for any origin. A table origin has no Auto Loader, so only the write
half applies there; the framework works that out rather than asking.

Under `add_new_columns`, a new column fails the run once. This is standard Auto Loader
behavior: it records the column in the schema location and stops the stream, and the
restart reads the file with the column included. Give every file-origin task
`max_retries: 1` so that restart happens within the same run; without it, every run that
meets a new column fails and the next scheduled run picks it up.

Two writes don't refuse an unexpected column on their own. A MERGE with `autoMerge` off
accepts the batch and discards the column, and SCD2 (and so COMPLETE_DELTA) commits its
close before the insert that Delta would refuse. UPSERT, SCD2 and COMPLETE_DELTA therefore
compare the batch against the target before their first commit and raise
`PlatformPolicyViolation` (`unexpected_columns`), so `fail_on_new_columns` means the same
thing on every verb.

## 5. Cast config file (Layer 3)

Column types live in their own YAML, referenced by `typing.cast_config`. A type list can be
long, and keeping it out of the workflow config stops it from muddying the task parameters:

```yaml
columns:
  - name: ID
    cast: { target_type: bigint }
  - name: VISIT_DATE
    cast: { target_type: date, format: "yyyy-MM-dd" }
  - name: MODIFIED
    cast: { target_type: timestamp, format: "M/d/yyyy h:mm:ss a" }
```

The file carries types and nothing else. Whether casts are validated is the workflow-level
`typing.validate_casts`, not a property of the type list, and unknown keys are rejected here
for the same reason they are in the task config.

**A column the config does not name keeps the type it arrived with.** Out of Auto
Loader that is string, so declaring types on the way into Bronze is enough to define
the table. It matters on promotion: a Bronze→Silver task that declares nothing is
saying *no changes*, not *flatten back to string*, so Bronze's types survive. Note
that re-declaring the same config at Silver is not a way to achieve this — a
`date`/`timestamp` entry would re-parse an already-typed column with its source
format and fail the cast validation.

Cast validation is single-pass, and framework metadata columns are exempt — a `__*` column named here is left alone, with a warning in the audit table.

## 6. Checks file (Layer 4)

Data quality rules live in their own YAML too, referenced by `policies.checks_file`, in
[Databricks DQX](https://databrickslabs.github.io/dqx/) format:

```yaml
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: ID
```

The framework reads the path and hands the contents to DQX; the check vocabulary is DQX's,
not this framework's. See [04_policies.md](04_policies.md) for how the file is applied and
what `criticality` means to the gate.

## 7. Worked example

A Bronze→Silver promotion task, carrying history with COMPLETE_DELTA and a deletes feed:

```yaml
- task_key: bronze_to_silver
  python_wheel_task:
    package_name: dbx-flame
    entry_point: dbx-flame
    named_parameters:
      <<: [*basic_config_params]           # catalog, env, metadata_path anchors
      source.origin: table
      source.schema_name: functional_testing
      source.table: *updates_table
      source.deletes_table: *deletes_table
      output.verb: complete_delta
      output.schema_name: functional_testing
      output.table: *silver_table
      output.keys: "ID, NAME"
      output.event_time.column: UPDATEDTIME
      output.event_time.format: yyyyMMddHHmmss
      output.dedup.enabled: true
      output.dedup.columns: "ID, NAME"
      output.dedup.order_by: UPDATEDTIME
      output.dedup.order_by_format: yyyyMMddHHmmss
      output.deletes.keys: "ID, NAME"
      output.deletes.event_time.column: DATE_DELETED
      output.deletes.event_time.format: yyyyMMddHHmmss
```

Every parameter is a flat `key: value` string, so YAML anchors and Asset Bundle
substitutions keep working; the Start layer is what turns the dotted keys into the
nested, validated `TaskConfig` above.

Note `output.event_time.format`. It is needed only while `UPDATEDTIME` is still a
string. Once a cast config has typed that column, the format must be **omitted** here —
re-parsing an already-typed timestamp with a source format yields NULL and fails cast
validation. See §5.
