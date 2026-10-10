---
name: flame-data-setup
description: Helps a user onboard a dataset onto dbx-flame, from raw files into Bronze and on to Silver. Use when someone wants to load a new source, choose a write verb, configure a workflow task, or understand how verbs, snapshots and deletes behave. Asks the questions that matter, proposes the setup, and explains it in plain language.
---

# Setting up a dataset with dbx-flame

Your job: understand how the data arrives and what the business needs in Silver, then
propose a working setup and explain it in plain words. Never guess a setting. If an
answer is missing, ask. Check exact keys in `docs/02_config_schema.md` before you write
YAML; unknown keys fail the task.

## 1. The picture to explain first

```
Provider files ──► Inbound volume ──► BRONZE ──► SILVER ──► Gold (SQL view, planned)
                                     raw copy    business view
```

- **Bronze** keeps every export exactly as it arrived, plus provenance columns. It's
  almost always `append`, so nothing is ever lost.
- **Silver** is where the business rules live. The **verb** you pick decides what Silver
  looks like: latest state only, or full history.
- Each step is one task in a Databricks job, configured with flat `key: value`
  parameters. No Python per dataset.
- **Every file name must carry its export timestamp** (e.g. `CLAIMS_20261005050000.csv`).
  It becomes `__EXPORT_DATE`, the export's identity. A file without one fails the run.
- **The source can also be a table we didn't create** (a federated database, another
  team's catalog). It has no files and no stamp, so Bronze reads it whole on every run
  (`source.origin: table`, `source.increment_strategy: full_read`, `output.verb: append`):
  each read becomes one snapshot, stamped with the read time. Ask how big the table is
  and how often it may be read, since every run reads all of it from the source system.
  Ask whether it has a real modification date (a DATE or TIMESTAMP column). With one, a
  large table can be read by its changes instead (`source.increment_strategy: delta_read`,
  `source.anchor_dt.column: <that column>`), but deletes and back-dated changes are then
  invisible; without one, history re-versions every row on every read (see the README's
  Usage Cheat-Sheet).

## 2. Ask these questions

Ask in plain language, a few at a time, and skip what's already answered. Explain *why*
when a question isn't obvious.

**A. The files (→ Bronze)**
1. What format: CSV, a delimited `.txt` (which delimiter?), or JSON? Is there a header row?
   *(SAS isn't available yet.)*
2. For JSON: one document per file, or one record per line (JSON Lines)? Is the data
   wrapped in an envelope (metadata plus a `data` array)?
3. What does a file name look like? Where's the timestamp, and in what shape:
   `20261005050000` or `2026-10-05T05:00:00`?
4. Where do files land (volume path and folder)? Are they all in that one folder? *(Files
   in subfolders aren't read.)*
5. Can the columns change over time? If a new column appears, should it be added, or
   should the load stop so someone can check?
6. Do any columns need real types (dates, numbers)? What are their formats?

**B. How data is delivered**
7. Does each export contain **everything** (a full snapshot) or **only what changed**?
8. How often does it arrive, and how often will the job run? Can several exports pile up
   before a run (weekends, outages, a provider testing)?
9. Are deletes sent, and how: as a separate file, or by leaving the record out of the
   next full snapshot?
10. Does the provider send **both** a periodic full export and change files?

**C. What the business needs in Silver**
11. Is the latest state enough, or is **history** needed (what a record looked like on a
    given date)?
12. If history: must **every** version appear, or is the latest per run enough?
13. What identifies a record? (The key: one column or several.)
14. Is there a per-record modification date? What's its format, and can it be blank?
15. When a record disappears from a full snapshot, does that mean it was deleted?
16. Any data quality rules? (e.g. the key is never empty, or a value is unique.)

## 3. Choose the Silver verb

| The business needs… | Delivery | Verb |
|---|---|---|
| Only the latest snapshot, no history | Full snapshots | `full` |
| Latest state per key, no history | Changes | `upsert` |
| History, latest version per run is enough | Changes | `scd2` |
| History with **every** version, and deletes as files | Changes (+ deletes) | `complete_delta` |
| History, and a missing record means deleted | Full snapshots | `complete_delta` + `output.snapshot_scope: full` |
| Both of the above, from one source | Full + change files | two `complete_delta` tasks (§5) |

Bronze is `append` in every case, so each export is kept.

## 4. What each verb does (how to explain it)

**`append`**: adds every row it receives. Bronze's default. Silver rarely uses it.

**`full`**: Silver becomes the newest export, replacing what was there. No history.
- Several exports waiting? Only the newest is written.
- An export older than what Silver holds is skipped.
- An empty export is skipped; Silver is never emptied by accident.

**`upsert`**: one row per key, kept up to date (SCD Type 1).
- With an event time, a row is updated only by a newer version.
- Without one, the latest load wins, so set `output.dedup.order_by` or
  `output.dedup.enabled: false`.

**`scd2`**: history per key. A change closes the old row (`__END_DATE`) and opens a new
current one (`__CURRENT_FLAG = 'Y'`).
- Only changes are applied. **A record missing from a load is not deleted.**
- Several versions of a key in one run collapse to the newest, so in-between versions
  are lost. If they matter, use `complete_delta`.
- No deletes feed.

**`complete_delta`**: history that replays every waiting export in order, one at a time,
so every version appears.
- Reads from a Bronze table (`source.origin: table`).
- Optional deletes feed: a second Bronze table of deleted keys. Deleted records are
  flagged `__DELETED_FLAG = 'Y'` and closed; nothing is ever physically removed.
- `output.snapshot_scope: full`: each export is the complete truth. A record missing
  from it is closed (implicitly deleted). Of several waiting exports, only the newest is
  applied.

Explain with a small table, e.g. what Silver holds after exports 1, 2, 3 for keys A and
B. `docs/03_write_verbs.md` §5–6 has worked examples you can reuse.

## 5. Nuances to check before proposing

- **Exports must arrive in order.** Silver reads only exports newer than what it already
  holds; a late export is never read and has to be reloaded by hand. A split export must
  carry the same timestamp on every part and arrive in the same run.
- **Pile-ups:** `full` and `complete_delta` full scope apply only the newest waiting export
  (the others stay in Bronze). `scd2` keeps only the newest version per key.
  `complete_delta` delta scope replays them all.
- **Event times must be present and parseable.** UPSERT, SCD2 and COMPLETE_DELTA fail the
  batch on a blank or bad date (`event_time_invalid`). Ask if blanks happen.
- **Full snapshots use the export time as the event time** (`output.event_time.column:
  __EXPORT_DATE`), so a record's history follows the exports.
- **Full + change files (two feeds):** two Bronze tasks, and two Silver tasks into one
  table, with the **full task first**. The job's `depends_on` enforces the order.
  - Full task: `snapshot_scope: full`, event time `__EXPORT_DATE`.
  - Delta task: `source.increment_anchor: true`, event time = the modification date, so
    it applies only changes made after the newest full export.
  - **Both** Bronze tasks set `source.anchor_dt.column` (the modification date).
- **JSON Lines** needs `source.options.multiline: false`, or only the first record of each
  file is read.
- **JSON envelope**: `source.preprocessors: record_envelope` plus `source.envelope_fields`
  (the keys to lift into columns, e.g. the record ID). The rest of each item stays as JSON
  text under `DATA`, so the provider can reshape it without breaking the load.
- **Types:** declare them once, in a cast config on the Bronze task. Once a date column is
  typed, drop its `.format` from Silver settings.
- **New columns:** the default `fail_on_new_columns` stops the load.
  `add_new_columns` grows the table, but the run that first meets the column fails and
  must be re-run once.
- **Table names are UPPERCASE.** The physical catalog is `{catalog}_{env}`.

## 6. Templates

Shared parameters (YAML anchors keep tasks short):

```yaml
shared_params: &shared_params
  catalog: claims                       # physical catalog: claims_${bundle.target}
  env: "${bundle.target}"
  metadata_path: /Volumes/claims_${bundle.target}/landing/metadata/   # checkpoints
  schema_evolution: fail_on_new_columns

identity: &identity                     # ties every audit row to the job and task
  __workflow_id: "{{job.id}}"
  __workflow_run_id: "{{job.run_id}}"
  __task_key: "{{task.name}}"
  __task_run_id: "{{task.run_id}}"
```

A task (Bronze shown; Silver only changes `named_parameters`):

```yaml
- task_key: inbound_to_bronze
  libraries:
    - pypi: {package: dbx-flame}        # pin the version in production
  python_wheel_task:
    package_name: dbx-flame
    entry_point: dbx-flame
    named_parameters:
      <<: [*shared_params, *identity]
      source.origin: csv
      source.path: /Volumes/claims_${bundle.target}/landing/inbound/
      source.directory: CLAIMS
      source.file_extension: txt          # only if not .csv
      source.options.delimiter: "|"       # only if not ","
      typing.cast_config: /Workspace/.../claims_cast.yml   # optional
      output.verb: append
      output.schema_name: bronze
      output.table: CLAIMS
```

Silver `named_parameters` by pattern (each adds `source.origin: table`,
`source.schema_name`, `source.table`, `output.schema_name` and `output.table`):

| Pattern | Settings |
|---|---|
| Latest snapshot | `output.verb: full` |
| Latest per key | `output.verb: upsert`, `output.keys`, `output.event_time.column` (+ `.format`) |
| History per run | `output.verb: scd2`, `output.keys`, `output.event_time.column` (+ `.format`) |
| Every version | `output.verb: complete_delta`, `output.keys`, `output.event_time.column` |
| + deletes file | `source.deletes_table`, `output.deletes.keys`, `output.deletes.event_time.column` |
| Full snapshots, history | `complete_delta`, `output.snapshot_scope: full`, `output.event_time.column: __EXPORT_DATE` |

Optional files: a cast config (`typing.cast_config`) and a DQX ruleset
(`policies.checks_file`). Formats are in `docs/02_config_schema.md` §5–6.

## 7. Hand-over

Give the user:
1. **A one-paragraph summary** in business terms: what Silver will hold, what happens to
   late, missing or deleted records, and what makes a run fail.
2. **The job YAML**: the Bronze and Silver tasks, `depends_on` in the right order.
3. **The cast config and ruleset**, if needed.
4. **What to agree with the provider**: the file-name timestamp format, export order, no
   blank keys or event times, and how deletes are sent.
5. **How to test it**: deploy, run with a few sample files, then check the Silver rows
   and the audit table (`monitoring_{env}.audit.logs`).
