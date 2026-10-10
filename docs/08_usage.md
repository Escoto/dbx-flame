# 08 — Usage

Which source, increment strategy (`source.increment_strategy`) and verb to combine at each
step, and the traps that a valid config doesn't rule out. The parameters themselves are in
[02_config_schema.md](02_config_schema.md), and what each verb does in
[03_write_verbs.md](03_write_verbs.md).

## 1. Cheat-sheet

✅ recommended · ✔️ possible · ⚠️ valid, but read the note · ❌ rejected by the framework

| Step | Source | Strategy | Verb | | Why |
|---|---|---|---|---|---|
| Inbound → Bronze | Files (csv, json) | `checkpoint` | APPEND | ✅ | Bronze keeps every export exactly as it arrived |
| | | `checkpoint` | FULL · UPSERT · SCD2 | ✔️ | Bronze stops being the original record |
| | | `watermark` · `full_read` · `delta_read` | any | ❌ | Auto Loader only resumes from its checkpoint |
| | | — | COMPLETE_DELTA | ❌ | Replays snapshots from a table, not files |
| External table → Bronze | A table we didn't create (e.g. a federated one) | `full_read` | APPEND | ✅ | One complete snapshot per run, stamped with the read time. Sees deletes |
| | | `delta_read` | APPEND | ✅ | Only the rows changed since the last run. For a table too large to read whole (§3) |
| | | `full_read` | FULL | ✔️ | Current state only: past reads are not kept |
| | | `delta_read` | FULL | ❌ | Would replace the table with only the changed rows |
| | | `checkpoint` · `watermark` | any | ❌ | It carries no `__EXPORT_DATE` to follow |
| Bronze → Silver | A table we stamped | `checkpoint` | FULL | ✅ | The newest export is the current state |
| | | `checkpoint` | UPSERT | ✅ | Latest version per key (SCD1); deletes stay |
| | | `checkpoint` · `watermark` | SCD2 | ✅ | History of changes; deletes stay |
| | | `watermark` | COMPLETE_DELTA, `snapshot_scope: delta` | ✅ | Replays every export in order, whole or partial. Keys an export omits stay current; only a deletes feed retires them |
| | | `watermark` + `increment_anchor` | COMPLETE_DELTA, `snapshot_scope: delta` | ✅ | Reads only rows whose `__ANCHOR_DT` moved, so a full feed resent unchanged costs nothing |
| | | `watermark` | COMPLETE_DELTA, `snapshot_scope: full` | ⚠️ | Only over a Bronze that holds whole exports (§2) |
| | | `watermark` + `increment_anchor` | COMPLETE_DELTA, `snapshot_scope: full` | ❌ | Reads only the rows that moved; every other one would be retired |
| | | `full_read` · `delta_read` | any | ❌ | Would stamp every export it already holds again |
| External table → Silver | A table we didn't create | `full_read` | UPSERT · SCD2 · COMPLETE_DELTA | ⚠️ | Skips Bronze, so no original record is kept. Prefer landing it in Bronze first |
| | | `delta_read` | UPSERT · SCD2 · COMPLETE_DELTA (`snapshot_scope: delta`) | ⚠️ | Same, and deletes never reach Silver |
| | | `delta_read` | COMPLETE_DELTA, `snapshot_scope: full` | ❌ | Each read is partial: every row it lacks would be retired |
| | | `delta_read` + a deletes table | COMPLETE_DELTA | ❌ | Land the deletes feed in Bronze with its own `full_read` task, then apply it from there |

## 2. Notes on every row

- **Keyed verbs detect change by `output.event_time.column`**, never by content. Point it at
  a real change date from the source; under `delta_read` it is usually the column the read
  follows. With `__EXPORT_DATE`, every export or full read re-versions every unchanged row
  in SCD2 and COMPLETE_DELTA, and rewrites it in UPSERT (gh #51).
- **COMPLETE_DELTA with `snapshot_scope: full` re-versions every row of each snapshot**,
  changed or not, so history grows by the table's size per export (gh #51).
- **COMPLETE_DELTA with `snapshot_scope: full` needs whole exports.** It retires every key
  the newest export omits, so its source must hold each export whole: files, or a Bronze
  filled by APPEND from files or `full_read`. A Bronze filled by `delta_read`, an anchored
  watermark, SCD2 or COMPLETE_DELTA holds only part of each export, and every row it lacks
  is retired. Start rejects the single-task combinations; across two tasks it is yours to
  configure (gh #36).
- **Full + delta feeds** from the same source run as two tasks into the same Silver: the
  full task (`snapshot_scope: full`) first, then the delta task, usually anchored
  (`source.increment_anchor`) so it applies only what came after the newest full export.
  The job's task dependencies enforce the order, not the framework.

## 3. A table we didn't create: full or delta read

| | `full_read` | `delta_read` |
|---|---|---|
| Reads | the whole table, every run | only rows whose `source.anchor_dt` column is past the target's highest `__ANCHOR_DT` |
| Needs | nothing | a DATE, TIMESTAMP or TIMESTAMP_NTZ column recording when each row last changed |
| Each read is | one complete snapshot | a partial one |
| Deletes at the source | seen: the row is missing from the next snapshot | invisible |
| Cost on the source system | the whole table per run: mind its size and the load | only the changed rows; the filter reaches the source |

`delta_read` also misses:

- a row that lands later with a change time at or below the watermark (late or back-dated
  updates). A DATE column makes this a whole day: a row changed later on the last day read
  is never picked up;
- a row whose change time is NULL.

When deletes matter and the table is small enough, prefer `full_read`. A deletes feed the
source keeps as its own table is landed in Bronze with its own `full_read` task.

## 4. Verb selection guide

| Scenario | Verb |
|---|---|
| Land raw files into Bronze | `append` (or `full` for full-refresh drops) |
| Bronze→Silver, source sends change feeds, all history must be visible | `complete_delta` |
| Bronze→Silver, current-state tracking with history, latest per batch is enough | `scd2` |
| Source sends complete snapshots, only the latest matters | `full` |
| Source sends complete snapshots, absent = deleted, history must stay | `complete_delta` + `snapshot_scope: full` |
| Bronze→Silver, latest state only, no history | `upsert` |
| Reference data, full refresh | `full` |
| Gold | not a verb — a materialized view over Silver ([Gold](00_overview.md#gold)) |
