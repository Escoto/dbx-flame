# 03 — Write Verbs

The Output layer writes with one of five verbs. Verbs are **layer-agnostic**: any origin can pair with any verb whose declared requirements are satisfied (matrix in [02_config_schema.md](02_config_schema.md) §3). All Delta writes preserve the metadata contract below.

## 0. Metadata contract (all history-tracking verbs)

| Column | Set by | Meaning |
|---|---|---|
| `__EXPORT_DATE` | Pipeline | Source export timestamp (from the file name, per `source.snapshot_time_pattern`; the read time for a full or delta read) |
| `__SOURCE` | Pipeline | How and where the record was read: `FILE:<path as Unity Catalog reports it>`, or `TABLE:<catalog>.<schema>.<table>@<read time>` for a full or delta read of a table we didn't create |
| `__RECORD_HASH` | Pipeline | sha256 of the record's values where it entered the framework (below); how the keyed verbs tell a changed record from an unchanged one |
| `__ANCHOR_DT` | Pipeline | `source.anchor_dt` as a timestamp (only when configured; for a delta read, the source column it followed) |
| `__LAST_MODIFIED_DT` | Output | When the framework last wrote the row in this table: every verb's insert, and every update (UPSERT's match, SCD2's close, `snapshot_scope: full` expiry, the delete flag and close). One value per run, shared by all its micro-batches and statements |
| `__START_DATE` | Output | Validity start of the history row |
| `__END_DATE` | Output | Validity end (NULL = open) |
| `__CURRENT_FLAG` | Output | `'Y'` current version / `'N'` historical |
| `__DELETED_FLAG` | Output | `'Y'` soft-deleted entity |

`__SOURCE`, `__EXPORT_DATE` and `__RECORD_HASH` are written once, where the data enters the framework, and never overwritten on promotion. Every Silver and Gold row therefore still names the exact file it came from, when it was exported and what it contained: record-level traceability, which table lineage alone can't give. `__EXPORT_DATE` is the only source of truth for time; nothing parses it back out of `__SOURCE`.

### Change detection: `__RECORD_HASH`

The hash is stamped where a record enters: a file read, whatever the target layer, or a read of a table we didn't create. A table we stamped carries it unchanged, and no later task recomputes it, so a Bronze row and the Silver row promoted from it share one hash.

- **Inputs**: the values of every non-framework column, after the rename and the cast, sorted by the final column name. Column names don't go in, so the order columns were read in never matters. Framework (`__`) columns are left out, so the same record in a new export, with a new `__EXPORT_DATE` and `__SOURCE`, keeps its hash.
- **Form**: each value as text, prefixed with its length (so `("ab", "c")` and `("a", "bc")` differ); NULL has its own marker, distinct from `''`. A timestamp goes in as microseconds since the epoch, so the session time zone doesn't change it; nested values as JSON, NULL fields included. `sha2(…, 256)` over the joined text.
- **Reproducible from the stored row**: the row holds both the final names and the cast values, so anyone can recompute its hash from the table alone.
- **What counts as a change**: any value. A new column, even NULL in every row, is a new schema and so a new version of every record; renaming a column is a schema change too. Changing a cast changes the values, and with them the hash.
- **Accepted risk: two paths, two hashes.** Raw → Bronze → Silver hashes after the Bronze task's rename and cast; a file read straight into Silver hashes after the Silver task's. The same file row gets the same hash both ways only when both tasks give it the same names and the same values: `05/01/2026` kept as STRING and cast to DATE give two hashes. Feed each Silver table by one path only. A table that switches path re-versions each key once, at the switch.
- **Accepted risk: a column that changes with every export.** Every non-framework column is hashed, so one whose value moves with each export while the record doesn't gives every record a new hash, and that export re-versions every row. `record_envelope` lifts the envelope's export date into such a column, `EXPORT_DATE`, and a vendor `extracted_at` behaves the same. For an envelope source, every full re-sync therefore re-versions the whole table; delta pushes carry only changed records, so they are unaffected. Leaving such columns out of the hash is gh #82.

`__LAST_MODIFIED_DT` is not provenance: each table has its own, and a write replaces the value a row read from another framework table carried in. A row's `__SOURCE` and `__EXPORT_DATE` lead back to its Bronze row, whose own `__LAST_MODIFIED_DT` is the ingest time. It is metadata only: no verb or increment strategy reads it. A table we didn't create that already has a `__LAST_MODIFIED_DT` column has that value overwritten.

Every table the framework writes also keeps `delta.dataSkippingStatsColumns` (set by `DeltaTableConfig`): Delta's default first 32 leaf columns, plus `__EXPORT_DATE` and `__ANCHOR_DT` wherever they sit, so filters on them can skip files. The property replaces Delta's default rather than adding to it, so it is recomputed after any write that evolves the schema. It also keeps the comments and tags from the task's `table_config`, and a comment on each of the columns above, as Unity Catalog metadata ([04_policies.md](04_policies.md#8-table-comments-and-tags)).

Each verb reports the rows it wrote as one KPI event (`source = KPI` in the audit log; names in `observability/kpi.py`). The names follow the verb, not the layer: a verb can write any layer, and the audit row's catalog, schema and table already say which.

| Verb | KPI event |
|---|---|
| APPEND | `rows_appended` |
| FULL | `rows_overwritten` |
| UPSERT | `rows_upserted` |
| SCD2, COMPLETE_DELTA | `rows_historized` |
| COMPLETE_DELTA deletes feed | `rows_retired` |

---

## 1. APPEND

> *Get the latest from a layer and load it into the next.* Typical: Inbound→Bronze. Also valid Bronze→Silver.

- **Requires**: target only.
- **Semantics**: write incoming records to the target with Delta `append` (with `mergeSchema` when schema evolution is enabled). No keys, no history columns beyond what the Pipeline added.
- **Increments**: checkpoint; for a table we didn't create, `full_read` (each read lands as one more snapshot) or `delta_read` (only the rows changed since the last read).

## 2. FULL

> *Snapshot and overwrite.* Typical: reference tables, full-refresh feeds.

- **Requires**: target only.
- **Semantics**: Delta `overwrite` of the target with the current dataset (with `mergeSchema`). Empty input → **skip** with an audit log entry. A source that produced nothing is a run with no news, not an instruction to empty the table.
- **Current dataset = newest export**: a backlog can hand one batch several exports. Only the rows carrying the batch's latest `__EXPORT_DATE` are written; the older exports are superseded, never unioned in.
- **Newer always wins**: a batch whose export is older than the target's `max(__EXPORT_DATE)` is **skipped** with an audit log entry. Yesterday's snapshot never overwrites today's.
- **Increments**: checkpoint, or `full_read`: each read is the current dataset, and an empty one is still skipped. Not `delta_read`, which would replace the target with only the changed rows.

## 3. UPSERT — *new in the rewrite* (SCD Type 1)

> *Latest state per key, no history.* Typical: Bronze→Silver when history isn't needed.

- **Requires**: `output.keys`. Optional: `output.event_time.*` (when present, "newer wins" uses it; otherwise last write wins), `output.dedup.*` (on by default — without `output.event_time`, set `output.dedup.order_by` or `output.dedup.enabled: false`).
- **Semantics**: Delta `MERGE` on the keys —
  - matched → update all columns, only if the `__RECORD_HASH` differs (and, when `event_time` is configured, the source is newer);
  - not matched → insert.
- No `__START_DATE/__END_DATE/__CURRENT_FLAG/__DELETED_FLAG` columns; `__LAST_MODIFIED_DT` is still maintained. A matched row the batch doesn't beat, or whose content is unchanged, is not written and keeps its stamp.
- Target absent → create via append.
- **Increments**: checkpoint, `full_read` or `delta_read`. Re-reading every key is safe: newer wins, and an unchanged row is a no-op.

## 4. SCD2

> *Latest from one layer to the next, history-tracked by keys and date.*

- **Requires**: `output.keys`, `output.event_time.column` (+ `.format` if it is a string column).
- **Optional**: `output.dedup.*`.
- **Changes only**: every batch is a set of changes. A key the batch does not mention is left untouched — its absence is never read as a deletion. `snapshot_scope: full` is rejected: expiring and re-inserting every record on each load would turn Silver into a duplicate of Bronze. A source whose complete snapshot *is* the truth belongs on FULL (no history) or COMPLETE_DELTA with `snapshot_scope: full` (history kept).
- **No deletes feed**: SCD2 runs per batch, and a stream has no way to cut a second source at the same point as its updates. A source that sends deletes separately belongs on COMPLETE_DELTA.
- **Increments**: checkpoint (each micro-batch flows through the algorithm below), `watermark`, `full_read` or `delta_read`. The anti-filter (step 5b) makes a repeated full read open versions only for rows whose content changed.

**Algorithm** (per batch):

1. Optional rename patterns; event-time/dedup-column normalization to timestamp. These normalized columns are held internally and never persisted, so the target schema stays the one the source defines.
2. Dedup (on by default, before the policy gate): keep the latest row per key (`output.keys`), ordered by event time desc, ties broken by `__EXPORT_DATE` desc (the newer export wins). `dedup.columns` / `dedup.order_by` override either.
3. Stamp `__LAST_MODIFIED_DT` with the run's write time.
4. Target absent → create with metadata init (`__START_DATE` = event_time or now, `__END_DATE` = NULL, flags Y/N).
5. Target present:
   a. **Schema check**: a column the target lacks is refused (`unexpected_columns`) unless `schema_evolution` adds it. It runs before anything commits: steps b and c are separate commits, so an insert refused after the close would leave those keys with no current row.
   b. **Anti-filter**: drop source rows whose key's current row has the same `__RECORD_HASH` (an unchanged record opens no version, whatever its event time), or an event_time ≥ theirs (idempotent re-runs, late/duplicate files are no-ops). Only the current row counts: a key that was closed and comes back unchanged opens a new version.
   c. **Close**: Delta merge — matched current, not-deleted rows with older event_time get `__END_DATE` = source event_time, `__CURRENT_FLAG` = 'N', `__LAST_MODIFIED_DT` = the run's write time.
   d. **Insert**: surviving source rows appended as new current rows.

Note: SCD2 collapses to *latest per key within the processed increment* (step 2). If the increment contains v1→v2→v3 of the same key, Silver records the transition current-state → v3. A backlog split across several micro-batches leaves one history row per batch, and the current row is always the newest. When **every** intermediate version must appear in history, use COMPLETE_DELTA.

## 5. COMPLETE_DELTA

> *Everything not yet promoted, replayed step by step — every evolution of the data is represented in Silver.*

- **Requires**: table origin (`source.table` = updates table), `output.keys`, `output.event_time.column`.
- **Optional**: deletes feed (§7), `output.dedup.*` (applied per snapshot), `output.snapshot_scope` (§6).
- **Increments**: watermark — only source rows with `__EXPORT_DATE > max(target.__EXPORT_DATE)` (a typed timestamp comparison, not a string one; the watermark defaults to 1900-01-01 when the target is empty or absent). `source.increment_anchor: true` swaps `__EXPORT_DATE` for `__ANCHOR_DT`, the per-record date Bronze stamped; not with `snapshot_scope: full` (§6). Or `full_read`: each run is one complete snapshot, and the deletes table, if any, shares its stamp. Or `delta_read`, under `snapshot_scope: delta` only: each run is one partial snapshot. Under `snapshot_scope: full` the source must hold whole exports ([08_usage.md](08_usage.md) §2).
- **Ordering contract**: exports must reach Silver in order. One that lands after a newer export was promoted is behind the watermark and is never read; reloading it is a manual step. The file-name timestamp is a snapshot's only identity, so an export split across files must stamp every part identically and deliver them all to the same run: parts with different stamps are separate exports (under `snapshot_scope: full` the last one supersedes the rest), and a part that arrives after its siblings were promoted sits on the watermark and is skipped.

**Algorithm**:

1. Read updates (and deletes, if configured) newer than the watermark.
2. Split the increment into **snapshots** by `__EXPORT_DATE`, which Bronze parsed from each file name. Each snapshot is cut with a filter on the column itself, so its merge skips the other exports' files.
3. Order snapshots chronologically (under `snapshot_scope: full`, keep only the newest export onward, §6); **for each snapshot, in order**: run the SCD2 merge (§4 steps 1–5) for its updates, then apply its deletes (§7).
4. No snapshots + missing target → create the empty target (schema from source).

### Worked example — why replay matters

Source exports three files for table `SUBJECTS` (key `ID`, event time `UPDATEDTIME`):

| File (snapshot) | Content |
|---|---|
| `subjects_20260101120000.csv` | `A` v1 (`UPDATEDTIME` 2026-01-01), `B` v1 |
| `subjects_20260102120000.csv` | `A` v2 (2026-01-02) — and deletes file: `B` (deleted 2026-01-02) |
| `subjects_20260103120000.csv` | `A` v3 (2026-01-03) |

All three land in Bronze before the Silver task runs (a backlog — weekend, reprocessing, new table). COMPLETE_DELTA replays snapshot 1, then 2, then 3. Silver afterwards:

| ID | payload | __START_DATE | __END_DATE | __CURRENT_FLAG | __DELETED_FLAG |
|----|---------|--------------|------------|----------------|----------------|
| A | v1 | 2026-01-01 | 2026-01-02 | N | N |
| A | v2 | 2026-01-02 | 2026-01-03 | N | N |
| A | v3 | 2026-01-03 | NULL | Y | N |
| B | v1 | 2026-01-01 | 2026-01-02 | N | **Y** |

Every evolution is represented: A's full version chain with correct validity windows, and B's life-and-deletion. Plain SCD2 over the same backlog would dedup to the latest per key and produce only `A v3 (current)` — A's v1→v2 transitions and B's existence would never reach Silver. **This is the "everything from one layer to the next, not only the very latest" requirement.**

## 6. Modifier: `snapshot_scope` (COMPLETE_DELTA)

- **`delta`** (default): the source sends only changes. Records absent from an increment are simply untouched.
- **`full`**: the source sends the complete dataset — typically a monthly or on-demand export that supersedes whatever Silver holds, while history stays. Each full export supersedes every one before it, so of the pending exports only the **newest** is replayed; the older ones stay in Bronze, which is where export-by-export history lives. Deletes stamped after that export still apply. Before merging, every current row the snapshot **omits or changed** is expired (`__CURRENT_FLAG`='N', `__END_DATE` = the export's timestamp from its file name, `__LAST_MODIFIED_DT` = the run's write time), in one merge. A changed record is expired whatever its event time, and the merge re-inserts it; an unchanged one stays current and untouched; anything absent stays expired. This is how implicit deletes work for full-snapshot sources.
- Closing at the export's time rather than the run's keeps windows honest however late the job runs: a dropped record closes at the latest moment it can have left the source, and with `output.event_time.column: __EXPORT_DATE` a re-asserted record reopens at exactly the instant its previous version closed. A per-record event time reopens it at its own, earlier date instead, and the two windows overlap.
- A full export must be read whole, so `source.increment_anchor` is rejected under `full` at Start: the anchor would hand the merge only the records that moved, and every record the export still carries unchanged would be expired.
- Re-run with no new data: nothing is expired and nothing merged — the watermark and the anti-filter together make the whole run a no-op. Re-running a job must never change the table.
- **Full + delta feeds** from one source run as two tasks into the same Silver, the full one first ([08_usage.md](08_usage.md) §2).
- SCD2 does not take this modifier (§4).

### Worked example

Target has current rows `A, B, C`. A `snapshot_scope: full` load arrives containing `A (unchanged), B (changed), D (new)`:

| ID | Outcome |
|----|---------|
| A | untouched: same `__RECORD_HASH`, so it stays the current row |
| B | old row expired; new current row with changed payload |
| C | expired at the export's timestamp, **no** new row — implicitly deleted (stays `__DELETED_FLAG='N'`, it simply has no current version) |
| D | new current row |

## 7. Modifier: deletes feed (COMPLETE_DELTA)

A second Delta table (`source.deletes_table`) carrying delete records, with its own `output.deletes.keys` and `output.deletes.event_time.*` — all three are required together. Applied after the updates of the same snapshot. Soft-delete semantics, via a double merge:

1. every key match in the target → `__DELETED_FLAG` = 'Y' (all history rows of the entity are flagged), `__LAST_MODIFIED_DT` = the run's write time;
2. the open current row (`__END_DATE IS NULL AND __CURRENT_FLAG='Y'`) → `__END_DATE` = delete event time, `__CURRENT_FLAG` = 'N', `__LAST_MODIFIED_DT` = the run's write time.

No physical deletes, ever — history is preserved.

## 8. Step records: the recipe for a table's state

Every verb logs each step it commits to the target as `step_promoted` audit records, one per source (`__SOURCE`) the step applied. Read in order, they are the recipe for the target's state on any date: the latest `REPLACE` step at or before that date, then every step after it. The framework delivers the recipe; replaying it is a data engineering task.

| Verb | One step | `mode` |
|---|---|---|
| APPEND | one batch or micro-batch | `APPEND` |
| FULL | one overwrite with the batch's newest export | `REPLACE` |
| UPSERT, SCD2 | one batch or micro-batch | `MERGE` |
| COMPLETE_DELTA | one snapshot: its updates, then its deletes | updates: `REPLACE` under `snapshot_scope: full`, else `MERGE`; deletes: `MERGE` |

- **The record**: `name` = `step_promoted`, `total` = the target's Delta version after the step, `time_stamp` = when it was processed, `metadata` = `{"verb", "mode", "feed", "source", "version"}`. `feed` is `updates` or `deletes`; only COMPLETE_DELTA has a deletes feed.
- **`version` groups a step.** Records with the same version are one step and are replayed together: SCD2 and UPSERT deduplicate per batch, so a step's sources applied one at a time give a different result. A step lists every source handed to the write, including one whose rows changed nothing.
- **No change, no step.** A skipped write (FULL with empty or older input), a rerun with nothing new, and an export a newer one superseded leave no record.
- **Written as soon as the step commits**, not at the end of the run. A run that fails partway leaves records for the steps it finished and none for the rest; a COMPLETE_DELTA snapshot whose deletes never ran is half-applied and has none.
- **Code and config**: each record carries `__flame_version`, and its `__task_run_id` leads to the run's `pipeline_start` record, whose metadata holds the task's parameters.
- **State means current rows.** The recipe rebuilds the rows current on that date, not the history before the `REPLACE`. A target with no `REPLACE` step lists every step since its first, so the list can be long.

### The state on a date

```sql
-- The steps that built a target as it stood at :as_of, in the order to apply them.
WITH steps AS (
  SELECT
    CAST(get_json_object(metadata, '$.version') AS BIGINT) AS version,
    get_json_object(metadata, '$.mode') AS mode,
    get_json_object(metadata, '$.feed') AS feed,
    get_json_object(metadata, '$.source') AS source,
    time_stamp AS processed_dt
  FROM monitoring_dev.audit.flame_logs
  WHERE name = 'step_promoted'
    AND catalog = :catalog AND `schema` = :schema AND `table` = :table
    AND time_stamp <= :as_of
),
start AS (
  SELECT coalesce(max(version), -1) AS version FROM steps WHERE mode = 'REPLACE'
)
SELECT steps.version, mode, feed, source, processed_dt
FROM steps, start
WHERE steps.version >= start.version
ORDER BY steps.version, feed DESC, source
```

| version | mode | feed | source | processed_dt |
|---|---|---|---|---|
| 42 | REPLACE | updates | FILE:/Volumes/.../SITE_2025_SEPT_01.csv | 2025-09-03 05:00:01 |
| 45 | MERGE | updates | FILE:/Volumes/.../SITE_2025_SEPT_25.csv | 2025-09-26 05:00:01 |
| 48 | MERGE | updates | FILE:/Volumes/.../SITE_2025_OCT_10.csv | 2025-10-11 05:00:01 |

A table that was dropped and recreated starts its versions again: add `AND time_stamp >= <its creation>`.

### Rolling back a source

The last healthy version before a source was promoted is the previous step's version in the same run, or the run's `promotion_start` version when it was the run's first step. Versions between two steps are half-applied and are never a restore target.

```sql
-- The version to restore so the target stands as it did before :source.
WITH audit AS (
  SELECT __task_run_id, name, total,
         get_json_object(metadata, '$.source') AS source
  FROM monitoring_dev.audit.flame_logs
  WHERE catalog = :catalog AND `schema` = :schema AND `table` = :table
    AND name IN ('promotion_start', 'step_promoted')
),
step AS (
  SELECT __task_run_id, total AS version FROM audit
  WHERE name = 'step_promoted' AND source = :source
  ORDER BY version
  LIMIT 1
)
SELECT coalesce(
  (SELECT max(a.total) FROM audit a, step s
   WHERE a.name = 'step_promoted' AND a.__task_run_id = s.__task_run_id
     AND a.total < s.version),
  (SELECT a.total FROM audit a, step s
   WHERE a.name = 'promotion_start' AND a.__task_run_id = s.__task_run_id)
) AS restore_version
```

Restore with `RESTORE TABLE <target> TO VERSION AS OF <restore_version>`. A NULL means the run created the table: drop it instead. What a restore does not undo:

- **The next run, by increment strategy.** `watermark` and `delta_read` follow the target, so the next run reads every later step again, the bad source included; fix or remove it in Bronze first. `full_read` reads the source table whole again. `checkpoint` has already moved past those steps: the next run does not read them again, and they stay missing until the checkpoint is reset.
- **Bronze** still holds the source's rows.
- **Downstream tables** keep the data they already took from the target.

Which verb to choose for a source, and with which increment strategy: [08_usage.md](08_usage.md).
