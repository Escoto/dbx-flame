# 03 — Write Verbs

The Output layer writes with one of five verbs. Verbs are **layer-agnostic**: any origin can pair with any verb whose declared requirements are satisfied (matrix in [02_config_schema.md](02_config_schema.md) §3). All Delta writes preserve the metadata contract below.

## 0. Metadata contract (all history-tracking verbs)

| Column | Set by | Meaning |
|---|---|---|
| `__EXPORT_DATE` | Pipeline | Source export timestamp (from the file name, per `source.snapshot_time_pattern`) |
| `__FILEPATH` | Pipeline | Source file the record arrived from |
| `__ANCHOR_DT` | Pipeline | `source.anchor_dt` as a timestamp (only when configured) |
| `__BRONZE_LAST_MODIFIED_DT` | Pipeline | Bronze ingest time (dropped on promotion) |
| `__SILVER_LAST_MODIFIED_DT` | Output | Last framework write touching the record |
| `__START_DATE` | Output | Validity start of the history row |
| `__END_DATE` | Output | Validity end (NULL = open) |
| `__CURRENT_FLAG` | Output | `'Y'` current version / `'N'` historical |
| `__DELETED_FLAG` | Output | `'Y'` soft-deleted entity |

Every table the framework writes also keeps `delta.dataSkippingStatsColumns` (set by `DeltaTableConfig`): Delta's default first 32 leaf columns, plus `__EXPORT_DATE` and `__ANCHOR_DT` wherever they sit, so filters on them can skip files. The property replaces Delta's default rather than adding to it, so it is recomputed after any write that evolves the schema. It also keeps the task's `output.tags` as Unity Catalog tags.

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
- **Increments**: file origins via Auto Loader checkpoint; table origin via streaming checkpoint.

## 2. FULL

> *Snapshot and overwrite.* Typical: reference tables, full-refresh feeds.

- **Requires**: target only.
- **Semantics**: Delta `overwrite` of the target with the current dataset (with `mergeSchema`). Empty input → **skip** with an audit log entry. A source that produced nothing is a run with no news, not an instruction to empty the table.
- **Current dataset = newest export**: a backlog can hand one batch several exports. Only the rows carrying the batch's latest `__EXPORT_DATE` are written; the older exports are superseded, never unioned in.
- **Newer always wins**: a batch whose export is older than the target's `max(__EXPORT_DATE)` is **skipped** with an audit log entry. Yesterday's snapshot never overwrites today's.

## 3. UPSERT — *new in the rewrite* (SCD Type 1)

> *Latest state per key, no history.* Typical: Bronze→Silver when history isn't needed.

- **Requires**: `output.keys`. Optional: `output.event_time.*` (when present, "newer wins" uses it; otherwise last write wins), `output.dedup.*` (on by default — without `output.event_time`, set `output.dedup.order_by` or `output.dedup.enabled: false`).
- **Semantics**: Delta `MERGE` on the keys —
  - matched → update all columns (when `event_time` configured: only if source is newer);
  - not matched → insert.
- No `__START_DATE/__END_DATE/__CURRENT_FLAG/__DELETED_FLAG` columns; `__SILVER_LAST_MODIFIED_DT` is still maintained.
- Target absent → create via append.

## 4. SCD2

> *Latest from one layer to the next, history-tracked by keys and date.*

- **Requires**: `output.keys`, `output.event_time.column` (+ `.format` if it is a string column).
- **Optional**: `output.dedup.*`.
- **Changes only**: every batch is a set of changes. A key the batch does not mention is left untouched — its absence is never read as a deletion. `snapshot_scope: full` is rejected: expiring and re-inserting every record on each load would turn Silver into a duplicate of Bronze. A source whose complete snapshot *is* the truth belongs on FULL (no history) or COMPLETE_DELTA with `snapshot_scope: full` (history kept).
- **No deletes feed**: SCD2 runs per batch, and a stream has no way to cut a second source at the same point as its updates. A source that sends deletes separately belongs on COMPLETE_DELTA.
- **Increments**: table origin with streaming checkpoint (each micro-batch flows through the algorithm below); file origins supported the same way.

**Algorithm** (per batch):

1. Optional rename patterns; event-time/dedup-column normalization to timestamp. These normalized columns are held internally and never persisted, so the target schema stays the one the source defines.
2. Dedup (on by default, before the policy gate): keep the latest row per key (`output.keys`), ordered by event time desc, ties broken by `__EXPORT_DATE` desc (the newer export wins). `dedup.columns` / `dedup.order_by` override either.
3. Add `__SILVER_LAST_MODIFIED_DT`; drop `__BRONZE_LAST_MODIFIED_DT`.
4. Target absent → create with metadata init (`__START_DATE` = event_time or now, `__END_DATE` = NULL, flags Y/N).
5. Target present:
   a. **Schema check**: a column the target lacks is refused (`unexpected_columns`) unless `schema_evolution` adds it. It runs before anything commits: steps b and c are separate commits, so an insert refused after the close would leave those keys with no current row.
   b. **Anti-filter**: drop source rows whose event_time ≤ the target's current row's event_time for the same keys (idempotent re-runs, late/duplicate files are no-ops).
   c. **Close**: Delta merge — matched current, not-deleted rows with older event_time get `__END_DATE` = source event_time, `__CURRENT_FLAG` = 'N'.
   d. **Insert**: surviving source rows appended as new current rows.

Note: SCD2 collapses to *latest per key within the processed increment* (step 2). If the increment contains v1→v2→v3 of the same key, Silver records the transition current-state → v3. A backlog split across several micro-batches leaves one history row per batch, and the current row is always the newest. When **every** intermediate version must appear in history, use COMPLETE_DELTA.

## 5. COMPLETE_DELTA

> *Everything not yet promoted, replayed step by step — every evolution of the data is represented in Silver.*

- **Requires**: table origin (`source.table` = updates table), `output.keys`, `output.event_time.column`.
- **Optional**: deletes feed (§7), `output.dedup.*` (applied per snapshot), `output.snapshot_scope` (§6).
- **Increments**: watermark — only source rows with `__EXPORT_DATE > max(target.__EXPORT_DATE)` (a typed timestamp comparison, not a string one; the watermark defaults to 1900-01-01 when the target is empty or absent). `source.increment_anchor: true` swaps `__EXPORT_DATE` for `__ANCHOR_DT`, the per-record date Bronze stamped; not with `snapshot_scope: full` (§6).
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
- **`full`**: the source sends the complete dataset — typically a monthly or on-demand export that supersedes whatever Silver holds, while history stays. Each full export supersedes every one before it, so of the pending exports only the **newest** is replayed; the older ones stay in Bronze, which is where export-by-export history lives. Deletes stamped after that export still apply. Before merging, **all** current rows in the target are expired (`__CURRENT_FLAG`='N', `__END_DATE` = the export's timestamp from its file name). The merge then re-inserts what the snapshot contains — anything absent stays expired. This is how implicit deletes work for full-snapshot sources.
- Closing at the export's time rather than the run's keeps windows honest however late the job runs: a dropped record closes at the latest moment it can have left the source, and with `output.event_time.column: __EXPORT_DATE` a re-asserted record reopens at exactly the instant its previous version closed. A per-record event time reopens it at its own, earlier date instead, and the two windows overlap.
- A full export must be read whole, so `source.increment_anchor` is rejected under `full` at Start: the anchor would hand the merge only the records that moved, and every record the export still carries unchanged would be expired.
- Re-run with no new data: nothing is expired and nothing merged — the watermark and the anti-filter together make the whole run a no-op. Re-running a job must never change the table.
- **Full + delta feeds**: a source that sends both runs them as two tasks into the same Silver — the full task (`snapshot_scope: full`) first, then the delta task. The job's task dependencies enforce that order, not the framework. The full task reads on `__EXPORT_DATE`; the delta task usually anchors on a per-record modification date (`source.increment_anchor`, reading the `__ANCHOR_DT` both Bronze feeds stamp), so it applies only the changes made after what the newest full export already carries.
- SCD2 does not take this modifier (§4).

### Worked example

Target has current rows `A, B, C`. A `snapshot_scope: full` load arrives containing `A (unchanged), B (changed), D (new)`:

| ID | Outcome |
|----|---------|
| A | old row expired, identical new current row inserted (full-snapshot sources re-assert every record) |
| B | old row expired; new current row with changed payload |
| C | expired at the export's timestamp, **no** new row — implicitly deleted (stays `__DELETED_FLAG='N'`, it simply has no current version) |
| D | new current row |

## 7. Modifier: deletes feed (COMPLETE_DELTA)

A second Delta table (`source.deletes_table`) carrying delete records, with its own `output.deletes.keys` and `output.deletes.event_time.*` — all three are required together. Applied after the updates of the same snapshot. Soft-delete semantics, via a double merge:

1. every key match in the target → `__DELETED_FLAG` = 'Y' (all history rows of the entity are flagged);
2. the open current row (`__END_DATE IS NULL AND __CURRENT_FLAG='Y'`) → `__END_DATE` = delete event time, `__CURRENT_FLAG` = 'N', `__SILVER_LAST_MODIFIED_DT` = now.

No physical deletes, ever — history is preserved.

## 8. Verb selection guide

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
