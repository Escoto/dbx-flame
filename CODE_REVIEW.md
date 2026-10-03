# Code Review — Production Gaps

Mismatches between the docs (00–06) and the code, plus problems likely to show up in
production. Open GitHub issues (#2–#12) are left out. ✔ marks findings reproduced with
a throwaway local Spark test.

| No. | Name | Description | Severity |
|---|---|---|---|
| 1 | ~~SCD2 commits the close before an insert that can fail ✔~~ | **Tracked in gh #22** (new-column cause; type drift is left to the data engineer owning the cast config). ~~Closing superseded rows (and COMPLETE_DELTA's full-scope expiry) commits before the insert. If the insert fails every time (a new column under the default `fail_on_new_columns`, or type drift), those keys have no current row. Under COMPLETE_DELTA `snapshot_scope: full` the whole table has none, and retries fail the same way.~~ | ~~High~~ |
| 2 | ~~NULL or unparseable event time silently corrupts history ✔~~ | **Tracked in #18.** ~~`as_timestamp` returns NULL for a missing value or a format mismatch, and nothing rejects it. SCD2 then skips both the anti-filter and the close and opens a second current row. UPSERT skips the update and dedup keeps an arbitrary row.~~ | ~~High~~ |
| 3 | Default `multiline: true` reads one row per JSON Lines file ✔ | The framework flips Spark's JSON default to multi-line. A JSON Lines export then silently yields only its first record. | Mid |
| 4 | `add_new_columns` fails every run that meets a new column | Auto Loader deliberately stops the stream when it adds a column and expects a restart. Neither the framework nor the workflow YAML retries, and the docs only say the target grows. | Mid |
| 5 | Cast config is validated per batch, not at Start | The cast YAML is read inside `prepare()`, so a bad path, unknown key or invalid type fails only after data is read, and an idle run never checks it. The DQX ruleset, by contrast, is validated at Start. | Mid |
| 6 | ~~COMPLETE_DELTA recomputes the pending slice for every snapshot~~ | **Tracked in gh #3**, which caches every batch once in `_gate_and_write`. ~~The increment is never persisted, so each snapshot's merge re-runs prepare() over the pending rows.~~ | ~~Mid~~ |
| 7 | Fan-in tasks share one checkpoint and schema location | Both paths are derived from the target table only. Two tasks writing one target (e.g. two inbound folders into one Bronze table) share streaming offsets and the inferred schema. | Mid |
| 8 | `__SILVER_LAST_MODIFIED_DT` misses closes, expiries and delete flags | The docs define it as the last framework write to touch the row, but the close, full-scope expiry and delete-flag merge don't update it. Incremental readers keyed on it miss those changes. | Mid |
| 9 | Options irrelevant to the verb pass as silent no-ops | `output.dedup.*`, `output.keys` and `output.event_time` on APPEND/FULL, CSV options on JSON (even `escape`, which the docs say applies) all validate and do nothing. The loader already refuses this pattern for `envelope_fields`, and for `snapshot_time_pattern` and `anchor_dt` on a delta origin. | Mid |
| 10 | Documented table tagging is never invoked | `apply_tags` has no caller and no config key. It also returns silently when the table is missing, where the docs promise a warning. | Mid |
| 11 | KPI names in docs differ from what writers emit | The docs promise `bronze_new_records` / `silver_new_records` for APPEND, but it emits `rows_appended`; FULL and UPSERT emit `rows_overwritten` and `rows_upserted`. The constants in `kpi.py` are unused. | Mid |
| 12 | `flatten_nested` and JSON schema logging are untracked P6 gaps | `flatten_nested` is registered, passes Start validation, then raises `NotImplementedError`. The JSON tree-schema logging from the roadmap doesn't exist, and neither gap has an issue. | Mid |
| 13 | Config validation failures leave no audit row | `load_config` and `build_context` raise before the logger exists. A misconfigured run therefore leaves nothing in the audit table, although the README says every run writes to it. | Mid |
| 14 | ~~Inbound glob misses uppercase extensions and subfolders~~ | ~~`*.csv` is case-sensitive and flat, so `.CSV` files and dated subfolders are skipped. The run still succeeds with nothing ingested.~~ **Fixed:** the extension matches in any case, and `__EXPORT_DATE` is read from the file name only. **Won't implement:** subfolders. | ~~Low~~ |
