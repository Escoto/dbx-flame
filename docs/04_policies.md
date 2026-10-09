# 04 — Policies (Data Quality)

## 1. Scope

The Policies layer is a **gate**. It evaluates data quality rules against the dataset
produced by Pipeline + Typing, *before* the Output layer writes it, and either lets the
batch through or refuses it.

The rules themselves are not this framework's business. Evaluation is delegated to
[Databricks DQX](https://databrickslabs.github.io/dqx/): the framework hands DQX a
DataFrame and a ruleset, and reads back the verdict. Which checks exist, what arguments
they take and how to express them are DQX's documentation to write, not ours — see the
[DQX check reference](https://databrickslabs.github.io/dqx/docs/reference/quality_checks/).

These are **user policies**: rules about the business data, opted into per task. They are
distinct from **platform policies**, the framework's own checks, which a task can't switch
off ([§9](#9-platform-policies)).

This reverses an earlier native-first design. A hand-rolled rule framework would have
shipped two checks plus a registry to grow more; DQX ships several dozen validated checks
on the first day, for one dependency.

## 2. Configuration

One task parameter points at a ruleset:

```yaml
policies.checks_file: /Volumes/.../checks/SUBJECTS.yml   # optional
```

With no `policies.checks_file`, the layer does nothing at all.

The ruleset is a detached per-table YAML in DQX's own format, referenced by path — exactly
as `typing.cast_config` holds column types:

```yaml
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: ID
- criticality: warn
  check:
    function: is_unique
    arguments:
      columns: [ID]
```

Severity lives on the rule rather than in the workflow config. It is a property of the
check, and DQX already models it as `criticality`.

## 3. How a run uses it

1. **Start** reads the ruleset and calls DQX's `validate_checks`. An unknown check
   function, a misspelled argument or a missing required parameter fails the task here,
   before anything is read.
2. **Between dedup and Output** the runner applies the checks, which appends DQX's
   `_errors` / `_warnings` result columns to the batch.
3. The runner aggregates those columns in one pass into per-check counts and logs each
   result to the audit table. The checked DataFrame never leaves the runner: `run()` hands
   back nothing and the caller writes the frame it already held, so the result columns
   **cannot** reach the target. That is structural, not a `drop()` call that could be
   removed by accident.
4. If any row carries an `error`, the runner raises `PolicyViolation` — after logging
   everything, so one run reports every rule the batch broke rather than the first.

### Severity mapping

DQX's `criticality` describes which DataFrame a row lands in when checks are *split*. This
layer does not split, so it reinterprets:

| DQX `criticality` | Here |
|---|---|
| `warn` | logged as `WARNING`; the batch is written |
| `error` | logged as `ERROR`; the whole batch is discarded and the run stops |

### Streaming

For streaming origins the runner executes inside the same `foreachBatch` as Typing and
Output, per micro-batch.

A micro-batch that trips an `error` check is discarded **whole**. `PolicyViolation` is
raised before the writer is ever called, so nothing from that batch reaches the target —
not the rows that passed, not a partial write — and its offsets are never committed to the
checkpoint.

The exception then propagates out of `foreachBatch` and terminates the streaming query, so
no later micro-batch is read or evaluated: the run stops at the first batch that fails
rather than pressing on with the rest of the backlog. It surfaces to the entry point as a
`StreamingQueryException` wrapping the `PolicyViolation`, which records a
`pipeline_failure` audit row and re-raises, failing the task.

Micro-batches that already succeeded earlier in the same run stay committed — see
[§6](#6-known-limitation--a-failed-batch-does-not-unwind-earlier-ones).

DQX's engine requires a Databricks `WorkspaceClient`, which is not available inside
`foreachBatch`. The ruleset is therefore read and validated **once on the driver** before
the stream starts, and only the evaluation — which DQX supports without workspace access —
runs inside the closure.

## 4. Audit log shape

Policy results reuse the audit contract (`` `monitoring_{env}`.`audit`.`logs` ``):

| Field | Value |
|---|---|
| `type` | `WARNING` / `ERROR` per criticality |
| `source` | `Policies` |
| `name` | the check name DQX reports |
| `total` | offending row count |
| `description` | DQX's own message for the check |

One row per check that failed. A batch that broke nothing logs a single `INFO`
`policies_passed` instead of a row per check: DQX names its checks itself, and
reconstructing those names just to report that nothing happened would couple the
framework to how DQX derives them.

## 5. Schema drift is not a policy

There is no native rule here, because nothing native is needed.

A **new** column is already handled by the task-level `schema_evolution` knob, which
drives Auto Loader on the read and `mergeSchema`/`autoMerge` on the write
([02_config_schema.md](02_config_schema.md) §4). Adding a rule for it would duplicate a
mechanism that already refuses loudly.

A **missing** column is a genuine gap: Delta accepts the batch and writes NULLs, at either
`mergeSchema` setting, and no `schemaEvolutionMode` value reports it — every mode in that
option is about columns appearing, not disappearing.

That gap is accepted deliberately, because the ruleset already covers the case that
matters. A column worth protecting has a check naming it, and DQX reports a check whose
column has vanished rather than skipping it quietly:

```
_errors: [{name: "name_is_null", skipped: true,
           message: "Check evaluation skipped due to invalid check columns: ['NAME']"}]
```

At `criticality: error` that refuses the batch, naming the column. A column with no check
on it is, by definition, one nobody asked to protect.

## 6. Known limitation — a failed batch does not unwind earlier ones

A streaming source is written micro-batch by micro-batch. If batch 5 of N trips an `error`
check, batches 1–4 are already committed and the Auto Loader checkpoint has advanced past
them. The task fails and the run goes red, but the target holds a partial load.

This is accepted for now. Re-running resumes at the offending batch and fails again until
the data is fixed, which is the safe direction to fail in. Unwinding the committed batches
with Delta time travel is the intended fix and is not built.

## 7. Future — quarantine

DQX can split a batch into valid and invalid rows instead of refusing it whole
(`apply_checks_by_metadata_and_split`). That is a nice-to-have rather than today's need: it
introduces a second write target and the configuration to describe it, where a gate
introduces neither. The runner is the only place that would change.

## 8. Table tagging

`output.tags.<name>: <value>` keeps Unity Catalog tags on the target. `DeltaTableConfig`
applies them after every write through `observability/tagging.py`: it sets only the tags
whose value changed, leaves tags it doesn't name alone, and logs a `table_not_tagged`
warning when the target doesn't exist. It is governance metadata, not a dataset rule, so it
sits outside the Policies layer.

## 9. Platform policies

Platform policies protect what the framework itself relies on: watermarks, merges and
history. They aren't configurable, and they're listed in one place, `PlatformPolicy` in
`policies/platform.py`.

| | Platform policies | User policies |
|---|---|---|
| Purpose | Contracts the framework depends on | Rules about the business data |
| Defined by | The framework | The task's DQX ruleset (`policies.checks_file`) |
| Can a task switch them off? | No | Yes: no ruleset, no checks |
| Severity | Always fail the batch | `warn` or `error`, per check |

The platform policies, named as the audit row a violation writes:

| Policy | Fails the batch when |
|---|---|
| `unstamped_file` | a file name doesn't carry the agreed export stamp |
| `anchor_column_missing` | `source.anchor_dt.column` isn't in the batch |
| `anchor_unparseable` | a `source.anchor_dt` value doesn't parse as a timestamp |
| `anchor_not_stamped` | an anchored read meets a table without `__ANCHOR_DT` |
| `cast_silent_null` | a cast turns a present value into NULL |
| `empty_source_schema` | a new target would be created from a batch with no columns |
| `unexpected_columns` | a batch carries a new column and `schema_evolution` doesn't add it |
| `event_time_invalid` | a date a keyed verb compares is missing, blank or doesn't parse |
| `malformed_table` | a source table carries one of `__SOURCE` and `__EXPORT_DATE` but not the other |
| `unstamped_table` | a checkpoint or watermark read meets a table with neither: only `full_read` may stamp one |
| `stamped_full_read` | a `full_read` meets a table we stamped, whose exports it would load again |

The last three run on the table origin's read, before anything is read from it, and apply
to its deletes table too ([02_config_schema.md](02_config_schema.md) has the full rule).

`event_time_invalid` covers `output.event_time`, `output.dedup.order_by` and, on
COMPLETE_DELTA, `output.deletes.event_time`. Only UPSERT, SCD2 and COMPLETE_DELTA check it,
because only they compare dates; a NULL there would otherwise open a second current row,
skip an update or let dedup keep an arbitrary row.

Each check runs where its data is first available. For a batch, the order is:

```
read (malformed_table · unstamped_table · stamped_full_read) → unstamped_file → prepare()
  → event_time_invalid → dedup → user policies (DQX) → writer
```

Dedup runs after `event_time_invalid`, because it orders rows by those dates: a row
whose date doesn't parse would sort last and be dropped quietly. It runs before the
user policies, so they judge exactly the rows the writer receives, and a key the batch
legitimately sends twice doesn't fail `is_unique`.

COMPLETE_DELTA replays a backlog one snapshot at a time, so it dedups each snapshot on
its own and runs the user policies once per snapshot it replays, all before its first
merge. Each audit row names its snapshot. Under `snapshot_scope: full`, superseded
exports are never replayed, so they are never judged.

A violation writes an `ERROR` audit row, flushes the audit log, then raises
`PlatformPolicyViolation`. Nothing from the batch is written.
