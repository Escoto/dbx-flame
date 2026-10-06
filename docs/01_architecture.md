# 01 — Architecture

## 1. Package layout

```
dbx_flame/
├── docs/                            # this document set
├── pyproject.toml                   # wheel build; pytest / black / flake8 / mypy config
├── src/dbx_flame/
│   ├── context/                     # LAYER 1 — Start
│   │   ├── config.py                #   pydantic TaskConfig + nested models + enums
│   │   ├── loader.py                #   flat params → nested dict → TaskConfig (coercion, aggregation of errors)
│   │   └── context.py               #   Context: config + spark + run identity + logger + resolved names/paths
│   ├── pipelines/                   # LAYER 2 — Pipeline
│   │   ├── base.py                  #   SourcePipeline protocol
│   │   ├── csv_source.py            #   Auto Loader csv (also txt via file_extension)
│   │   ├── json_source.py           #   Auto Loader json
│   │   ├── sas_source.py            #   Auto Loader binaryFile discovery + pandas.read_sas
│   │   ├── delta_source.py          #   Delta table origin (checkpoint or watermark increments)
│   │   ├── preprocessors.py         #   registry: record_envelope
│   │   └── enrichment.py            #   provenance columns, column sanitization, rename patterns
│   ├── typecast/                    # LAYER 3 — Typing
│   │   ├── models.py                #   CastConfiguration (column specs, formats)
│   │   └── service.py               #   cast + single-pass silent-NULL validation
│   ├── policies/                    # LAYER 4 — Policies
│   │   ├── base.py                  #   PolicyResult, PolicyViolation, Severity
│   │   ├── checks.py                #   DQX ruleset: read + validate the detached YAML
│   │   └── runner.py                #   apply DQX checks, log, enforce error criticality
│   ├── output/                      # LAYER 5 — Output
│   │   ├── base.py                  #   Writer protocol + verb requirements declaration
│   │   ├── delta/
│   │   │   ├── append.py            #   APPEND
│   │   │   ├── full.py              #   FULL (overwrite)
│   │   │   ├── upsert.py            #   UPSERT (SCD1)
│   │   │   ├── scd2.py              #   SCD2 merge engine (close-and-insert)
│   │   │   └── complete_delta.py    #   snapshot replay orchestration on top of scd2.py
│   │   ├── table_config.py          #   DeltaTableConfig: table-level settings (statistics columns)
│   │   └── files.py                 #   FileWriter — interface only (implementation deferred)
│   ├── observability/
│   │   ├── audit_logger.py          #   buffered logger → `monitoring_{env}`.audit.logs
│   │   ├── kpi.py                   #   KPI event helpers
│   │   └── tagging.py               #   Unity Catalog table tagging
│   └── entrypoints/
│       ├── run.py                   #   single wheel entry point: params → Context → layers 2..5
│       └── pipeline.py              #   drives the configured origin into the configured verb
└── tests/                           # pytest + chispa; fixtures
```

## 2. Execution flow

One entry point serves every task. Which classes participate is decided entirely by config.

```mermaid
sequenceDiagram
    participant W as Workflow task (wheel)
    participant S as Start (context)
    participant P as Pipeline
    participant T as Typing
    participant Q as Policies
    participant O as Output

    W->>S: flat named parameters
    S->>S: coerce + validate (aggregate ALL errors)
    S-->>W: fail fast on invalid config
    S->>P: Context
    P->>P: origin reader → pre-processors → provenance → sanitize/rename
    P->>T: DataFrame
    T->>T: cast declared columns (others pass through) + NULL validation
    T->>Q: DataFrame
    Q->>Q: evaluate rules → audit log; raise on fail severity
    Q->>O: DataFrame
    O->>O: verb writer (append/full/upsert/scd2/complete_delta)
    O-->>W: KPI + audit flush, exit code
```

Notes:

- For **streaming origins** (csv/json/sas via Auto Loader) the chain Typing→Policies→Output runs inside `foreachBatch` per micro-batch; the layer boundaries are function boundaries, not job boundaries.
- For **COMPLETE_DELTA** the Output layer itself loops over snapshots (see [03_write_verbs.md](03_write_verbs.md)); Typing/Policies run once on the full increment before the loop.
- Every failure path flushes the audit log buffer before propagating the exception.

## 3. Layer contracts

### 3.1 Start → Context (`dbx_flame.context`)

**Input**: flat `key: value` task parameters (Databricks wheel-task named parameters; dotted keys express nesting, e.g. `source.options.delimiter`).
**Output**: a frozen `Context`:

```python
@dataclass(frozen=True)
class Context:
    config: TaskConfig            # validated pydantic model (02_config_schema.md)
    spark: SparkSession
    run: RunIdentity              # job_id, task_run_id (from Databricks job context)
    logger: AuditLogger           # bound to the resolved target table
    # resolved conveniences (all derived from config, computed once):
    catalog: str                  # f"{catalog}_{env}"
    source_table: str | None      # fq backticked name, delta origins
    deletes_table: str | None
    target_table: str             # fq backticked name
    inbound_glob: str | None      # file origins
    checkpoint_location: str
    schema_hints_location: str
    increment_strategy: IncrementStrategy   # delta origins; declared by the verb
```

Rules:

- **All validation happens here** — types, enums, required-parameter matrix for the configured origin × verb (see 3.5), unknown-key rejection. Errors are aggregated into one report and raised once (fail fast).
- No other layer may read raw parameters.

### 3.2 Pipeline (`dbx_flame.pipelines`)

```python
class SourcePipeline(Protocol):
    def read(self, ctx: Context) -> DataFrame: ...
```

- A "batch" here is simply the DataFrame in hand — an updates batch or a deletes batch; there is no wrapper type. `read` returns the updates; where a verb supports a deletes feed, `read_deletes` returns it (or `None`).
- File origins return an Auto Loader streaming DataFrame (`availableNow` semantics applied at write time); `sas` returns a batch-per-file iterator internally but exposes the same downstream flow.
- `delta` origin supports two increment strategies (declared by the verb): `checkpoint` (Spark streaming from the source table — used by SCD2/APPEND/FULL from Delta) and `watermark` (`__EXPORT_DATE > max(target.__EXPORT_DATE)` — used by COMPLETE_DELTA). It also exposes the optional **deletes feed** as a second DataFrame.
- **Pre-processors**: `source.preprocessors` is an ordered list of names resolved against a registry:

```python
class PreProcessor(Protocol):
    name: ClassVar[str]
    def apply(self, df: DataFrame, ctx: Context) -> DataFrame: ...

PREPROCESSORS = {"record_envelope": RecordEnvelope}
```

  `record_envelope` unwraps the common vendor JSON envelope (metadata:export_date + data[] → URI/DATA/EXPORT_DATE). There is deliberately no generic flattener: exploding nested arrays multiplies rows and denormalizes the data, so flattening a payload is left to its consumer. Adding a vendor shape = one registered class + config, no changes to `json_source`.
- **Enrichment** (shared, applied after pre-processors): provenance columns (`__bronze_last_modified_dt`, `__filePath`, `__EXPORT_DATE` from the file-name regex), column sanitization (replace ` ,;{}()=./`, tab and newline with `_`, uppercase, drop `_rescued_data`), optional structured rename patterns.

### 3.3 Typing (`dbx_flame.typecast`)

Validation is single-pass: the `_orig_*` comparison happens inside the same batch before the write, so no second stream and no scratch checkpoints are needed. Framework metadata columns are exempt from user cast configs. Public API:

```python
class CastService:
    def apply(self, df: DataFrame, cfg: TypingConfig, ctx: Context) -> DataFrame
```

### 3.4 Policies (`dbx_flame.policies`)

```python
@dataclass
class PolicyResult:
    policy: str; failed_count: int; severity: Severity; details: str

class PolicyRunner:
    def run(self, df: DataFrame, ctx: Context, batch: str | None = None) -> None: ...
```

- Rule evaluation is delegated to [Databricks DQX](https://databrickslabs.github.io/dqx/). The layer is a **gate**: it applies the configured ruleset, aggregates DQX's `_error`/`_warning` result columns into per-check counts, logs each to the audit table, drops the result columns, and raises `PolicyViolation` after evaluating **everything** if any row carried an `error`. The DataFrame handed on is the one that arrived. It judges the batch after dedup; COMPLETE_DELTA gates each replayed snapshot instead, naming it in the audit rows (`batch`).
- The ruleset is a detached YAML named by `policies.checks_file`, read and validated at Start. Nothing in this layer is native: schema drift belongs to the task-level `schema_evolution` knob, not here. Full semantics in [04_policies.md](04_policies.md).

### 3.5 Output (`dbx_flame.output`)

```python
class Writer(Protocol):
    verb: ClassVar[Verb]
    requires: ClassVar[Requirements]   # declared needs: keys? event_time? snapshot_time? deletes-support?
    def write(self, df: DataFrame, ctx: Context) -> None: ...
```

- Each verb **declares** its requirements; the Start layer validates the configured combination against them (this is how verbs stay layer-agnostic while misconfiguration fails fast, e.g. "SCD2 requires `output.keys` and `output.event_time.column`").
- `DeltaWriter` implementations: `append`, `full`, `upsert`, `scd2`, `complete_delta` — full semantics in [03_write_verbs.md](03_write_verbs.md). `complete_delta` composes the `scd2` merge engine inside its snapshot loop rather than duplicating it.
- `FileWriter` is **specified** (same protocol, target = Volume path + format) and stubbed with `NotImplementedError` — implementation is future work (export workflows), per the agreed scope.
- Shared mechanics: dedup block (window/keep-latest), event-time normalization (string → timestamp with format — held internally and never persisted), table-creation path with SCD2 metadata initialization, Delta `mergeSchema` behavior.

## 4. Observability

- `AuditLogger` writes the audit contract (`` `monitoring_{env}`.`audit`.`logs` ``, fixed schema, partitioned, auto-created) and **buffers** entries, flushing per stage transition and on any failure so a crash cannot lose the trail. Every entry is also printed to the driver log as it is logged, so a run whose flush never lands still leaves its trail there. Each run records `promotion_start` with the target's Delta version before any data moves: the version to restore when a failure is beyond what a retry repairs. API: `info/warning/error(name, source, description, total)`, plus `kpi(name, total, description)` for the named KPI events dashboards query.
- Errors always **raise**; logging never swallows control flow.

## 5. Error handling model

| Failure | Behavior |
|---|---|
| Invalid/missing/unknown config | Aggregated `ConfigValidationError` at Start; nothing executed |
| Unknown origin / verb | Config validation error (enum) |
| Source table missing (delta origin) | `RuntimeError` before any write |
| Target absent + empty incoming schema | Error — there is nothing to define the table from |
| Cast silent-NULL detected | `PlatformPolicyViolation` (`cast_silent_null`) with one example value per column |
| `error`-criticality check failed | `PolicyViolation` after all checks evaluated |
| Any exception | Audit buffer flushed, exception propagates, task fails |

## 6. Extension points (how the framework evolves)

| To add… | You write… | You do NOT touch… |
|---|---|---|
| a new file format | one `SourcePipeline` class + enum value | other pipelines, writers |
| a vendor-specific input shape | one `PreProcessor` + registry entry | `json_source` / generic flow |
| a new DQ rule | an entry in the DQX ruleset YAML | any Python at all |
| quarantine instead of a gate | the runner's handling of DQX's result columns | rules, writers, config |
| a new write verb | one `Writer` + `Requirements` declaration | Start/Pipeline/Typing |
| file export targets | implement `FileWriter` | Delta writers |

## 7. Deployment model

- Built as a wheel (src layout) and deployed via Databricks Asset Bundles; workflow tasks use `python_wheel_task` with `package_name: dbx-flame`, `entry_point: dbx-flame`, and flat `named_parameters`.
- Workflow YAML structure (anchors, job clusters, permissions blocks) is ordinary Databricks Asset Bundle YAML ([02_config_schema.md](02_config_schema.md) has a worked example).
- Python 3.11 / Spark 15.4.x runtime, matching Databricks Runtime 15.4 LTS.
