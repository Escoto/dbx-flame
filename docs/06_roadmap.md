# 06 — Implementation Roadmap

Phases, each independently reviewable and shippable. Every phase lands with its unit tests
(per [05_testing.md](05_testing.md)). Sequencing follows the dependency chain
Context → Pipeline/Typing → Writers → Policies → remaining origins.

## Status

| Phase | Scope | State |
|---|---|---|
| P0 | Scaffold | ✅ complete |
| P1 | Start layer + observability | ✅ complete |
| P2 | Pipeline (CSV + Delta) and Typing | ✅ complete |
| P3 | Output verbs: APPEND, FULL, UPSERT | ✅ complete |
| P4 | Output verbs: SCD2, COMPLETE_DELTA | ✅ complete |
| P5 | Policies layer | ✅ complete |
| P6 | Pipeline (JSON + SAS) and pre-processors | 🟨 in progress |

---

## P0 — Scaffold

- Package with src layout, `pyproject.toml` (wheel build; pinned Python 3.11, PySpark/delta-spark matching runtime 15.4.x; dev deps pytest, chispa, black, flake8, mypy, ruff).
- Test harness: local Spark + Delta fixture (`conftest.py`), pre-commit wiring.
- Empty layer packages with the protocols/interfaces from [01_architecture.md](01_architecture.md) stubbed.

**Exit**: `pytest` green on a placeholder test; wheel builds.

## P1 — Start layer + Observability

- `context/`: pydantic `TaskConfig` model tree, flat-param loader (dotted-key splitting, coercion, comma-lists, enums), unknown-key rejection, aggregated `ConfigValidationError`, origin × verb requirements validation (against writer-declared `Requirements`), resolved names/paths, frozen `Context`.
- `observability/`: buffered `AuditLogger`, KPI helpers, `tagging.py`.
- `entrypoints/run.py`: parameter intake → Context → dispatch skeleton.

**Exit**: every parameter in [02_config_schema.md](02_config_schema.md) parses and validates; all negative-config unit tests pass.

## P2 — Pipeline (CSV + Delta) and Typing

- `pipelines/`: `csv_source` (Auto Loader incl. `txt` extension, options, schema hints/evolution), `table_source` (checkpoint + watermark strategies, deletes feed exposure), `enrichment` (provenance, sanitization, rename patterns), pre-processor registry.
- `typecast/`: cast service with single-pass validation and metadata-column exemption.

**Exit**: a CSV fixture flows source → typed DataFrame with correct provenance/sanitization; unit tests for both increment strategies pass.

## P3 — Output verbs: APPEND, FULL, UPSERT

- `output/base.py` (Writer protocol + `Requirements`), `delta/append.py`, `delta/full.py` (empty-skip), `delta/upsert.py` (SCD1, newer-wins), table-creation paths, `mergeSchema` behavior, `files.py` interface stub.
- End-to-end wiring in `run.py`: the first complete flows (CSV→Bronze APPEND).

**Exit**: Inbound→Bronze works end to end on a Databricks workspace; UPSERT unit-tested against in-memory Delta.

## P4 — Output verbs: SCD2 + COMPLETE_DELTA

- `delta/scd2.py`: dedup block, event-time normalization (internal only), anti-filter (no temp views, no SQL string interpolation), close-and-insert merge, metadata init, `snapshot_scope: full` expiry, deletes double-merge.
- `delta/complete_delta.py`: typed watermark, snapshot-id derivation (both file-name patterns), ordered replay composing the scd2 engine, empty-table creation path, cheap emptiness checks.

**Exit**: unit suites reproduce the multi-snapshot worked example from [03_write_verbs.md](03_write_verbs.md) §5.

## P5 — Policies layer

- `policies/`: `checks.py` (read + validate the DQX ruleset at Start), `runner.py` (apply, aggregate, log, enforce `error` criticality), audit integration. No native rules: schema drift is the task-level `schema_evolution` knob.
- Wire the runner between Typing and the write in `entrypoints/pipeline`, for both the batch and `foreachBatch` paths.

**Exit**: policy unit tests green; an `error`-criticality violation fails a run end to end while `warn` does not.

*Outstanding*: the end-to-end half is unproven. The SCD2 platform test carries a ruleset but
exercises only the passing path, so no platform test yet shows an `error` violation failing a
run or a `warn` letting one through.

## P6 — Pipeline (JSON + SAS) and pre-processors

- `json_source` (options, tree-schema logging), the `record_envelope` pre-processor, `sas_source` (binaryFile discovery, pandas read, empty-file rules, WINDOWS-1252).

**Exit**: a JSON envelope fixture and a SAS fixture flow end to end; the pre-processor registry is covered by tests.

*Landed*: `json_source`, the `record_envelope` pre-processor and `source.envelope_fields`, proven
end to end by `complete_delta_json_full_test` (two feeds, two Bronze tables, one Silver).

*Open*: `sas_source` and the SAS fixture. Schema logging moved to gh #24, for every origin.

## Out of scope (explicitly deferred)

- File/export writer implementations (`files.py` stays an interface).
- Quarantining invalid rows to a second target; the gate refuses the batch whole.
- Source→Inbound retrieval, governance views, export workflows.
- A generic `flatten_nested` pre-processor: exploding nested arrays multiplies rows and
  denormalizes the data, so flattening a payload is left to its consumer.

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Wheel-task rollout friction (permissions/policies expecting notebook tasks) | P3's exit criterion includes a real workspace deployment; fallback documented: a thin notebook shim calling the package |
| Streaming semantics drift (availableNow + foreachBatch layering) | Platform tests re-run twice to prove idempotence |
| Snapshot regex mismatch on real file names | Both patterns unit-tested, and exercised end to end by the COMPLETE_DELTA platform tests |
