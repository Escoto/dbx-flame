---
name: flame-data-engineer
description: Coding standards, design principles and working agreements for dbx-flame. Use before writing, refactoring, reviewing or testing any code, config, workflow YAML or docs in this repo, and when breaking a feature or GitHub issue into changes.
---

# dbx-flame engineering standards

Read `CLAUDE.md` first: it covers architecture, conventions and how to run things.
This skill covers *how* to build here. Follow it in every session, whatever the model.

## 1. Working agreements

- **Discuss before you change framework logic.** Read the code, propose a short plan
  (what changes, the impact, open questions), then wait for approval. Docs, tests and
  approved work need no second approval.
- **Never commit or push.** The project runs in a regulated environment. The user also
  creates branches, deploys and runs platform tests; you may monitor runs with the CLI.
- **Run natively on Linux**: `poetry`, `gh` and `databricks` directly. Only when the
  session runs on Windows, run them through WSL (`wsl -d Ubuntu-24.04 -- bash -lc '...'`).
- **Keep answers short.** Report what changed, what was verified, and what's left open.
- **Ask before posting** anything to GitHub, except an issue the user asked you to create.

## 2. Design principles

**One home per concern.** Before adding logic, search for where it already lives. If
two places do the same thing, extract it to a single home and make both call it. The
existing homes:

| Concern | Home |
|---|---|
| Typed task config and Start validation | `context/config.py` (pydantic validators), `context/loader.py` |
| Origin → reader | `SOURCES` in `pipelines/registry.py` |
| Shared Auto Loader options | `cloud_files_options` in `pipelines/base.py` |
| Verb → writer, and what it needs | `WRITER_BY_VERB` in `output/registry.py`, `Requirements` in `output/base.py` |
| Shared write mechanics (parsing dates, dedup, promotion, schema evolution) | `output/mechanics.py` |
| Table-level Delta properties | `DeltaTableConfig` in `output/table_config.py` |
| Framework-enforced checks | `PlatformPolicy` + `violate()` in `policies/platform.py` |
| User data-quality rules | DQX ruleset, `PolicyRunner` in `policies/runner.py` |
| Named preprocessors | `PREPROCESSORS` in `pipelines/preprocessors.py` |
| Audit trail | `ctx.logger` (`observability/audit_logger.py`) |

**Registries over branching.** A new origin, verb, policy or preprocessor is a new entry
in its registry, not a new `if` in the caller. Callers look the entry up.

**Declare, then validate at Start.** A component declares what it needs (a verb's
`Requirements`, a config model's validators). Start checks every combination before any
data is read. A layer never reads raw task parameters, only the validated `Context`.

**Fail fast and loud; never silently.**
- A setting that would do nothing for this origin or verb is **rejected at Start**, not
  ignored (see `schema_hints`, `anchor_dt`, `envelope_fields`).
- A broken data contract (bad stamp, silent NULL from a cast, unparseable date) fails the
  batch through `violate(ctx, PlatformPolicy.X, source, message)`. It writes the audit row,
  flushes, then raises. A new contract is a new `PlatformPolicy` member, never a one-off
  exception class.
- A value that would turn into a silent NULL is an error. Log-and-continue is only for
  cases that are genuinely harmless, and they log a `warning`.
- Nothing is written from a batch that fails, and a multi-step write checks everything
  before its first commit.

**Keep it as simple as the problem allows.** No speculative options, no abstraction with
one caller, no config for behaviour nobody asked for. Prefer an opt-in setting with a safe
default over changing behaviour for every task. Greenfield: "parity" is never a reason.

## 3. Breaking a feature down

1. **Place it in a layer**: Start → Pipeline → Typing → Policies → Output, plus
   `observability/` and `entrypoints/`. A batch flows:
   `read → provenance → reject_unstamped → prepare() → platform checks → dedup → DQX → writer`.
2. **Config first.** Add the field to the pydantic model with a safe default, its
   validation (and rejections) at Start, and a line in `docs/02_config_schema.md`.
3. **Find the home.** Reuse an existing helper or registry; extend it rather than copy
   it. Only add a module when no existing home fits.
4. **Small functions, one job each.** Private helpers start with `_`. Pure DataFrame
   transformations stay free of I/O (as `prepare()` is), and actions that can refuse are
   kept apart from them.
5. **Tests**: a unit test per behaviour; a platform test when it touches real Auto Loader,
   Delta or the job DAG.
6. **Docs** in the same change: the relevant `docs/0X_*.md`, and README Status if the
   capability changed.
7. **Refactor first, then the feature**, as two reviewable steps, when the feature needs a
   new home.

## 4. Code style

- Python 3.11, `from __future__ import annotations`, type hints everywhere; imports used
  only in hints go under `if TYPE_CHECKING:`.
- black (line length 99), flake8 and yamllint clean. Check with `black --check`, because
  `make qa` rewrites files in place.
- **Comments explain why, in 1–2 sentences.** Never restate the code. A module and each
  public function get a docstring: a one-line summary, then the reason if it isn't obvious.
- Record guarantees from validation with an assert and a reason:
  `assert path and directory  # guaranteed by SourceConfig`.
- Enums are `StrEnum`; a registry of names that must stay distinct gets `@unique`.
- Names describe behaviour: `require_valid_times`, `reject_unstamped`, `compared_times`.

### Naming

| Thing | Convention | Example |
|---|---|---|
| Delta tables | UPPERCASE (validated) | `SUPPLIER_TXT_SILVER_1` |
| Framework columns | `__` + UPPERCASE | `__EXPORT_DATE`, `__ANCHOR_DT` |
| Task parameters | dotted, lower snake | `source.options.delimiter` |
| Audit event names | lower snake, the event itself | `unstamped_file`, `rows_appended` |
| Platform test jobs | `<verb>_<format>_<scenario>_test` | `full_txt_weekly_test` |

### Spark

- Build `Column` expressions; never interpolate values into SQL strings. Backtick column
  names: ``F.col(f"`{name}`")``.
- Compare dates through `as_timestamp`, so every check parses exactly as the writers do.
- One aggregate pass per check. Keep one example with `F.first(..., ignorenulls=True)`
  rather than collecting rows; `isEmpty()` rather than `count()` for emptiness.
- Filter on real columns (`__EXPORT_DATE`, `__ANCHOR_DT`) so the filter reaches the scan.
- Never persist helper columns to a target.
- Cache only a frame read twice, and `unpersist()` in `finally`.
- Session conf changes are restored on the way out (see `schema_auto_merge`).

## 5. Testing

- **Unit** (`make test`): pytest, local Spark + Delta, 70% coverage gate.
  - Test names are sentences about behaviour:
    `test_scd2_refuses_a_batch_with_a_null_event_time`.
  - A docstring says why the case matters when the name can't.
  - For a bug, first write the test that reproduces it, then fix it.
  - `ctx` is a `MagicMock` when only `ctx.logger` or config is touched; use the module's
    `_ctx` helper and a throwaway database for real tables.
  - Assert the audit row (`ctx.logger.error.call_args.kwargs`) for every failure path.
- **Platform** (`platform_tests/<name>/` + `workflows/platform_tests/<name>.yml`):
  - Steps: `_shared.py` (constants and asserts), `0_cleanup.py`, `1_generate_data.py`,
    `2_validate_bronze.py`, `3_validate_silver.py`. Scripts take `[*base, *catalog, env, round]`.
  - Clean **only** the test's own tables and directories; the suite runs in parallel.
  - Fixtures are deterministic and small; assert exact rows, not just counts.
  - Register the job in `workflows/integration_test_suite.yml` and add a row to
    `docs/05_testing.md`.
  - Validate with `databricks bundle validate -t dev` before handing over.

## 6. GitHub issues

Writing, labelling and choosing issues follows the `flame-pm` skill.

## 7. Windows editing

On Windows, Bash heredocs mangle `\n` escapes. For a multi-line patch there, write a Python script to
the scratchpad with the Write tool and run it by path. Each replacement asserts its
target matches exactly once.

## 8. Done checklist

- [ ] The plan was approved, if framework logic changed.
- [ ] Logic lives in its one home; nothing is duplicated; registries updated.
- [ ] Invalid config is rejected at Start; broken data fails through `violate()`.
- [ ] Unit tests cover each behaviour and failure path; `make test` passes.
- [ ] black, flake8 and yamllint are clean; the bundle validates if YAML changed.
- [ ] Docs, the config schema and `05_testing.md` are updated.
- [ ] The summary states what was verified, and what only a platform run can prove.
