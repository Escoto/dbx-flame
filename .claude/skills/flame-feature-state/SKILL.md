---
name: flame-feature-state
description: Show a feature in progress as one sample config, marking which settings work today and which story will add the rest. Use when the user asks for an example config, a sample task or "how would this look" for a feature that is still being built.
---

# Feature state as a sample config

One workflow task YAML that shows the finished feature. It says which parts exist today
and which story adds each of the rest.

## Gather the state

1. Read the feature's epic and its stories (`gh issue view <N>`) for the settings each
   story adds.
2. Read the config model in `context/config.py` for the settings that exist today. The
   code decides what is ready, not a closed issue.
3. Pick one realistic use-case. Name it by function (`CRO`, `CDR`, `EDC`), never by
   vendor or customer.

## Write it

- One `named_parameters` block, grouped by story with a divider comment:
  `# ── available now (gh #42) ──`, then `# ── from gh #44: integrity ──` and so on.
- Ready settings first, then the later stories in delivery order.
- Give each setting a short comment where the value isn't self-evident: the effect it
  has, or the receiver rule it meets.
- Include the run identity parameters, so the block can be copied as it is.

Example:

```yaml
# ── from gh #43: shaping ─────────────────────────────
export.exclude_metadata: true        # drop __SOURCE, __EXPORT_DATE, …
```

## Around it

- Before the block, one sentence on how to read it: Start rejects the settings under a
  `from gh #N` heading as unknown until that story lands.
- After it, show what the run produces (a file tree or table rows) when that makes the
  feature clearer.
- End with one line naming the settings that only restate a default, and the minimum
  set a typical user needs.
