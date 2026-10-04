---
name: flame-pm
description: How dbx-flame plans work through GitHub issues. Use when choosing what to work on next, evaluating or triaging an issue, or writing and labelling a new one.
---

# dbx-flame planning

GitHub issues are the only backlog. No TODO files or review docs: a finding worth keeping
becomes an issue. Refer to issues as **gh #N**.

## 1. Choosing what to work on

```bash
gh issue list --state open --label priority-high
gh issue view <N>
```

- Work high → medium → low. Within a priority, bugs before enhancements.
- An issue without a priority is waiting on an input (data, a decision); its comments say
  which. Don't pick it up until that input exists.
- Before starting, check the issue still matches the code: paths, names and commands drift.
  Fix stale references in the issue first.

## 2. Writing an issue

Short, plain and direct. Fewer words that each carry weight. Say *what* must change, never
*how* to build it.

- **Title:** one sentence, no trailing period. Name the component when it helps:
  `COMPLETE_DELTA: a failed run can lose a snapshot's deletes`.
- **One problem per issue.** Merge two only when one change fixes both.
- Describe behavior, not code. A path or name only when the reader needs it to find the
  problem.

```markdown
## Description
**Current behavior:** 1–2 sentences.
**Expected behavior:** 1–2 sentences.

## Deliverables
1. ...

## Acceptance criteria
- [ ] ...
```

Drop Deliverables when the expected behavior already says it all.

## 3. Labelling

Every issue gets **one type** and **one priority**. Add a second type only when the issue
genuinely mixes both.

| Type | When |
|---|---|
| `bug` | The framework does something wrong or differs from its docs. |
| `enhancement` | New capability or improvement; nothing is broken. |
| `chore` | Housekeeping: tests, dependencies, DDL, tooling. |
| `documentation` | Docs only. |

| Priority | When |
|---|---|
| `priority-high` | Can pollute Silver or Gold data, writes a false audit record (a compliance risk in regulated environments), or leaves a gap users need to adopt the framework. |
| `priority-medium` | Blocks promoting valid data, weakens traceability, or lets a misconfiguration pass silently. |
| `priority-low` | Performance, cost, rare edge cases, or anything with no effect on data or promotion. |
| none | Can't be scheduled yet; a comment says what it is waiting for. |

Judge by impact on the data and the audit trail, not by effort.

## 4. Keeping issues current

- A decision that changes scope or priority goes in a comment on the issue: what was
  decided, and why.
- Ask before posting to GitHub, except an issue or edit the user asked for.

```bash
gh issue create --title "..." --label bug,priority-high --body-file <file>
gh issue edit <N> --add-label enhancement,priority-low
gh issue comment <N> --body "..."
```
