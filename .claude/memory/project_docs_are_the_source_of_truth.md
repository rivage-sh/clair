---
name: project_docs_are_the_source_of_truth
description: Read site_docs/docs/ to learn clair behaviour — do not read src/ first, and do not copy docs content into memory
metadata:
  type: project
---

`site_docs/docs/` is the source of truth for clair behaviour. It is Markdown, it is small
(approximately 9000 words), and `grep` finds content in it quickly. CI publishes it, so it
stays correct.

**Why:** Memory notes about features became wrong. An old note sent agents to
`src/clair/auth/environments.py`, a path that does not exist. The published documentation
cannot rot in this way, because a wrong page is a visible bug.

**How to apply:**

- To learn what a feature does, `grep` `site_docs/docs/` first. Read `src/` only when you
  need the implementation, not the behaviour.
- Map for orientation: `concepts/` (Trouve, DAG, project layout, environments),
  `guides/` (routing, incrementality, tests, selectors, pandas, per-database config),
  `cli/` (one page for each subcommand), `reference/` (API for Trouve, Column, RunConfig,
  Tests).
- When you change behaviour, change the matching page in `site_docs/docs/` in the same PR.
  Do not add a memory note that repeats the page.
- Write a memory note only for what the documentation cannot hold: a design rule
  (see [[project_design_invariants]]) or a correction the user gave you.
