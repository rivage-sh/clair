---
name: project_docs_are_the_source_of_truth
description: Read site_docs/docs/ to learn clair behaviour — do not read src/ first, and do not copy docs content into memory
metadata:
  type: project
---

`site_docs/docs/` is the source of truth for clair behaviour. It is Markdown, it is small
(approximately 9000 words), and `grep` finds content in it quickly.

**Why:** Memory notes about features became wrong. An old note sent agents to
`src/clair/auth/environments.py`, a path that does not exist. The documentation is the
better source because users read it, so an error there gets a report.

**But the documentation also rots.** The pandas guide, the landing page and the README
documented a `PandasTrouve` class that was never built, while the API reference correctly
documented the `df_fn` field that was. The pages came from a design spec, and nobody
changed them when the implementation took a different shape. mkdocs does not execute the
examples, so CI did not catch it.

**Therefore: the code is the final authority.** Read the documentation first for
orientation, then confirm any API detail against `src/` or `example_projects/` before you
depend on it. When the two disagree, the code wins and the page is a bug.

**How to apply:**

- To learn what a feature does, `grep` `site_docs/docs/` first. Then confirm the exact API
  against `src/` or a project in `example_projects/`.
- Search for the field name, not only the class name. The pandas feature was invisible to a
  search for `PandasTrouve`, because the real name is `df_fn`.
- Map for orientation: `concepts/` (Trouve, DAG, project layout, environments),
  `guides/` (routing, incrementality, tests, selectors, pandas, per-database config),
  `cli/` (one page for each subcommand), `reference/` (API for Trouve, Column, RunConfig,
  Tests).
- When you change behaviour, change the matching page in `site_docs/docs/` in the same PR.
  Do not add a memory note that repeats the page.
- Write a memory note only for what the documentation cannot hold: a design rule
  (see [[project_design_invariants]]) or a correction the user gave you.
