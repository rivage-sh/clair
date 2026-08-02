---
name: project_design_invariants
description: Seven design rules that clair never breaks — read before you propose a new feature or a refactor
metadata:
  type: project
---

These rules control all clair design work. A change that breaks one of them is wrong,
even if the code operates correctly.

1. No Jinja. SQL is a plain Python f-string.
2. No YAML for configuration. YAML holds credentials only, in `~/.clair/environments.yml`.
   All other configuration is Python.
3. The file path gives `database.schema.table`. Trouve files are three levels below the
   project root.
4. `clair compile` makes no connection to Snowflake. It is a local operation only.
5. Validation is eager. An invalid Trouve raises at construction time, not at run time.
6. Shared SQL logic is a normal Python function, which you import normally.
7. All warehouse access goes through the `WarehouseAdapter` ABC in
   `src/clair/adapters/base.py`. The runner must not import `SnowflakeAdapter` directly.

**Why:** These rules are the product position against dbt and SQLMesh. Python-native, no
template language, no configuration language, full IDE support.

**How to apply:** Before you write a new module or change an interface, compare your design
to this list. For feature behaviour that these rules do not cover, read `site_docs/`
(see [[project_docs_are_the_source_of_truth]]).
