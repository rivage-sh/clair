# Topics

Each page starts with what the subject is, then shows how to configure it. Read the first
five pages before you use clair. Read the others when you need the behaviour.

- **[Trouve](trouve.md)** — the basic unit. One Python file, one Snowflake object.
- **[Project Layout](project-layout.md)** — how the directory structure maps to Snowflake names.
- **[DAG](dag.md)** — the dependency graph. clair builds it from the Python imports.
- **[The Anatomy of a Run](anatomy-of-a-run.md)** — the seven steps of `clair run`, and the address that each Trouve reads.
- **[Environments](environments.md)** — Snowflake connection profiles.
- **[Selectors](selectors.md)** — run only a subset of your project.
- **[Data Quality Tests](data-quality-tests.md)** — attach tests to Trouves.
- **[Staging](staging.md)** — clair publishes a Trouve only after its tests pass.
- **[Incrementality](incrementality.md)** — APPEND and UPSERT strategies for large tables.
- **[Pandas-Native Transformations](pandas-native.md)** — write pipeline steps as Python functions with pandas.
- **[Routing](routing.md)** — remap the Snowflake target of each environment.
- **[Per-Database & Schema Config](per-database-schema-config.md)** — warehouse and role overrides per directory.

For the field tables of each Python class, read the [Reference](../reference/index.md).
