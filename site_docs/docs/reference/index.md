# Reference

The full API reference for the Python classes of clair.

You import all the public types directly from `clair`:

```python
from clair import (
    Trouve, TrouveType,
    Column, ColumnType,
    RunConfig, RunMode, IncrementalMode, UpsertConfig,
    TestUnique, TestNotNull, TestRowCount, TestUniqueColumns,
    DatabaseDefaults, SchemaDefaults,
    ProjectConfig,
)
```

- **[Python API](python-api.md)** — the operations: `clair.run()`, `clair.compile()`, `clair.test()`, `clair.docs()` and `clair.catalog()`
- **[Trouve](trouve-api.md)** — the core classes: `TrouveAbc`, `Trouve` for SQL, `PandasTrouve` for pandas, and `SeedTrouve` for a table that holds its rows in the file
- **[Column](column-api.md)** — column definitions
- **[RunConfig](run-config-api.md)** — incremental materialization config
- **[Tests](tests-api.md)** — data quality test classes

`ProjectConfig` holds no page of its own. It goes in `__clair_project__.py` at the project
root, and [Project Layout](../topics/project-layout.md)
gives the complete behaviour.
