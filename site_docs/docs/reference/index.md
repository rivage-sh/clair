# Reference

The full API reference for the Python classes of clair.

You import all the public types directly from `clair`:

```python
from clair import (
    Append,
    Column,
    ColumnType,
    DatabaseDefaults,
    DeleteInsert,
    FullRefresh,
    Incremental,
    RunConfig,
    RunMode,
    SchemaDefaults,
    TestNotNull,
    TestRowCount,
    TestUnique,
    TestUniqueColumns,
    Upsert,
)
```

- **[Python API](python-api.md)** — the operations: `clair.run()`, `clair.compile()`, `clair.test()`, `clair.docs()` and `clair.catalog()`
- **[Trouve](trouve-api.md)** — the core classes: `TrouveAbc`, `Trouve` for SQL, `PandasTable` for pandas, and `SeedTable` for a table that holds its rows in the file
- **[Column](column-api.md)** — column definitions
- **[RunConfig](run-config-api.md)** — `FullRefresh` or `Incremental`, and the incremental strategies
- **[Tests](tests-api.md)** — data quality test classes
