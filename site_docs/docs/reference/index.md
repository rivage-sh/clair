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
)
```

- **[Python API](python-api.md)** — the operations: `clair.run()`, `clair.compile()`, `clair.test()`, `clair.catalog()` and `clair.serve_docs()`
- **[Trouve](trouve-api.md)** — the core classes: `TrouveAbc`, `Trouve` for SQL, and `PandasTrouve` for pandas
- **[Column](column-api.md)** — column definitions
- **[RunConfig](run-config-api.md)** — incremental materialization config
- **[Tests](tests-api.md)** — data quality test classes
