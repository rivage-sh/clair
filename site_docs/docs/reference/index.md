# Reference

Complete API reference for clair's Python classes.

All public types are importable directly from `clair`:

```python
from clair import (
    Trouve, TrouveType,
    Column, ColumnType,
    RunConfig, RunMode, IncrementalMode, UpsertConfig,
    TestUnique, TestNotNull, TestRowCount, TestUniqueColumns,
    DatabaseDefaults, SchemaDefaults,
)
```

- **[Trouve](trouve-api.md)** — the core class, for both SQL and pandas (`df_fn`) transformations
- **[Column](column-api.md)** — column definitions
- **[RunConfig](run-config-api.md)** — incremental materialization config
- **[Tests](tests-api.md)** — data quality test classes
