# Reference

Complete API reference for clair's Python classes.

All public types are importable directly from `clair`:

```python
from clair import (
    Trouve, TrouveType,
    PandasTrouve,
    Column, ColumnType,
    RunConfig, RunMode, IncrementalMode, UpsertConfig,
    TestUnique, TestNotNull, TestRowCount, TestUniqueColumns,
    DatabaseDefaults, SchemaDefaults,
)
```

- **[Trouve](trouve-api.md)** — the core SQL-based class, and `PandasTrouve` for pandas-native transformations
- **[Column](column-api.md)** — column definitions
- **[RunConfig](run-config-api.md)** — incremental materialization config
- **[Tests](tests-api.md)** — data quality test classes
