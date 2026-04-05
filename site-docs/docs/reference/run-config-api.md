# RunConfig API

```python
from clair import RunConfig, RunMode, IncrementalMode, UpsertConfig
```

## `RunMode`

```python
class RunMode(StrEnum):
    FULL_REFRESH = "full_refresh"
    INCREMENTAL  = "incremental"
```

## `IncrementalMode`

```python
class IncrementalMode(StrEnum):
    APPEND = "append"
    UPSERT = "upsert"
```

## `UpsertConfig`

Fine-grained column control for UPSERT MERGE statements.

```python
class UpsertConfig(BaseModel):
    update_columns: list[str] | None = None
    insert_columns: list[str] | None = None
```

| Field | Default | Description |
|-------|---------|-------------|
| `update_columns` | `None` (all non-key columns) | Columns to include in `WHEN MATCHED THEN UPDATE SET` |
| `insert_columns` | `None` (all columns) | Columns to include in `WHEN NOT MATCHED THEN INSERT` |

## `RunConfig`

```python
class RunConfig(BaseModel):
    run_mode:             RunMode = RunMode.FULL_REFRESH
    incremental_mode:     IncrementalMode | None = None
    primary_key_columns:  list[str] | None = None
    join_sql:             str | None = None
    upsert_config:        UpsertConfig | None = None
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `run_mode` | `RunMode` | `FULL_REFRESH` | `FULL_REFRESH` recreates the table; `INCREMENTAL` applies only new data |
| `incremental_mode` | `IncrementalMode \| None` | `None` | Required when `run_mode=INCREMENTAL`: `APPEND` or `UPSERT` |
| `primary_key_columns` | `list[str] \| None` | `None` | Column names to match on for UPSERT. Generates `ON target.col = source.col`. |
| `join_sql` | `str \| None` | `None` | Custom `ON` clause for UPSERT (alternative to `primary_key_columns`). |
| `upsert_config` | `UpsertConfig \| None` | `None` | Column overrides for UPSERT MERGE statements. |

### Validation matrix

| `run_mode` | `incremental_mode` | `primary_key_columns` / `join_sql` | Valid? |
|---|---|---|---|
| `FULL_REFRESH` | `None` | Not set | ✓ |
| `FULL_REFRESH` | any | any | ✗ (`incremental_mode` only valid with INCREMENTAL) |
| `INCREMENTAL` | `None` | any | ✗ (`incremental_mode` required) |
| `INCREMENTAL` | `APPEND` | Not set | ✓ |
| `INCREMENTAL` | `APPEND` | Set | ✗ (`primary_key_columns`/`join_sql` not valid for APPEND) |
| `INCREMENTAL` | `UPSERT` | Neither set | ✗ (one required) |
| `INCREMENTAL` | `UPSERT` | Both set | ✗ (specify one, not both) |
| `INCREMENTAL` | `UPSERT` | Exactly one set | ✓ |

### Examples

```python
# Default — full refresh (explicit)
RunConfig()

# Append-only incremental
RunConfig(
    run_mode=RunMode.INCREMENTAL,
    incremental_mode=IncrementalMode.APPEND,
)

# UPSERT on a single key
RunConfig(
    run_mode=RunMode.INCREMENTAL,
    incremental_mode=IncrementalMode.UPSERT,
    primary_key_columns=["customer_id"],
)

# UPSERT with custom join condition
RunConfig(
    run_mode=RunMode.INCREMENTAL,
    incremental_mode=IncrementalMode.UPSERT,
    join_sql="target.customer_id = source.customer_id AND target.region = source.region",
    upsert_config=UpsertConfig(
        update_columns=["total_orders", "last_order_at"],
    ),
)
```

## See also

- [Incrementality guide](../guides/incrementality.md)
