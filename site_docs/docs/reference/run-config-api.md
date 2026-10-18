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
    APPEND        = "append"
    UPSERT        = "upsert"
    DELETE_INSERT = "delete_insert"
```

## `UpsertConfig`

Exact control of the columns in an UPSERT MERGE statement.

```python
class UpsertConfig(BaseModel):
    update_columns: list[str] | None = None
    insert_columns: list[str] | None = None
```

| Field | Default | Description |
|-------|---------|-------------|
| `update_columns` | `None` — all the columns that are not keys | The columns for `WHEN MATCHED THEN UPDATE SET` |
| `insert_columns` | `None` — all the columns | The columns for `WHEN NOT MATCHED THEN INSERT` |

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
| `run_mode` | `RunMode` | `FULL_REFRESH` | `FULL_REFRESH` makes the table again. `INCREMENTAL` applies the new data only. |
| `incremental_mode` | `IncrementalMode \| None` | `None` | `APPEND`, `UPSERT` or `DELETE_INSERT`. Necessary if `run_mode=INCREMENTAL`. |
| `primary_key_columns` | `list[str] \| None` | `None` | The columns to match on for UPSERT and DELETE_INSERT. clair generates `target.col = source.col`. |
| `join_sql` | `str \| None` | `None` | Your own match condition for UPSERT and DELETE_INSERT. Use it in place of `primary_key_columns`. |
| `upsert_config` | `UpsertConfig \| None` | `None` | Control of the columns in an UPSERT MERGE statement. UPSERT only. |

### Validation matrix

| `run_mode` | `incremental_mode` | `primary_key_columns` / `join_sql` | Valid? |
|---|---|---|---|
| `FULL_REFRESH` | `None` | Not set | ✓ |
| `FULL_REFRESH` | any | any | ✗ — `incremental_mode` applies to INCREMENTAL only |
| `INCREMENTAL` | `None` | any | ✗ — `incremental_mode` is necessary |
| `INCREMENTAL` | `APPEND` | Not set | ✓ |
| `INCREMENTAL` | `APPEND` | Set | ✗ — APPEND does not accept `primary_key_columns` or `join_sql` |
| `INCREMENTAL` | `UPSERT` | Neither set | ✗ — give one of the two |
| `INCREMENTAL` | `UPSERT` | Both set | ✗ — give one of the two, not both |
| `INCREMENTAL` | `UPSERT` | Exactly one set | ✓ |
| `INCREMENTAL` | `DELETE_INSERT` | Neither set, or both set | ✗ — give exactly one of the two |
| `INCREMENTAL` | `DELETE_INSERT` | Exactly one set | ✓ — `upsert_config` is not permitted |

### Examples

```python
# The default — a full refresh, written out
RunConfig()

# Incremental, with APPEND
RunConfig(
    run_mode=RunMode.INCREMENTAL,
    incremental_mode=IncrementalMode.APPEND,
)

# UPSERT on one key
RunConfig(
    run_mode=RunMode.INCREMENTAL,
    incremental_mode=IncrementalMode.UPSERT,
    primary_key_columns=["customer_id"],
)

# UPSERT with your own join condition
RunConfig(
    run_mode=RunMode.INCREMENTAL,
    incremental_mode=IncrementalMode.UPSERT,
    join_sql="target.customer_id = source.customer_id AND target.region = source.region",
    upsert_config=UpsertConfig(
        update_columns=["total_orders", "last_order_at"],
    ),
)

# DELETE_INSERT: replace each day that the new rows hold
RunConfig(
    run_mode=RunMode.INCREMENTAL,
    incremental_mode=IncrementalMode.DELETE_INSERT,
    primary_key_columns=["created_date"],
)
```

## See also

- [Incrementality](../topics/incrementality.md)
