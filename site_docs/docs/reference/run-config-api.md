# RunConfig API

```python
from clair import Append, DeleteInsert, FullRefresh, Incremental, RunConfig, RunMode, Upsert
```

`RunConfig` is the type of `Trouve.run_config`. It is one of two classes:

```python
RunConfig = FullRefresh | Incremental
```

A full refresh takes no parameter. An incremental run takes a strategy, and each strategy holds only the parameters that it reads. Thus a parameter that a strategy does not read is an error when the Trouve file runs.

## `FullRefresh`

Make the object again on each run. It is the default `run_config`.

```python
class FullRefresh(BaseModel):
    run_mode: Literal[RunMode.FULL_REFRESH] = RunMode.FULL_REFRESH
```

## `Incremental`

Apply the new data only, to the object that exists.

```python
class Incremental(BaseModel):
    run_mode: Literal[RunMode.INCREMENTAL] = RunMode.INCREMENTAL
    strategy: Append | Upsert | DeleteInsert | None = None
```

| Field | Default | Description |
|-------|---------|-------------|
| `strategy` | `None` | The built-in method that writes the new data. `None` tells clair that a [`build_sql()` override](../guides/custom-build-sql.md) writes the incremental SQL. A `Trouve` with no override and no strategy raises a `CompileError` on an incremental run. |

## Strategies

### `Append`

Insert the new rows, and keep each row that the table holds. It takes no parameter.

```python
class Append(BaseModel): ...
```

### `Upsert`

Merge the new rows: update each row that matches, and insert each other row. It needs `columns` on the Trouve.

```python
class Upsert(BaseModel):
    primary_key_columns: list[str] | None = None
    join_sql:            str | None = None
    update_columns:      list[str] | None = None
    insert_columns:      list[str] | None = None
```

| Field | Default | Description |
|-------|---------|-------------|
| `primary_key_columns` | `None` | The columns to match on. clair generates `target.col = source.col` for each one. |
| `join_sql` | `None` | Your own match condition, with the aliases `target` and `source`. Use it in place of `primary_key_columns`. |
| `update_columns` | `None` — each column that is not a key | The columns for `WHEN MATCHED THEN UPDATE SET`. With `join_sql`, the default is each column. |
| `insert_columns` | `None` — each column | The columns for `WHEN NOT MATCHED THEN INSERT`. |

Give `primary_key_columns` or `join_sql`, and not both.

### `DeleteInsert`

Delete the target rows that the new rows replace, then insert all the new rows. It needs no `columns`.

```python
class DeleteInsert(BaseModel):
    primary_key_columns: list[str] | None = None
    join_sql:            str | None = None
    range_column:        str | None = None
```

| Field | Default | Description |
|-------|---------|-------------|
| `primary_key_columns` | `None` | Delete each target row whose key a new row holds. The same meaning as on `Upsert`. |
| `join_sql` | `None` | Delete each target row that this condition matches, with the aliases `target` and `source`. |
| `range_column` | `None` | Delete each target row whose value of this column is between the minimum and the maximum of the new rows. |

Give one of the three fields only.

## `RunMode`

The mode of one run. `--run-mode` on the CLI, `clair.run_mode` in a Trouve file, and the `effective_mode` argument of `build_sql()` hold a `RunMode`.

```python
class RunMode(StrEnum):
    FULL_REFRESH = "full_refresh"
    INCREMENTAL  = "incremental"
```

## Examples

```python
# The default — a full refresh, written out
FullRefresh()

# Append the new rows
Incremental(strategy=Append())

# Merge on one key
Incremental(strategy=Upsert(primary_key_columns=["customer_id"]))

# Merge with your own join condition, and update two columns only
Incremental(
    strategy=Upsert(
        join_sql="target.customer_id = source.customer_id AND target.region = source.region",
        update_columns=["total_orders", "last_order_at"],
    ),
)

# Replace each day that the new rows hold
Incremental(strategy=DeleteInsert(primary_key_columns=["created_date"]))

# Replace each day from the first to the last day of the new rows
Incremental(strategy=DeleteInsert(range_column="created_date"))

# A build_sql() override writes the incremental SQL
Incremental()
```

## See also

- [Incrementality](../topics/incrementality.md)
- [Write your own build with `build_sql()`](../guides/custom-build-sql.md)
