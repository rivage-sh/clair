# Column API

```python
from clair import Column, ColumnType
```

## `ColumnType`

Named constants for common Snowflake data types. Because `ColumnType` is a `StrEnum`, members compare equal to their string values (`ColumnType.STRING == "STRING"`).

| Constant | Snowflake type |
|----------|----------------|
| `STRING` | STRING |
| `VARCHAR` | VARCHAR |
| `NUMBER` | NUMBER |
| `FLOAT` | FLOAT |
| `INTEGER` | INTEGER |
| `BOOLEAN` | BOOLEAN |
| `DATE` | DATE |
| `TIMESTAMP` | TIMESTAMP |
| `TIMESTAMP_NTZ` | TIMESTAMP_NTZ |
| `TIMESTAMP_LTZ` | TIMESTAMP_LTZ |
| `TIMESTAMP_TZ` | TIMESTAMP_TZ |
| `VARIANT` | VARIANT |
| `ARRAY` | ARRAY |
| `OBJECT` | OBJECT |

For parameterised types without a named constant (e.g. `NUMBER(18,2)`), pass a plain string:

```python
Column(name="price", type="NUMBER(18,2)")
```

## `Column`

```python
class Column(BaseModel):
    name:     str
    type:     str
    docs:     str = ""
    nullable: bool = True
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | required | Column name as it appears in Snowflake |
| `type` | `str` | required | Snowflake data type. Use `ColumnType` constants or a plain string for parameterised types. |
| `docs` | `str` | `""` | Documentation string. `clair docs` shows it. |
| `nullable` | `bool` | `True` | Tells you if the column accepts NULL values |

### Example

```python
from clair import Column, ColumnType

columns=[
    Column(name="order_id",    type=ColumnType.STRING,        nullable=False),
    Column(name="customer_id", type=ColumnType.STRING),
    Column(name="amount",      type="NUMBER(18,2)",           docs="The order total in USD"),
    Column(name="created_at",  type=ColumnType.TIMESTAMP_NTZ),
    Column(name="metadata",    type=ColumnType.VARIANT,       nullable=True),
]
```

## When columns are required

`columns` is optional for most Trouves. But UPSERT mode **needs** it, because clair uses the column list to build the MERGE statement. If you configure UPSERT but do not give `columns`, `clair run` raises `ValueError`.
