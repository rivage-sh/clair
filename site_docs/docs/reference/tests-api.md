# Tests API

```python
from clair import TestUnique, TestNotNull, TestRowCount, TestUniqueColumns
```

All tests are Pydantic models. Zero rows returned by `to_sql()` = pass.

## `TestUnique`

Assert that a column contains no duplicate values.

```python
class TestUnique(Test):
    column: str
```

```python
TestUnique(column="order_id")
```

Generated SQL:

```sql
SELECT order_id, COUNT(*)
FROM mydb.myschema.mytable
GROUP BY order_id
HAVING COUNT(*) > 1
```

Run with `--sample`: **Yes**

## `TestNotNull`

Assert that a column contains no NULL values.

```python
class TestNotNull(Test):
    column: str
```

```python
TestNotNull(column="customer_id")
```

Generated SQL:

```sql
SELECT customer_id
FROM mydb.myschema.mytable
WHERE customer_id IS NULL
```

Run with `--sample`: **Yes**

## `TestRowCount`

Assert that the table row count falls within the given bounds. At least one of `min_rows` or `max_rows` must be set.

```python
class TestRowCount(Test):
    min_rows: int | None = None
    max_rows: int | None = None
```

```python
TestRowCount(min_rows=1)                      # at least 1 row
TestRowCount(max_rows=1_000_000)              # no more than 1M rows
TestRowCount(min_rows=100, max_rows=10_000)   # within range
```

Generated SQL (for `min_rows=100, max_rows=10_000`):

```sql
SELECT 1 FROM mydb.myschema.mytable HAVING COUNT(*) < 100
UNION ALL
SELECT 1 FROM mydb.myschema.mytable HAVING COUNT(*) > 10000
```

Run with `--sample`: **No** — skipped because row counts on sampled data are meaningless.

## `TestUniqueColumns`

Assert that a combination of columns is unique across all rows. Requires at least 2 columns.

```python
class TestUniqueColumns(Test):
    columns: list[str]   # minimum 2 entries
```

```python
TestUniqueColumns(columns=["customer_id", "created_date"])
```

Generated SQL:

```sql
SELECT customer_id, created_date, COUNT(*)
FROM mydb.myschema.mytable
GROUP BY customer_id, created_date
HAVING COUNT(*) > 1
```

Run with `--sample`: **Yes**

## Summary

| Class | Args | Run with `--sample`? |
|-------|------|----------------------|
| `TestUnique` | `column: str` | Yes |
| `TestNotNull` | `column: str` | Yes |
| `TestRowCount` | `min_rows: int \| None`, `max_rows: int \| None` | **No** |
| `TestUniqueColumns` | `columns: list[str]` (min 2) | Yes |

## `AnyTest`

The discriminated union used internally by clair to deserialize tests:

```python
AnyTest = TestUnique | TestNotNull | TestRowCount | TestUniqueColumns
```

Each test class has a `type` literal field used as discriminator: `"unique"`, `"not_null"`, `"row_count"`, `"unique_columns"`.

## See also

- [Data Quality Tests guide](../guides/data-quality-tests.md)
