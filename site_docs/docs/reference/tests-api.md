# Tests API

```python
from clair import TestUnique, TestNotNull, TestRowCount, TestUniqueColumns
```

All the tests are Pydantic models. Zero rows from `to_sql()` = pass.

## `TestUnique`

Assert that a column has no duplicate values.

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

Assert that a column has no NULL values.

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

Assert that the number of rows is between two limits. You must set `min_rows` or `max_rows`, or both.

```python
class TestRowCount(Test):
    min_rows: int | None = None
    max_rows: int | None = None
```

```python
TestRowCount(min_rows=1)                      # 1 row minimum
TestRowCount(max_rows=1_000_000)              # 1M rows maximum
TestRowCount(min_rows=100, max_rows=10_000)   # between the two limits
```

Generated SQL for `min_rows=100, max_rows=10_000`:

```sql
SELECT 1 FROM mydb.myschema.mytable HAVING COUNT(*) < 100
UNION ALL
SELECT 1 FROM mydb.myschema.mytable HAVING COUNT(*) > 10000
```

Run with `--sample`: **No** — clair skips it, because a row count has no meaning on sampled data.

## `TestUniqueColumns`

Assert that a group of columns is unique in all the rows. You must give 2 columns or more.

```python
class TestUniqueColumns(Test):
    columns: list[str]   # 2 entries minimum
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

clair uses this discriminated union internally to deserialize the tests:

```python
AnyTest = TestUnique | TestNotNull | TestRowCount | TestUniqueColumns
```

Each test class has a `type` literal field. clair uses it as the discriminator: `"unique"`, `"not_null"`, `"row_count"`, `"unique_columns"`.

## See also

- [Data Quality Tests guide](../guides/data-quality-tests.md)
