# Data Quality Tests

Attach tests to any TABLE or VIEW Trouve. Tests run automatically after each successful node during `clair run`, and can also be run standalone with `clair test`.

## Attaching tests

```python
from clair import (
    Trouve, TrouveType,
    TestNotNull, TestRowCount, TestUnique, TestUniqueColumns,
)

trouve = Trouve(
    type=TrouveType.TABLE,
    sql=f"SELECT * FROM {upstream}",
    tests=[
        TestUnique(column="order_id"),
        TestNotNull(column="customer_id"),
        TestRowCount(min_rows=1),
        TestUniqueColumns(columns=["customer_id", "created_date"]),
    ],
)
```

## Test types

### `TestUnique`

Assert that a column contains no duplicate values.

```python
TestUnique(column="order_id")
```

Generated SQL:

```sql
SELECT order_id, COUNT(*)
FROM refined.orders.daily
GROUP BY order_id
HAVING COUNT(*) > 1
```

Zero rows returned = pass.

### `TestNotNull`

Assert that a column contains no NULL values.

```python
TestNotNull(column="customer_id")
```

Generated SQL:

```sql
SELECT customer_id
FROM refined.orders.daily
WHERE customer_id IS NULL
```

### `TestRowCount`

Assert that the table row count falls within bounds. At least one of `min_rows` or `max_rows` must be set.

```python
TestRowCount(min_rows=1)              # at least 1 row
TestRowCount(max_rows=1_000_000)      # no more than 1M rows
TestRowCount(min_rows=100, max_rows=10_000)  # within range
```

Generated SQL (for `min_rows=100, max_rows=10_000`):

```sql
SELECT 1 FROM refined.orders.daily HAVING COUNT(*) < 100
UNION ALL
SELECT 1 FROM refined.orders.daily HAVING COUNT(*) > 10000
```

!!! note
    `TestRowCount` is skipped when running with `--sample`. Row counts are meaningless on sampled data.

### `TestUniqueColumns`

Assert that a combination of columns is unique across all rows. Requires at least 2 columns.

```python
TestUniqueColumns(columns=["customer_id", "created_date"])
```

Generated SQL:

```sql
SELECT customer_id, created_date, COUNT(*)
FROM refined.orders.daily
GROUP BY customer_id, created_date
HAVING COUNT(*) > 1
```

## Pass/fail semantics

Every test generates a SQL query. **Zero returned rows = pass. Any rows returned = fail.**

## Running tests

**Automatically after each successful node:**

```bash
clair run --project . --env dev
# tests run after each TABLE/VIEW succeeds
```

**Skip tests during a run:**

```bash
clair run --project . --env dev --no-test
```

**Standalone test run:**

```bash
clair test --project . --env dev
clair test --project . --env dev --select='refined.orders.*'
```

**Sampled testing** (most tests run against `SELECT TOP 1000 *`; `TestRowCount` is skipped):

```bash
clair run --project . --env dev --sample
clair test --project . --env dev --sample
```

## Test reference

| Class | Args | Skipped with `--sample`? |
|-------|------|--------------------------|
| `TestUnique` | `column: str` | No |
| `TestNotNull` | `column: str` | No |
| `TestRowCount` | `min_rows: int \| None`, `max_rows: int \| None` | **Yes** |
| `TestUniqueColumns` | `columns: list[str]` (min 2) | No |

See also: [Tests API reference](../reference/tests-api.md).
