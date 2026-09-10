# Data Quality Tests

Attach tests to any TABLE or VIEW Trouve. clair runs the tests after each successful node in `clair run`. You can also run them alone with `clair test`.

## Attach tests

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

Assert that a column has no duplicate values.

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

Zero rows in the result = pass.

### `TestNotNull`

Assert that a column has no NULL values.

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

Assert that the number of rows is between two limits. You must set `min_rows` or `max_rows`, or both.

```python
TestRowCount(min_rows=1)              # 1 row minimum
TestRowCount(max_rows=1_000_000)      # 1M rows maximum
TestRowCount(min_rows=100, max_rows=10_000)  # between the two limits
```

Generated SQL for `min_rows=100, max_rows=10_000`:

```sql
SELECT 1 FROM refined.orders.daily HAVING COUNT(*) < 100
UNION ALL
SELECT 1 FROM refined.orders.daily HAVING COUNT(*) > 10000
```

!!! note
    clair skips `TestRowCount` if you give `--sample`. A row count has no meaning on sampled data.

### `TestUniqueColumns`

Assert that a group of columns is unique in all the rows. You must give 2 columns or more.

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

Every test generates a SQL query. **Zero rows in the result = pass. One row or more = fail.**

## Run the tests

**Automatically after each successful node:**

```bash
clair run --env dev
# the tests run after each successful TABLE or VIEW
```

**Skip the tests in a run:**

```bash
clair run --env dev --no-test
```

**A test run on its own:**

```bash
clair test --env dev
clair test --env dev --select='refined.orders.*'
```

**Tests on a sample.** Most tests run against `SELECT TOP 1000 *`. clair skips `TestRowCount`:

```bash
clair run --env dev --sample
clair test --env dev --sample
```

## Test reference

| Class | Args | Does `--sample` skip it? |
|-------|------|--------------------------|
| `TestUnique` | `column: str` | No |
| `TestNotNull` | `column: str` | No |
| `TestRowCount` | `min_rows: int \| None`, `max_rows: int \| None` | **Yes** |
| `TestUniqueColumns` | `columns: list[str]` (min 2) | No |

See also: [Tests API reference](../reference/tests-api.md).

## Testing before publishing

A test runs after clair materializes a Trouve, because only then does a table exist to query. A direct write would thus tell you that production is already wrong. [Staging](staging.md) changes the order, and each run works this way: clair writes each Trouve to a staging address, runs the tests there, and gives the data its physical address only after each test passes.

```bash
clair run --env prod
```
