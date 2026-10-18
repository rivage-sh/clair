# Write your own build with `build_sql()`

clair makes the SQL of a Trouve with `Trouve.build_sql()`. A subclass of `Trouve` can
override it, and give each statement that it wants. Use it when the built-in strategies —
`Append`, `Upsert`, and `DeleteInsert` — do not do what the table needs.

This guide makes one such subclass: an incremental table that replaces a window of recent
days. The code is in `examples/projects/example_3`.

## What clair keeps, and what you give

`build_sql()` gives the statements that **write** the object. clair keeps each step around
them, and a subclass cannot change these steps:

- The clone of the physical table to the staging address, before an incremental build.
- The data quality tests on the staging object.
- The promotion of the staging object to the physical address, and the drop of the staging
  object.

Thus an override gets staging, tests, and promotion with no work. See
[The Anatomy of a Run](../topics/anatomy-of-a-run.md) for each step.

## The contract

```python
def build_sql(
    self,
    effective_mode: RunMode,
    run_id: str,
    staging_address: TrouveAddress | None = None,
) -> BuildPlan: ...
```

| Argument | Meaning |
|----------|---------|
| `effective_mode` | `FULL_REFRESH` or `INCREMENTAL`. clair decides it: a VIEW, a first run, and a run without `--run-mode incremental` all give `FULL_REFRESH`. |
| `run_id` | The identifier of this run. Put it in the name of each object that the plan makes for one run, thus two runs never share a name. |
| `staging_address` | The address to write. When it is `None`, write to `self.physical_address`. Write `staging_address or self.physical_address`. |

The method gives a `BuildPlan`:

| Field | Meaning |
|-------|---------|
| `statements` | The statements that write the object, in order. clair stops at the first statement that fails, and executes no statement after it. |
| `cleanup_statements` | The statements that remove each object that the plan makes for this run only. clair executes them after `statements`, when they pass and when one of them fails. |

A cleanup statement can run after any statement, thus it must be correct when the object
does not exist. Write `DROP TABLE IF EXISTS`. A failed cleanup statement does not fail the
Trouve, because the data stays correct. clair writes `run.node.cleanup_failure` to the log.

`build_sql()` takes no warehouse connection, and this is intentional. `clair compile`
calls it with no connection, thus a pull request shows the complete plan. Read
`self.compiled.resolved_sql` for the SQL of the Trouve, with each reference resolved.

## 1. Write the subclass

Put the subclass in a file that starts with `_`. Discovery skips such a file, thus it
holds Python that a Trouve file imports, and no Trouve.

```python
# _replace_window_trouve.py
from clair import BuildPlan, RunMode, Table, TrouveAddress


class ReplaceWindowTrouve(Table):
    """An incremental TABLE that replaces each row of a recent window.

    A full refresh builds the table in the same way as a Trouve. An incremental
    run deletes each row of the window from the target, and inserts the rows of
    the window again. Thus a late row lands, and a changed row replaces its old
    version.
    """

    window_column: str

    def build_sql(
        self,
        effective_mode: RunMode,
        run_id: str,
        staging_address: TrouveAddress | None = None,
    ) -> BuildPlan:
        if effective_mode == RunMode.FULL_REFRESH:
            return super().build_sql(effective_mode, run_id, staging_address)

        assert self.compiled is not None
        target = staging_address or self.physical_address
        window_rows = f"{self.physical_address}__window_{run_id}"
        create_window_rows = (
            f"CREATE OR REPLACE TABLE {window_rows} AS (\n{self.compiled.resolved_sql}\n)"
        )
        delete_window = (
            f"DELETE FROM {target}\n"
            f"WHERE {self.window_column} >= (SELECT MIN({self.window_column}) FROM {window_rows})"
        )
        insert_window = f"INSERT INTO {target}\nSELECT * FROM {window_rows}"
        return BuildPlan(
            statements=[create_window_rows, delete_window, insert_window],
            cleanup_statements=[f"DROP TABLE IF EXISTS {window_rows}"],
        )
```

The subclass adds the field `window_column`. A field on a subclass is a normal Pydantic
field, thus clair validates it when the Trouve file runs.

For a full refresh, `super().build_sql()` gives the normal `CREATE OR REPLACE TABLE`. For
an incremental run, the plan writes the rows of the window to a table of their own, deletes
the window from the target, and inserts the rows. The drop of the window table is a cleanup
statement, thus it runs when the `DELETE` or the `INSERT` fails too.

## 2. Use the subclass in a Trouve file

```python
# example_3_database/derived/daily_revenue.py
from _replace_window_trouve import ReplaceWindowTrouve
from example_3_database.refined.orders import trouve as refined_orders

import clair
from clair import (
    Column,
    ColumnType,
    Incremental,
    RunMode,
    TestUnique,
    TrouveType,
)

sql = f"""
    select
        created_date,
        count(*)    as order_count,
        sum(amount) as revenue
    from {refined_orders}
"""
if clair.run_mode == RunMode.INCREMENTAL:
    sql += """
        where created_date >= dateadd('day', -3, current_date())
    """
sql += """
    group by created_date
"""

trouve = ReplaceWindowTrouve(
    type=TrouveType.TABLE,
    docs="""
        The revenue of each day. In each incremental run, clair replaces the last 3 days,
        thus a late order changes the total of its day. ReplaceWindowTrouve overrides
        build_sql(), and its plan writes the incremental SQL.
    """,
    sql=sql,
    window_column="created_date",
    run_config=Incremental(),
    columns=[
        Column(name="created_date", type=ColumnType.DATE),
        Column(name="order_count", type=ColumnType.INTEGER),
        Column(name="revenue", type=ColumnType.FLOAT),
    ],
    tests=[TestUnique(column="created_date")],
)
```

`Incremental()` names no strategy, because the override writes the incremental SQL. A
`Trouve` with no override and no strategy raises a `CompileError` on an incremental run.

## 3. Read the plan

```bash
clair compile --run-mode incremental --select example_3_database.derived.daily_revenue
```

The compiled file shows the clone, the three statements, the cleanup, the test step, and
the promotion, in the order that clair executes them.
