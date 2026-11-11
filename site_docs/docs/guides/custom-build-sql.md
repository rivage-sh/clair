# Write your own build with `build_sql()`

clair makes the SQL of a Trouve with `Table.build_sql()`. A subclass of `Table` can
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
    target_address: TrouveAddress,
    target_column_names: list[str] | None = None,
) -> BuildPlan: ...
```

| Argument | Meaning |
|----------|---------|
| `effective_mode` | `FULL_REFRESH` or `INCREMENTAL`. clair decides it: a VIEW, a first run, and a run without `--run-mode incremental` all give `FULL_REFRESH`. |
| `run_id` | The identifier of this run. Put it in the name of each object that the plan makes for one run, thus two runs never share a name. |
| `target_address` | The address to write. clair gives the staging address to a run with tests, and the physical address to a run without tests. Each statement that writes the object names this address. |
| `target_column_names` | The columns of the target table, in the order of the warehouse. clair reads them before an incremental run. It is `None` in a full refresh and in `clair compile`. Give it to `self.insert_statement()`. |

The method gives a `BuildPlan`:

| Field | Meaning |
|-------|---------|
| `statements` | The statements that write the object, in order. clair stops at the first statement that fails, and executes no statement after it. |
| `cleanup_statements` | The statements that remove each object that the plan makes for this run only. clair executes them after `statements`, when they pass and when one of them fails. |

A cleanup statement can run after any statement, thus it must be correct when the object
does not exist. Write `DROP TABLE IF EXISTS`. A failed cleanup statement does not fail the
Trouve, because the data stays correct. clair writes `run.node.cleanup_failure` to the log.

`build_sql()` takes no warehouse connection, and this is intentional. `clair compile`
calls it with no connection, thus a pull request shows the complete plan.

`self.require_compiled()` gives the record of the run: `resolved_sql` holds the SQL of the
Trouve, with each reference resolved, and `physical_address` holds the address that the
run promotes to. Name an object that the plan makes for one run after the physical
address, as `window_rows` does below. The staging address already holds the run id.

`resolved_sql` holds the SQL of the run mode of the command line. A full refresh must read
`self.full_refresh_sql()` instead, because a first run of `--run-mode incremental` is a
full refresh too, and the SQL obeys the effective mode. `super().build_sql()` reads it for
you.

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
        target_address: TrouveAddress,
        target_column_names: list[str] | None = None,
    ) -> BuildPlan:
        if effective_mode == RunMode.FULL_REFRESH:
            return super().build_sql(effective_mode, run_id, target_address)

        compiled = self.require_compiled()
        window_rows = f"{compiled.physical_address}__window_{run_id}"
        create_window_rows = (
            f"CREATE OR REPLACE TABLE {window_rows} AS (\n{compiled.resolved_sql}\n)"
        )
        delete_window = (
            f"DELETE FROM {target_address}\n"
            f"WHERE {self.window_column} >= (SELECT MIN({self.window_column}) FROM {window_rows})"
        )
        insert_window = self.insert_statement(str(target_address), window_rows, target_column_names)
        return BuildPlan(
            statements=[create_window_rows, delete_window, insert_window],
            cleanup_statements=[f"DROP TABLE IF EXISTS {window_rows}"],
        )
```

The subclass adds the field `window_column`. A field on a subclass is a normal Pydantic
field, thus clair validates it when the Trouve file runs.

For a full refresh, `super().build_sql()` gives the normal `CREATE OR REPLACE TABLE`. For
an incremental run, the plan writes the rows of the window to a table of their own, deletes
the window from the target, and inserts the rows.

`self.insert_statement()` names each column of the target in the `INSERT`. Do not write
`INSERT INTO target SELECT * FROM ...`. That statement matches the columns by position, thus
two columns that change places in the SQL swap their values, and Snowflake gives no error. The drop of the window table is a cleanup
statement, thus it runs when the `DELETE` or the `INSERT` fails too.

The built-in `DeleteInsert(range_column=...)` deletes each row between the minimum and the
maximum of the new rows. This subclass deletes each row from the minimum on, thus it also
removes each row after the last new row.

## 2. Use the subclass in a Trouve file

```python
# example_3_database/derived/daily_revenue.py
from _replace_window_trouve import ReplaceWindowTrouve
from example_3_database.refined.orders import trouve as refined_orders

import clair
from clair import (
    THIS,
    Column,
    ColumnType,
    Incremental,
    RunMode,
    TestUnique,
)

sql = f"""
    select
        created_date,
        count(*)    as order_count,
        sum(amount) as revenue
    from {refined_orders}
"""
if clair.run_mode == RunMode.INCREMENTAL:
    sql += f"""
        where created_date >= dateadd('day', -3, (select max(created_date) from {THIS}))
    """
sql += """
    group by created_date
"""

trouve = ReplaceWindowTrouve(
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
`Table` with no override and no strategy raises a `CompileError` on an incremental run.

## 3. Read the plan

```bash
clair compile --env dev --run-mode incremental --select example_3_database.derived.daily_revenue
```

The compiled file shows the clone, the three statements, the cleanup, the test step, and
the promotion, in the order that clair executes them.

## 4. Validate the override

```bash
clair validate --env dev
```

`clair validate` calls `build_sql()` for each run mode, with the staging address and with
the physical address, and needs no connection. It reports an override that raises, and an
override that ignores `target_address`. See [`clair validate`](../cli/validate.md#a-build-plan-that-clair-cannot-make).
