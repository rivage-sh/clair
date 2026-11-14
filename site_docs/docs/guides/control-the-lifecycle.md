# Control the lifecycle of a Trouve

clair sends each statement that materializes a Trouve from one method: `plan()`. The
method calls one method for each phase of the lifecycle, in a fixed order. Each phase
method has a default, and the defaults give the safe path: clair writes a candidate,
tests it, and then promotes it to production.

You can change each statement. Override one phase method to change one step. Override
`plan()` to change many steps together. Override a policy method to change a decision.
clair keeps the order of the phases, and it executes each phase at the correct time.

This guide gives the lifecycle, the facts that a phase reads, and three overrides. The
code of the first two is in `examples/projects/example_3`.

## The lifecycle

The runner executes the phases in this order. Each phase holds a list of statements, and
a phase can be empty.

| # | Phase | What it does | The default | When it runs |
|---|-------|--------------|-------------|--------------|
| 1 | `prepare` | Makes the target ready for the build. | An incremental `Table` clones production to the staging address, and adds each new column. | Each run |
| 2 | `build` | Writes the rows. | `CREATE OR REPLACE TABLE ... AS`, or the statements of the incremental strategy. | Each run |
| 3 | `configure` | Sets the properties of the table after the write. | The clustering key. | Each run |
| 4 | `test` | Gives the data quality tests as queries. Each row of a result is a failure. | One query for each test of the Trouve. | Each run with tests |
| 5 | `promote` | Makes production hold the tested data. | `CREATE OR REPLACE TABLE <physical> CLONE <staging> COPY GRANTS`. A `View` makes the view again. | A staged run, when `should_promote()` gives `True` |
| 6 | `document` | Writes the docs as Snowflake comments on production. | Each comment that differs from the `docs`. | Each run |
| 7 | `retire` | Deals with the promoted staging object. | `DROP TABLE IF EXISTS <staging>`. | A staged run, after a promotion |
| 8 | `cleanup` | Removes each object that this run made for itself. | The new rows table of an `Upsert` or a `DeleteInsert`. | Each run, last, also after a failure and a cancel |

Two policy methods decide, and send no SQL:

| Method | When clair calls it | The default |
|--------|---------------------|-------------|
| `effective_mode(requested_mode, current_object)` | Before `plan()`. | `INCREMENTAL` when the command line asks for it, the `run_config` is `Incremental`, and production holds a table. Else `FULL_REFRESH`. |
| `should_promote(context, test_results)` | After the tests of a staged run. | `True` when no blocking test failed, and production holds no object of a different type. |

`plan()` gives a `NodePlan`, with one field for each phase:

```python
def plan(self, context: PlanContext) -> NodePlan:
    return NodePlan(
        prepare=self.prepare(context),
        build=self.build(context),
        configure=self.configure(context),
        test=self.test(context),
        promote=self.promote(context) if context.staged else [],
        document=self.document(context),
        retire=self.retire(context) if context.staged else [],
        cleanup=self.cleanup(context),
    )
```

## How the runner executes a plan

| Phase | A statement fails |
|-------|-------------------|
| `prepare`, `build`, `configure` | The node fails, and clair sends no more of these statements. Production keeps its data. A staged run keeps the candidate at the staging address. |
| `test` | A query that fails counts as a failed test. |
| `promote` | The node fails, and the candidate stays. If the first statement succeeded, the error tells you that production holds the tested data, and it names the statement to run again. |
| `document` | The node fails. Production holds the new data, and the error names the statement to run again. |
| `retire` | clair writes `run.node.staging_drop_failed` to the log. The node stays a success. |
| `cleanup` | clair writes `run.node.cleanup_failure` to the log, and executes the next cleanup statement. The node keeps its status. |

A run with `--no-test` has no staging step: the build writes production, `promote` and
`retire` stay empty, and no test runs. A cancel stops the node before its next statement.
The cleanup still runs, and the promotion does not.

A cleanup statement can run after any statement, thus it must be correct when its object
does not exist. Write `DROP TABLE IF EXISTS`.

## What a phase reads: `PlanContext`

`plan()` opens no connection. The runner reads the warehouse first, and gives what it read
in a `PlanContext`. `clair compile` and `clair validate` make the same plan with no
connection, thus a pull request shows the complete plan.

| Field | Meaning |
|-------|---------|
| `run_id` | The identifier of this run. Put it in the name of each object that the plan makes for this run only. |
| `effective_mode` | What `effective_mode()` gave: `FULL_REFRESH` or `INCREMENTAL`. |
| `staged` | `True` when the run tests a candidate before the promotion. `False` for `--no-test`. |
| `offline` | `True` in `clair compile` and `clair validate`. The fields that clair reads from the warehouse are then `None`. |
| `physical_address` | The production address. |
| `staging_address` | The address of the candidate, or `None` when `staged` is `False`. |
| `target_address` | The address that `prepare`, `build`, `configure` and `test` write and read: the staging address of a staged run, else the physical address. |
| `current_object` | The object at the physical address before the run, with its type and its comment. `None` when the address holds nothing. |
| `target_columns` | The columns of the production table, in the order of the warehouse. An incremental run only. |
| `added_columns` | The columns that the SQL gives, and that the production table does not have. |
| `target_column_names` | Each column of `target_columns`, then each column of `added_columns`. Give it to `insert_statement()`. |
| `current_comments` | The comments of the production table before the run. |

## The building blocks

A `Table` gives clair's own SQL as public methods. An override calls them, and does not
copy SQL from the source of clair.

| Method | Gives |
|--------|-------|
| `create_statement(address)` | The `CREATE OR REPLACE TABLE ... AS` of a full refresh. |
| `create_new_rows_statement(run_id)` | The table that holds the new rows of this run, at `new_rows_address(run_id)`. |
| `merge_statement(target_address, source_address)` | The `MERGE` of the `Upsert` strategy. |
| `delete_replaced_rows_statement(target_address, source_address)` | The `DELETE` of the `DeleteInsert` strategy. |
| `insert_statement(address, new_rows, target_column_names)` | An `INSERT` that names each column. `new_rows` is a table name, or a subquery in parentheses. |
| `clone_statement(source_address, clone_address)` | A zero-copy clone. |
| `add_column_statements(address, added_columns)` | One `ALTER TABLE ... ADD COLUMN` for each column. |
| `cluster_by_statements(address, incremental=...)` | The `ALTER` that sets or drops the clustering key. |
| `clone_promote_statements(staging_address, physical_address)` | The default promotion. |
| `comment_statements(address, current_comments)` | The comments of the Trouve and its columns. |
| `drop_statement(address)` | A `DROP TABLE IF EXISTS` or a `DROP VIEW IF EXISTS`. |
| `resolved_sql()`, `full_refresh_sql()` | The SQL of the Trouve, with each reference resolved. |

`super()` gives the default of each phase. An override often keeps the default for one
mode, and changes the other.

Put a subclass in a file that starts with `_`. Discovery skips such a file, thus it holds
Python that a Trouve file imports, and no Trouve.

## 1. Keep the production table object

The default promotion replaces the production table with a clone of the candidate. A clone
is fast for a table of each size, but it is a new object. A Snowflake feature that attaches
to the object, and not to the name, starts again at each run: Time Travel before the run,
a stream, and the change tracking of a dynamic table.

`MirroredUpsert` keeps the object. clair tests the `MERGE` on a clone of production, as
for each `Table`. The promotion then runs the same `MERGE` into production, from the same
new rows table.

```python
# _mirrored_upsert.py
from clair import PlanContext, RunMode, Table


class MirroredUpsert(Table):
    """An Upsert that keeps the production table object.

    clair tests the MERGE on a clone of production. The promotion then runs the
    same MERGE into production, from the same new rows table, and does not
    replace the table. A full refresh still replaces it.
    """

    def promote(self, context: PlanContext) -> list[str]:
        if context.effective_mode == RunMode.FULL_REFRESH:
            return super().promote(context)
        return [
            # prepare added each new column to the clone. Production needs them too.
            *self.add_column_statements(context.physical_address, context.added_columns),
            self.merge_statement(
                context.physical_address,
                self.new_rows_address(context.run_id),
            ),
        ]
```

```python
# example_3_database/derived/customer_order_totals.py
from _mirrored_upsert import MirroredUpsert
from example_3_database.refined.orders import trouve as refined_orders

from clair import Column, ColumnType, Incremental, TestUnique, Upsert

trouve = MirroredUpsert(
    docs="""
        The totals of each customer, merged in place. A stream on this table keeps reading.

        clair tests the MERGE on a clone, as for each Table. MirroredUpsert then runs the
        same MERGE into this table, and does not replace it. Thus the table keeps its Time
        Travel history, and a stream or a dynamic table on it keeps working.
    """,
    sql=f"""
        select
            customer_id,
            count(*)    as total_orders,
            sum(amount) as total_amount
        from {refined_orders}
        group by customer_id
    """,
    run_config=Incremental(strategy=Upsert(primary_key_columns=["customer_id"])),
    columns=[
        Column(name="customer_id", type=ColumnType.STRING),
        Column(name="total_orders", type=ColumnType.NUMBER),
        Column(name="total_amount", type=ColumnType.FLOAT),
    ],
    tests=[TestUnique(column="customer_id")],
)
```

The subclass overrides one phase, and adds no test for `--no-test`: clair calls `promote()`
only in a staged run. A run with `--no-test` merges into production in the `build` phase.

The cleanup runs after the promotion, thus the new rows table still exists when the second
`MERGE` reads it. The cost is a second `MERGE` in each incremental run. A full refresh
still replaces the table. An integration test of clair puts a stream on this table, and
reads it after an incremental run.

## 2. Keep each candidate as a snapshot

The default `retire` drops the staging object after the promotion. `AuditedTable` keeps
the candidate of each run as a snapshot in its place:

```python
# _audited_table.py
from clair import PlanContext, Table


class AuditedTable(Table):
    """Keep each promoted candidate as a snapshot, and do not drop it."""

    def retire(self, context: PlanContext) -> list[str]:
        snapshot = f"{context.physical_address}__audit_{context.run_id}"
        return [f"ALTER TABLE {context.staging_address} RENAME TO {snapshot}"]
```

```python
# example_3_database/derived/order_status_counts.py
from _audited_table import AuditedTable
from example_3_database.refined.orders import trouve as refined_orders

from clair import Column, ColumnType, TestNotNull, TestUnique

trouve = AuditedTable(
    docs="""
        The number of orders in each status. Each run keeps its tested candidate as a snapshot.

        AuditedTable renames the promoted staging table to order_status_counts__audit_<run_id>,
        and does not drop it. Thus each run leaves a copy of the data that passed its tests.
    """,
    sql=f"""
        select
            order_status,
            count(*) as order_count
        from {refined_orders}
        group by order_status
    """,
    columns=[
        Column(name="order_status", type=ColumnType.STRING),
        Column(name="order_count", type=ColumnType.NUMBER),
    ],
    tests=[TestUnique(column="order_status"), TestNotNull(column="order_status")],
)
```

`retire()` runs only after a promotion, thus each snapshot holds data that passed its
tests. The rename costs nothing: the candidate already exists, and clair renames it in the
metadata. clair does not drop the snapshots. Drop the old ones on a schedule. An
integration test of clair runs this Trouve, and reads the snapshot.

## 3. Never rebuild a table by accident

An incremental run changes to a full refresh when production holds no table. For a large
table, a full refresh can cost more than a failure. Override the policy:

```python
from clair import RunMode, Table, WarehouseObject
from clair.exceptions import RunError


class NeverRebuild(Table):
    """Fail the node, and do not change to a full refresh, when the table is missing."""

    def effective_mode(
        self, requested_mode: RunMode, current_object: WarehouseObject | None
    ) -> RunMode:
        if requested_mode == RunMode.INCREMENTAL and current_object is None:
            raise RunError(
                f"{self.logical_address} has no table. Run --run-mode full_refresh on purpose."
            )
        return super().effective_mode(requested_mode, current_object)
```

The error fails this node only, and clair skips each Trouve downstream of it. `clair
compile` gives `effective_mode()` an object of the type of the Trouve, thus the compile
does not raise.

## Read the plan

```bash
clair compile --env dev --run-mode incremental --select example_3_database.derived.customer_order_totals
```

The compiled file names each phase, in the order of the run. For `MirroredUpsert`, the
`promote` phase shows the `MERGE` into production in place of the clone:

```sql
-- ── promote · when should_promote() gives True ──────────────
-- object type: if <physical> holds an object of a type other than TABLE, clair stops here and writes nothing to it
-- merge the new rows into the target table
MERGE INTO <physical> AS target
USING <physical>__clair_new_rows_<run_id> AS source
ON target.customer_id = source.customer_id
...
```

## Read the record of a run

Each statement of a run result names its `Phase`, thus the record of a run has the same
shape as the plan. The artifact file of each Trouve in `.clair/artifacts/<run_id>/` gives
each phase a section with the header of `clair compile`. The `test` section holds each
test query and its outcome:

```sql
-- ── test · a blocking test that fails stops the promotion ────
-- statement 4/7: success, query ID 01b2...
-- unique (customer_id), ERROR: passed
SELECT customer_id, COUNT(*) FROM <staging> GROUP BY customer_id HAVING COUNT(*) > 1
```

A failure names its phase. The summary of `clair run` says `FAILED in promote`, and
`RunFailure.failed_phase` gives the same answer to a caller of `clair.run()`. A statement
that clair did not send keeps its phase, with the status `not_run`. See
[`Phase`](../reference/python-api.md#phase).

## Validate the plan

```bash
clair validate --env dev
```

`clair validate` calls `plan()` for each run mode, with staging and without staging, and
needs no connection. It reports a plan that raises, and a `build` phase that does not
write to `context.target_address`. See
[`clair validate`](../cli/validate.md#a-build-plan-that-clair-cannot-make).

## What stays outside a Trouve

- **The reads.** clair reads `INFORMATION_SCHEMA` and `DESCRIBE TABLE` before the plan,
  because `plan()` has no connection. You get the results in `PlanContext`.
- **The session.** `USE ROLE`, `USE WAREHOUSE`, the query tag and the statement timeout
  belong to the connection. Set their values on the Trouve, or in `__schema_config__.py`.
  See [Per-database and per-schema config](../topics/per-database-schema-config.md).
- **The database and the schema.** clair sends `CREATE DATABASE IF NOT EXISTS` and
  `CREATE SCHEMA IF NOT EXISTS` one time before the run, because many Trouves share one
  schema.

A `PandasTable` and a `SeedTable` have the same phases. clair writes their DataFrame
before the `build` phase, with `write_pandas`. That write is not SQL, thus you cannot
change it, and the statements of the `build` phase run after it.

## See also

- [The Anatomy of a Run](../topics/anatomy-of-a-run.md)
- [Staging](../topics/staging.md)
- [Incrementality](../topics/incrementality.md)
- [Trouve API](../reference/trouve-api.md)
