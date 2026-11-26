# Staging

Snowflake can test a table only after the table exists. A direct write therefore puts untested data into production first, and the tests report the fault after a reader saw the wrong numbers.

Staging closes that window, and it is **how clair writes**. There is no flag. Clair writes each Trouve to a run-scoped staging address, runs the tests there, and gives the data its physical address only after each test passes.

```bash
clair run --env prod
```

`--no-test` is the one flag that stops this. It removes the tests that decide the promotion, so a staging address protects nothing. That run writes to each physical address directly.

## The three addresses

clair gives each Trouve three addresses, and uses these three names everywhere:

| The address | What it is |
|-------------|-----------|
| logical address | The name that the file path gives. The DAG edges and the selectors use it. |
| physical address | The name that clair writes to. [Routing](routing.md) makes it from the logical address. |
| staging address | The physical address plus the run-scoped suffix `__clair_<run_id>`. The data goes there first. |

## The steps for one Trouve

1. **Build at the staging address.** Clair materializes the Trouve as `<table>__clair_<run_id>`, beside the physical object in the same database and the same schema.
2. **Test the staging object.** The data quality tests of the Trouve examine the staging object, and not the physical object.
3. **Promote after the tests pass.** For a `TABLE`, clair runs `CREATE OR REPLACE TABLE <physical> CLONE <staging>`. Snowflake does this in the metadata, so the time does not grow with the table size. Clair then drops the staging copy. For a `VIEW`, clair runs `CREATE OR REPLACE VIEW ...`. The new object gets the future grants of its schema: see [Grants](#grants).
4. **Keep the candidate after a failure.** The physical object keeps the data that it had. Clair does not drop the staging object — see [After a failure](#after-a-failure). The Trouve gets the FAILED status, and clair skips each Trouve downstream of it.

Clair promotes each node immediately after the tests of that node, and before the next node starts. A dependent thus reads its upstream at the physical address that its SQL names. You never write `__clair_` in a Trouve file.

## Grants

Clair supports one model of access control: **future grants on the schema**. Each object in one schema has the same sensitivity, and access to a schema is all or nothing: a role reads each object of the schema, or none. Give each role its access to the schema:

```sql
GRANT USAGE ON SCHEMA analytics.finance TO ROLE finance_reader;
GRANT SELECT ON FUTURE TABLES IN SCHEMA analytics.finance TO ROLE finance_reader;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA analytics.finance TO ROLE finance_reader;
```

Snowflake attaches a privilege to the *object*, and not to the name, and clair makes a new object on each run. A future grant belongs to the schema, thus each new object gets its privileges when Snowflake makes it, and no statement of clair carries a grant. If two tables need different access, they go in two schemas. The directory tree of a project gives the schemas, thus `tree` shows the access model.

This is a position. Clair takes others of the same kind: for example, it has no freshness checks, because freshness is metadata about the data, and not a part of the transformation. In the same way, clair builds objects, and the access to them is a fact of the schema. dbt supports a grant on one object, with `copy_grants` and the `grants` config. Clair does not, and its statements stay simple: no statement copies a grant, on any path.

### What each statement keeps

A probe in Snowflake gave these results, in a schema with a future grant of SELECT, and a grant of INSERT on each existing table:

| The statement | The future grant | The grant on the object |
|---|---|---|
| `CREATE` of a new table, view or clone | Kept | — |
| `CREATE OR REPLACE`, with `AS` or `CLONE` | Kept | Lost |
| `CREATE OR REPLACE ... COPY GRANTS` | Kept | Kept |
| `ALTER TABLE ... SWAP WITH` | Kept | Lost |
| `write_pandas` with `overwrite=True` | Kept | Lost |

Each statement thus works with future grants. Clair sends the second row: the build, the promotion of a staged run, and the build of a run with `--no-test`, for each kind of Trouve. A grant on one object therefore goes at the next run that replaces production.

`OWNERSHIP` of each new object goes to the role that runs clair.

### Give a grant on one object yourself

Clair does not stop you. If you must keep a grant on one object, give it again in the `document` phase. Clair runs that phase on the physical object, after the promotion of a staged run and after the write of a run with `--no-test`, for a SQL Trouve and for a DataFrame Trouve:

```python
from clair import PlanContext, Table


class AuditedTable(Table):
    def document(self, context: PlanContext) -> list[str]:
        return [
            *super().document(context),
            f"GRANT SELECT ON TABLE {context.physical_address} TO ROLE auditor",
        ]
```

The same override works on a subclass of `PandasTable` or `SeedTable`. To copy each grant of the old object in place of one grant that you name, override `promote()` and add `COPY GRANTS` to its statement. See [Control the lifecycle of a Trouve](../guides/control-the-lifecycle.md).

## An incremental Trouve

An incremental Trouve changes data that already exists, so the staging object needs that data before the `INSERT` or the `MERGE` runs. Clair puts it there with a zero-copy clone:

```sql
-- ── prepare
CREATE OR REPLACE TABLE db.schema.orders__clair_<run_id> CLONE db.schema.orders

-- ── build
INSERT INTO db.schema.orders__clair_<run_id>
SELECT * FROM ( ... )

-- ── test · a blocking test that fails stops the promotion
SELECT ... FROM db.schema.orders__clair_<run_id> ...

-- ── promote · when should_promote() gives True
CREATE OR REPLACE TABLE db.schema.orders CLONE db.schema.orders__clair_<run_id>

-- ── retire · after the promotion
DROP TABLE IF EXISTS db.schema.orders__clair_<run_id>
```

Each step is a phase of `Trouve.plan()`, and a subclass can change it. The promotion
replaces the production table with a new object, thus Time Travel and a stream start
again at each run. To keep the object, see
[Keep the production table object](../guides/control-the-lifecycle.md#1-keep-the-production-table-object).

Snowflake makes a clone in the metadata, so this step takes constant time for a table of any size. Two conditions give the clone no base: the physical address holds nothing, and the physical address holds a view because the Trouve was a `VIEW` before. In each condition clair changes that Trouve to the full refresh mode and makes no clone. The promotion does not change.

## A Trouve that changes its type

Change a `Table` to a `View`, or the reverse, and the next run drops the old object and makes the new one. A change between a standard table and an [Iceberg table](#an-iceberg-table) does the same.

Snowflake replaces an object with an object of the same type only. `CREATE OR REPLACE VIEW` against a name that holds a table raises `Object '<name>' already exists.` Thus the old object must go first. A drop removes each grant on the object, but clair supports [future grants](#grants) only: the new object gets the grants of its schema again, and the drop loses nothing that the schema does not give back.

A staged run does these steps:

1. It builds the candidate at the staging address, and runs the data quality tests on it, as usual.
2. If a blocking test fails, it stops. The old object stays at the physical address, with its data.
3. If the tests pass, the promotion sends `DROP <old type> IF EXISTS <physical>`, then makes the new object from the candidate.

The DROP and the statement after it are two statements, because Snowflake has no statement that replaces one type with another. If the second one fails, the physical address holds no object, and the error says so. The tested candidate stays at the staging address, and the next run makes the object again.

A run with `--no-test` has no staging step. It sends the DROP before its first write. If the build then fails, the address holds no object.

`clair compile` opens no warehouse connection, thus it cannot know the type that the address holds, and its plan shows no DROP.

## A transient table

`transient=True` on a `Table`, a `PandasTable` or a `SeedTable` makes a TRANSIENT table. Snowflake keeps no Fail-safe copy of a transient table, its Time Travel lasts one day maximum, and Snowflake Backups do not include it. The table costs less storage, but Snowflake cannot recover it after that day. Use it for a table that clair can make again from its inputs.

In a transient schema or a transient database, each table is transient, also with `transient=False`. Snowflake decides the kind from the schema, and the runs operate in the same way.

Clair makes no TEMPORARY table. Snowflake drops a temporary table at the end of the session, and only that session can read it. Each thread of a run has its own session, thus a downstream Trouve cannot read the table, and the table is gone after the run. A `CREATE OR REPLACE TEMPORARY TABLE` also does not replace a permanent table of the same name: it hides that table from its own session only, and each other reader sees the old data.

```python
trouve = Table(
    sql=f"SELECT ... FROM {events}",
    transient=True,
)
```

To make each table of a database or a schema transient, set the default in `__database_config__.py` or `__schema_config__.py`:

```python
# staging/__database_config__.py
from clair import DatabaseDefaults

defaults = DatabaseDefaults(transient=True)
```

A Trouve that gives `transient=False` stays permanent, and a schema config replaces the database config. See [Per-Database & Schema Config](per-database-schema-config.md#resolution-order). A View and a Source make no table, and ignore the default. An Iceberg table also ignores it, because clair makes no transient Iceberg table.

The default is the option of each table, thus clair writes `TRANSIENT` in each statement. A transient schema in Snowflake does the same for each table in it, also for a table that clair does not make. Clair makes each schema that does not exist with `CREATE SCHEMA IF NOT EXISTS`, and that schema is permanent.

Snowflake cannot clone a transient table into a permanent one. Thus each `CREATE` of the Trouve says `TRANSIENT`: the build at the staging address, the clone of an incremental run, and the promotion.

When you change the option of a table that exists:

| The change | A full refresh | An incremental run |
|------------|----------------|--------------------|
| Permanent to transient | The table becomes transient. | The table becomes transient, and keeps its rows. |
| Transient to permanent | The table becomes permanent. | The run stops at the clone, and writes nothing. |

The promotion makes a new object, thus each change gets the future grants of the schema.

The incremental run that stops gives `Transient object cannot be cloned to a permanent object.` Run a full refresh, if the SQL can make each row again. If it cannot, make the table permanent yourself, and keep its rows:

```sql
CREATE OR REPLACE TABLE db.schema.events AS SELECT * FROM db.schema.events
```

Then run clair again. The incremental run clones a permanent table into a permanent one.

A run with `--no-test` has no staging step. An incremental run then adds the rows to the table as it is, and the kind of the table does not change. A full refresh makes the table again, with the new kind.

## A clustered table

`cluster_by` on a `Table`, a `PandasTable` or a `SeedTable` gives the table a clustering key. Each part is a column name or an SQL expression, and clair sends it as you write it. A column that your SQL makes with quotes, such as `"orderDate"`, needs the same quotes here: `cluster_by=[orderDate]`. Snowflake then reclusters the table in the background, and a query that filters on the key reads fewer micro-partitions. Automatic Clustering costs credits, thus give a key to a large table only.

```python
trouve = Table(
    sql=f"SELECT ... FROM {events}",
    cluster_by=["to_date(created_at)", "customer_id"],
)
```

A clone copies the clustering key, but Snowflake suspends Automatic Clustering on the clone. Each promotion is a clone, thus clair adds two statements:

1. After the build, `ALTER TABLE <staging> CLUSTER BY (...)`. A key that Snowflake rejects stops the node before the promotion.
2. After the promotion, `ALTER TABLE <physical> RESUME RECLUSTER`. If this statement fails, the node fails. The error names the clone as a statement that changed the physical table, and names the `RESUME RECLUSTER` as the statement that failed.

The key goes on with an `ALTER`, and not in the `CREATE TABLE ... AS SELECT`, because Snowflake asks for the column definitions in a CTAS that clusters.

An incremental run clones the physical table, and the clone keeps its key. The `ALTER` then replaces it with the key of the Trouve. When you remove `cluster_by`, the next incremental run removes the key: an incremental Trouve with no `cluster_by` gets `ALTER TABLE <staging> DROP CLUSTERING KEY` after the build. `clair compile` cannot see the key of a table, thus the drop is in each incremental plan, and Snowflake accepts it on a table with no key. A full refresh makes a new table with no key, thus it needs no drop.

A run with `--no-test` has no staging step and no clone. It sets the key on the physical table, and Automatic Clustering stays on.

## Comments in Snowflake

The `docs` of a Trouve becomes the comment of its table or view, and the `docs` of each `Column` becomes the comment of that column. Snowflake shows them in Snowsight, in `SHOW TABLES`, and in `INFORMATION_SCHEMA`. A catalog tool that reads Snowflake thus shows the same text as `clair docs`.

```python
trouve = View(
    sql=f"SELECT order_id, amount FROM {orders}",
    docs="The orders, one row each.",
    columns=[Column(name="order_id", type="NUMBER", docs="The order ID.")],
)
```

Clair adds these statements after the promotion:

```sql
ALTER VIEW <physical> SET COMMENT = 'The orders, one row each.'
ALTER VIEW <physical> ALTER COLUMN order_id COMMENT 'The order ID.'
```

The comments go onto the physical object, and not onto the staging object, because a promotion of a view is a `CREATE OR REPLACE VIEW`, and it removes each comment. If a comment statement fails, the node fails. The error names each promotion statement that changed the physical object, and the comment statement that failed. A run with `--no-test` writes the comments after the build.

Clair cleans a triple-quoted `docs` text as Python cleans a docstring: it removes the first and the last empty line, and the common indent.

A new object has no comment, thus clair writes each text that is not empty. The comments of the columns of a table go into one `ALTER TABLE <physical> ALTER COLUMN a COMMENT '...', COLUMN b COMMENT '...'`. A view gets one `ALTER VIEW <physical> ALTER COLUMN ... COMMENT '...'` for each column: Snowflake accepts `COMMENT ON COLUMN` on a table only, and one column in each `ALTER VIEW`.

An incremental table keeps its comments from the last run. The run already reads that table, for its type and for its columns, and the two answers hold the comments. Clair compares them with the `docs`, and writes only a comment that differs. An incremental run with unchanged `docs` thus sends no comment statement, and a `docs` text that you remove leaves the table on the next run. A column that you remove from `columns` keeps its old comment.

`clair compile` opens no connection, thus its plan shows each text that is not empty, as for a new object.

## An Iceberg table

`iceberg=Iceberg()` on a `Table` makes a Snowflake-managed Apache Iceberg table. Snowflake is the catalog, and it writes the data as Parquet files to an external volume. Another engine, for example Spark or Trino, can read the table from those files.

```python
from clair import Iceberg, Table

trouve = Table(
    sql=f"SELECT ... FROM {events}",
    iceberg=Iceberg(external_volume="analytics_lake"),
)
```

With no `external_volume`, Snowflake takes the default of the schema, the database or the account. Without one of those, it uses `SNOWFLAKE_MANAGED`, its own storage. Snowflake makes the base location of the files from the table name, thus each staging table has its own files.

Snowflake builds, clones and promotes an Iceberg table in the same way as a standard table. Each statement says `ICEBERG TABLE`: the build `CREATE OR REPLACE ICEBERG TABLE ... CATALOG = 'SNOWFLAKE' AS (...)`, the clone of an incremental run, the promotion, and each `ALTER`.

- Only a `Table` accepts `iceberg`. A `PandasTable` and a `SeedTable` write through `write_pandas`, which makes a standard table only.
- A `Table` cannot be `transient` and Iceberg. Snowflake gives no `TRANSIENT` in the clone of an Iceberg table, and each run clones.
- An Iceberg table accepts fewer data types than a standard table. For example, a timestamp must have the scale 6 or less, thus cast `CURRENT_TIMESTAMP()` to `TIMESTAMP_NTZ(6)`. Snowflake stops the build with the column name.

Snowflake cannot replace a standard table with an Iceberg table, or the reverse. A Trouve that changes its kind thus drops the old table before the promotion, as a change from a TABLE to a VIEW does: see [A Trouve that changes its type](#a-trouve-that-changes-its-type). An incremental run of an Iceberg Trouve against a standard table makes a full refresh candidate, because the clone of a standard table cannot make an Iceberg table.

## The plan

`clair compile` writes the complete plan to `_clairtifacts/`: the clone, the build at the staging address, a comment that marks the test step, and the promotion.

```bash
clair compile --env dev
clair compile --env dev --run-mode incremental
```

## After a failure

Clair keeps each object. It drops a staging object only after a promotion.

| The fault | The physical object | The staging object |
|-----------|---------------------|--------------------|
| The build | Keeps its data | Clair keeps it, if the build made it |
| A data quality test | Keeps its data | Clair keeps it. It is the rejected candidate. |
| The promotion | Holds the result of each statement that succeeded. The error names them. | Clair keeps it. It holds the tested data. |

In each condition the Trouve gets the FAILED status, clair skips each dependent, and `clair run` stops with the status code 1. The error message names the staging object, thus you can query it:

```
[1/3] db.schema.orders ... FAILED in test (2.4s)
      Error: the tests failed. db.schema.orders keeps its data. The rejected candidate stays at db.schema.orders__clair_<run_id>
```

This is the fastest path to a diagnosis. The candidate holds the exact data that failed the test. To make it again, you must run each upstream Trouve again.

The cost is that a failed run leaves objects. A staging table from a full refresh is a complete copy. A staging table from an incremental run starts with the micro-partitions of the physical table, and it uses more storage as the two tables diverge. Drop the objects when you do not need them:

```sql
SHOW TABLES LIKE '%__clair_%' IN SCHEMA db.schema;
```

## The costs

- **Storage.** Each Trouve holds two copies for a short time. A full refresh makes a complete second copy for the duration of the node. An incremental build makes a clone, which shares micro-partitions with the physical table and diverges as the rows change. An object from a failure stays until you drop it.
- **The tests give the guarantee.** `--no-test` stops the staging step, because tests that do not run cannot decide a promotion. A Trouve with no tests still goes to a staging address, and clair promotes it. Staging does not make test coverage that you did not write.
- **Name length.** The suffix adds 40 characters to the table name. Snowflake permits 255 characters for each name, and the limit applies to each name and not to the full `database.schema.table` path. A Trouve with a table name of more than 215 characters therefore fails before the SQL starts, with a message that tells you to use a shorter name.
- **`--sample`.** A sampled run builds the staging object from a sample of each input, and the tests decide the promotion on that object. clair stops a sampled run that writes to a logical address, thus a sample never reaches production. See [Sampled Runs](sampling.md).

## See also

- [Data Quality Tests](data-quality-tests.md)
- [Incrementality](incrementality.md)
- [clair run](../cli/run.md)
