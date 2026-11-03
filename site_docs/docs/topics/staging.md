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
3. **Promote after the tests pass.** For a `TABLE`, clair runs `CREATE OR REPLACE TABLE <physical> CLONE <staging> COPY GRANTS`. Snowflake does this in the metadata, so the time does not grow with the table size. Clair then drops the staging copy. For a `VIEW`, clair runs `CREATE OR REPLACE VIEW ... COPY GRANTS`.
4. **Keep the candidate after a failure.** The physical object keeps the data that it had. Clair does not drop the staging object — see [After a failure](#after-a-failure). The Trouve gets the FAILED status, and clair skips each Trouve downstream of it.

Clair promotes each node immediately after the tests of that node, and before the next node starts. A dependent thus reads its upstream at the physical address that its SQL names. You never write `__clair_` in a Trouve file.

## Grants

The promotion uses `COPY GRANTS`, and this is necessary. Snowflake attaches a privilege to the *object*, and not to the name. An `ALTER TABLE ... SWAP WITH ...` moves the grants of a table to the staging name, and leaves the production name with only the grants of the staging object. Clair would remove each privilege that an administrator granted on the target, on each run.

`COPY GRANTS` copies each privilege except `OWNERSHIP` from the object that Snowflake replaces. If the physical object does not exist, Snowflake copies from the clone source. The first run thus needs no different statement.

`OWNERSHIP` is the exception. It goes to the role that runs clair. If a different role owns the production object, the promotion changes the owner.

## An incremental Trouve

An incremental Trouve changes data that already exists, so the staging object needs that data before the `INSERT` or the `MERGE` runs. Clair puts it there with a zero-copy clone:

```sql
-- staging: clone the target, so the incremental statements have a base
CREATE OR REPLACE TABLE db.schema.orders__clair_<run_id> CLONE db.schema.orders

INSERT INTO db.schema.orders__clair_<run_id>
SELECT * FROM ( ... )

-- staging: the data quality tests run here

CREATE OR REPLACE TABLE db.schema.orders CLONE db.schema.orders__clair_<run_id> COPY GRANTS
DROP TABLE IF EXISTS db.schema.orders__clair_<run_id>
```

Snowflake makes a clone in the metadata, so this step takes constant time for a table of any size. Two conditions give the clone no base: the physical address holds nothing, and the physical address holds a view because the Trouve was a `VIEW` before. In each condition clair changes that Trouve to the full refresh mode and makes no clone. The promotion does not change.

## A Trouve that changes its type

Change a `Table` to a `View`, or the reverse, and the next run stops before the promotion. A change between a standard table and an [Iceberg table](#an-iceberg-table) does the same. You drop the old object, and the run after that makes the new one.

Snowflake replaces an object with an object of the same type only. `CREATE OR REPLACE VIEW` against a name that holds a table raises `Object '<name>' already exists.` The only repair is a drop, and a drop removes each grant on the object. The role that runs clair often cannot grant them again — for example in a managed access schema. Clair therefore does not drop the object. The person who changed the type decides about the grants.

The run does these steps:

1. It builds the candidate at the staging address, and runs the data quality tests on it, as usual.
2. It asks the warehouse for the type of the object at the physical address. The type is different, so the Trouve fails, and clair skips each Trouve downstream of it.
3. The physical object and its grants do not change. The tested candidate stays at the staging address, for your examination.

The error names the drop to run:

```text
db.schema.orders holds the type TABLE, and the Trouve now makes the type VIEW.
Snowflake cannot replace one type with the other. Clair does not drop the TABLE,
because a drop removes each grant on it. Run DROP TABLE db.schema.orders, then
run clair again.
The tested candidate stays at db.schema.orders__clair_<run_id>
```

After the drop, the address holds nothing, and the next run takes the usual path.

A run with no data quality tests has no staging step, thus it has no candidate to keep. It stops before it writes anything.

`clair compile` opens no warehouse connection, thus it cannot know the type that the address holds. The plan shows the condition as a comment: `-- object type: if <address> holds an object of a type other than TABLE, clair stops here and writes nothing to it`.

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

Snowflake cannot clone a transient table into a permanent one. Thus each `CREATE` of the Trouve says `TRANSIENT`: the build at the staging address, the clone of an incremental run, and the promotion.

When you change the option of a table that exists:

| The change | A full refresh | An incremental run |
|------------|----------------|--------------------|
| Permanent to transient | The table becomes transient. | The table becomes transient, and keeps its rows. |
| Transient to permanent | The table becomes permanent. | The run stops at the clone, and writes nothing. |

The promotion uses `COPY GRANTS`, thus each change keeps the grants.

The incremental run that stops gives `Transient object cannot be cloned to a permanent object.` Run a full refresh, if the SQL can make each row again. If it cannot, make the table permanent yourself, and keep its rows and its grants:

```sql
CREATE OR REPLACE TABLE db.schema.events COPY GRANTS AS SELECT * FROM db.schema.events
```

Then run clair again. The incremental run clones a permanent table into a permanent one.

A run with `--no-test` has no staging step. An incremental run then adds the rows to the table as it is, and the kind of the table does not change. A full refresh makes the table again, with the new kind.

## A clustered table

`cluster_by` on a `Table`, a `PandasTable` or a `SeedTable` gives the table a clustering key. Each part is a column name or an SQL expression. Snowflake then reclusters the table in the background, and a query that filters on the key reads fewer micro-partitions. Automatic Clustering costs credits, thus give a key to a large table only.

```python
trouve = Table(
    sql=f"SELECT ... FROM {events}",
    cluster_by=["to_date(created_at)", "customer_id"],
)
```

A clone copies the clustering key, but Snowflake suspends Automatic Clustering on the clone. Each promotion is a clone, thus clair adds two statements:

1. After the build, `ALTER TABLE <staging> CLUSTER BY (...)`. A key that Snowflake rejects stops the node before the promotion.
2. After the promotion, `ALTER TABLE <physical> RESUME RECLUSTER`. If this statement fails, the physical table holds the tested data, and the node fails with the statement to run again.

The key goes on with an `ALTER`, and not in the `CREATE TABLE ... AS SELECT`, because Snowflake asks for the column definitions in a CTAS that clusters.

An incremental run clones the physical table, and the clone keeps its key. The `ALTER` then replaces it with the key of the Trouve. When you remove `cluster_by`, the key stays on an incremental table until the next full refresh, because `clair compile` cannot see the key of a table. To remove it now, run `ALTER TABLE <physical> DROP CLUSTERING KEY`.

A run with `--no-test` has no staging step and no clone. It sets the key on the physical table, and Automatic Clustering stays on.

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

Snowflake builds, clones and promotes an Iceberg table in the same way as a standard table. Each statement says `ICEBERG TABLE`: the build `CREATE OR REPLACE ICEBERG TABLE ... CATALOG = 'SNOWFLAKE' AS (...)`, the clone of an incremental run, the promotion, and each `ALTER`. The promotion uses `COPY GRANTS`.

- Only a `Table` accepts `iceberg`. A `PandasTable` and a `SeedTable` write through `write_pandas`, which makes a standard table only.
- A `Table` cannot be `transient` and Iceberg. Snowflake gives no `TRANSIENT` in the clone of an Iceberg table, and each run clones.
- An Iceberg table accepts fewer data types than a standard table. For example, a timestamp must have the scale 6 or less, thus cast `CURRENT_TIMESTAMP()` to `TIMESTAMP_NTZ(6)`. Snowflake stops the build with the column name.

Snowflake cannot replace a standard table with an Iceberg table, or the reverse. A Trouve that changes its kind thus stops before the promotion, as a change from a TABLE to a VIEW does: the error names the drop to run, and the tested candidate stays. An incremental run of an Iceberg Trouve against a standard table makes a full refresh candidate, because the clone of a standard table cannot make an Iceberg table.

## The plan

`clair compile` writes the complete plan to `_clairtifacts/`: the clone, the build at the staging address, a comment that marks the test step, and the promotion.

```bash
clair compile
clair compile --run-mode incremental
```

## After a failure

Clair keeps each object. It drops a staging object only after a promotion.

| The fault | The physical object | The staging object |
|-----------|---------------------|--------------------|
| The build | Keeps its data | Clair keeps it, if the build made it |
| A data quality test | Keeps its data | Clair keeps it. It is the rejected candidate. |
| The promotion | Keeps its data | Clair keeps it. It holds the tested data. |

In each condition the Trouve gets the FAILED status, clair skips each dependent, and `clair run` stops with the status code 1. The error message names the staging object, thus you can query it:

```
db.schema.orders ... FAILED (2.4s)
      Error: the tests failed. db.schema.orders keeps its data.
             The rejected candidate stays at db.schema.orders__clair_<run_id>
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
- **`--sample`.** A sample applies to the staging object. `--sample` thus decides the promotion with a `TOP 1000` test, and not with a test of the complete table.

## See also

- [Data Quality Tests](data-quality-tests.md)
- [Incrementality](incrementality.md)
- [clair run](../cli/run.md)
