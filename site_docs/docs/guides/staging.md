# Staging

Snowflake can test a table only after the table exists. A direct write therefore puts untested data into production first, and the tests report the fault after a reader saw the wrong numbers.

Staging closes that window, and it is **how clair writes**. There is no flag. Clair writes each Trouve to a run-scoped staging address, runs the tests there, and gives the object its physical name only after each test passes.

```bash
clair run --project . --env prod
```

`--no-test` is the one flag that stops this. It removes the tests that decide the promotion, so a staging address protects nothing. That run writes to each physical address directly.

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

Snowflake makes a clone in the metadata, so this step takes constant time for a table of any size. If the physical table does not exist, clair changes that Trouve to the full refresh mode and makes no clone. The promotion does not change.

## The plan

`clair compile` writes the complete plan to `_clairtifacts/`: the clone, the build at the staging address, a comment that marks the test step, and the promotion.

```bash
clair compile --project .
clair compile --project . --run-mode incremental
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
