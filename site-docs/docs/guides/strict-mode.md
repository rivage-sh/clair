# Strict Mode

A table can only be tested once it has been materialized. In a normal run that means bad data lands in production first and the tests tell you about it afterwards — the table is already wrong, and anything reading it has already read the wrong numbers.

Strict mode closes that window. Every Trouve is built into a run-scoped staging object, tested there, and only swapped into its real name once every test passes.

```bash
clair run --project . --env prod --strict
```

## What happens per Trouve

1. **Build into staging.** The Trouve is materialized as `<table>__clair_<run_id>`, a sibling object in the same database and schema.
2. **Test the staging object.** The Trouve's data quality tests run against the staging object, not the target.
3. **Promote on pass.** For a `TABLE`, clair issues `CREATE OR REPLACE TABLE <target> CLONE <staging> COPY GRANTS` — a metadata-only operation whose cost does not scale with table size — then drops the staging copy. For a `VIEW`, the target is recreated with `CREATE OR REPLACE VIEW ... COPY GRANTS`.
4. **Keep the candidate on fail.** The target is left exactly as it was, and the staging object is deliberately **not** dropped — see [When something fails](#when-something-fails). The Trouve is reported as a failure and everything downstream of it is skipped.

Promotion happens immediately after each node's tests, before the next node starts. Dependents therefore always read their upstreams under the real names their SQL references — you never write `__clair_` anywhere yourself.

## Grants

Promotion uses `COPY GRANTS`, and it is not optional. Snowflake attaches privileges to the *object*, not to the name: an `ALTER TABLE ... SWAP WITH ...` carries a table's grants away under the staging name and leaves the production name holding only whatever the staging object was created with. Any privilege granted directly on a target would be silently revoked on every run.

`COPY GRANTS` copies every privilege except `OWNERSHIP` from the object being replaced — or, when the target does not yet exist, from the clone source, which is why promotion needs no special case for a first run.

`OWNERSHIP` is the exception: it lands on the role executing the run. If a production object is owned by some other role, strict mode changes its owner.

## Incremental Trouves

An incremental Trouve applies changes on top of state that already exists, so the staging object needs that state before the `INSERT` or `MERGE` can run. clair seeds it with a zero-copy clone:

```sql
-- strict: clone target into staging so incremental statements have a base
CREATE OR REPLACE TABLE db.schema.orders__clair_<run_id> CLONE db.schema.orders

INSERT INTO db.schema.orders__clair_<run_id>
SELECT * FROM ( ... )

-- tests run here

CREATE OR REPLACE TABLE db.schema.orders CLONE db.schema.orders__clair_<run_id> COPY GRANTS
DROP TABLE IF EXISTS db.schema.orders__clair_<run_id>
```

Snowflake clones are metadata-only, so seeding the staging table is constant-time no matter how large the target is. If the target does not exist yet, clair falls back to a full refresh for that Trouve and skips the seeding clone; promotion is unchanged.

## Seeing the plan

`clair compile --strict` writes the full plan to `_clairtifacts/`, including the clone, the staging build, a comment marking where tests run, and the promotion:

```bash
clair compile --project . --strict
clair compile --project . --strict --run-mode incremental
```

## When something fails

Nothing is thrown away. A staging object is dropped only after it has been successfully promoted.

| What failed | Target | Staging object |
|-------------|--------|----------------|
| The build itself | Untouched | Retained, if it got far enough to be created |
| A data quality test | Untouched | Retained — the rejected candidate |
| The promotion | Untouched | Retained — it holds tested data and is the only record of the run |

In all three cases the Trouve is reported as `FAILED`, its dependents are skipped, `clair run` exits `1`, and the error names the staging object so you can query it directly:

```
db.schema.orders ... FAILED (2.4s)
      Error: strict mode: tests failed, db.schema.orders left unchanged
             (rejected candidate retained at db.schema.orders__clair_<run_id>)
```

This is the fastest path to a diagnosis. The candidate is the exact data that failed the test, and rebuilding it otherwise means re-running everything upstream of it.

The cost is that failed runs accumulate objects. Staging tables created by a full refresh are real copies; a retained incremental clone starts out sharing micro-partitions with its target but diverges — and therefore starts costing real storage — as the target changes. Drop them once you are done with them:

```sql
SHOW TABLES LIKE '%__clair_%' IN SCHEMA db.schema;
```

## Costs and caveats

- **Storage.** Each Trouve briefly holds two copies. For a full refresh that is a real second copy for the duration of the node; for an incremental build the clone shares micro-partitions with the target and only diverges as rows change. Objects left behind by failures persist until you drop them.
- **Tests are required.** `--strict` cannot be combined with `--no-test` — the whole point is to gate promotion on tests. A Trouve with no tests attached still goes through staging and is promoted; strict mode does not manufacture coverage you have not written.
- **Name length.** The suffix adds 40 characters to the table component. Snowflake caps each identifier at 255 — the limit is per object name, not per fully-qualified path — so a Trouve whose table name is within 40 characters of the cap fails at the naming step with a clear error rather than mid-run.
- **`--sample`.** Sampling applies to the staging object, so `--strict --sample` gates promotion on a `TOP 1000` check rather than the full table.

## See also

- [Data Quality Tests](data-quality-tests.md)
- [Incrementality](incrementality.md)
- [clair run](../cli/run.md)
