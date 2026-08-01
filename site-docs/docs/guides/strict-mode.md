# Strict Mode

A table can only be tested once it has been materialized. In a normal run that means bad data lands in production first and the tests tell you about it afterwards — the table is already wrong, and anything reading it has already read the wrong numbers.

Strict mode closes that window. Every Trouve is built into a run-scoped staging object, tested there, and only swapped into its real name once every test passes.

```bash
clair run --project . --env prod --strict
```

## What happens per Trouve

1. **Build into staging.** The Trouve is materialized as `<table>__clair_strict_<run_id>`, a sibling object in the same database and schema.
2. **Test the staging object.** The Trouve's data quality tests run against the staging object, not the target.
3. **Promote on pass.** For a `TABLE`, clair issues `ALTER TABLE ... SWAP WITH ...` — a metadata-only operation whose cost does not scale with table size — then drops the superseded copy. For a `VIEW`, the target is recreated with `CREATE OR REPLACE VIEW`.
4. **Discard on fail.** The staging object is dropped and the target is left exactly as it was. The Trouve is reported as a failure and everything downstream of it is skipped.

Promotion happens immediately after each node's tests, before the next node starts. Dependents therefore always read their upstreams under the real names their SQL references — you never write `__clair_strict_` anywhere yourself.

## Incremental Trouves

An incremental Trouve applies changes on top of state that already exists, so the staging object needs that state before the `INSERT` or `MERGE` can run. clair seeds it with a zero-copy clone:

```sql
-- strict: clone target into staging so incremental statements have a base
CREATE OR REPLACE TABLE db.schema.orders__clair_strict_<run_id> CLONE db.schema.orders

INSERT INTO db.schema.orders__clair_strict_<run_id>
SELECT * FROM ( ... )

-- tests run here

ALTER TABLE db.schema.orders__clair_strict_<run_id> SWAP WITH db.schema.orders
DROP TABLE IF EXISTS db.schema.orders__clair_strict_<run_id>
```

Snowflake clones are metadata-only, so seeding the staging table is constant-time no matter how large the target is. If the target does not exist yet, clair falls back to a full refresh for that Trouve and skips the clone; the staging object is then renamed into place rather than swapped.

## Seeing the plan

`clair compile --strict` writes the full plan to `_clairtifacts/`, including the clone, the staging build, a comment marking where tests run, and the promotion:

```bash
clair compile --project . --strict
clair compile --project . --strict --run-mode incremental
```

## Failure modes

| What failed | Target | Staging object |
|-------------|--------|----------------|
| The build itself | Untouched | Dropped |
| A data quality test | Untouched | Dropped |
| The swap or rename | Untouched | **Retained** — it holds tested data and is the only record of the run |

In all three cases the Trouve is reported as `FAILED`, its dependents are skipped, and `clair run` exits `1`.

## Costs and caveats

- **Storage.** Each Trouve briefly holds two copies. For a full refresh that is a real second copy for the duration of the node; for an incremental build the clone shares micro-partitions with the target and only diverges as rows change.
- **Tests are required.** `--strict` cannot be combined with `--no-test` — the whole point is to gate promotion on tests. A Trouve with no tests attached still goes through staging and is promoted; strict mode does not manufacture coverage you have not written.
- **Name length.** The suffix adds 47 characters to the table component. Snowflake caps identifiers at 255, so a Trouve whose name is within 47 characters of the cap fails at the naming step with a clear error rather than mid-run.
- **`--sample`.** Sampling applies to the staging object, so `--strict --sample` gates promotion on a `TOP 1000` check rather than the full table.

## See also

- [Data Quality Tests](data-quality-tests.md)
- [Incrementality](incrementality.md)
- [clair run](../cli/run.md)
