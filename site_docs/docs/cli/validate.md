# `clair validate`

Apply the routing entries in `__routing__.py` to every Trouve in the project, and report every problem at once.

This command needs no Snowflake credentials, so CI runs it on every change.

```bash
clair validate
clair validate --env prod
clair validate --project path/to/project
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `--project` | `.` | Path to the clair project root. |
| `--env` | `CLAIR_ENV` or `dev` | The environment to route for. It matches an entry in `__routing__.py`. |

## What it examines

- The routing file runs, and it gives a `RoutingTable`.
- The table has one entry for each environment name.
- Every directory name and file name makes a valid address.
- The entry for this environment runs on every TABLE and VIEW Trouve.
- Every address that the entry gives is a valid Snowflake identifier.
- No two Trouves go to one physical target.
- No Trouve names a different Trouve as text. See below.

## An address that is text

You point to a different Trouve with an import and an f-string:

```python
from mydb.refined.events import trouve as refined_events

trouve = Trouve(type=TrouveType.TABLE, sql=f"SELECT * FROM {refined_events}")
```

The f-string makes a token, and clair replaces the token with an address. If you write the
same address as text, you get no token:

```python
# Wrong: clair reads no reference from this text.
trouve = Trouve(type=TrouveType.TABLE, sql="SELECT * FROM mydb.refined.events")
```

Two faults follow, and Snowflake reports neither:

- The text makes no DAG edge. clair can build this Trouve before the Trouve that it reads.
- Routing does not move the text. In your dev environment this Trouve reads the production
  table, and each Trouve beside it reads your dev table.

`clair validate` reads the syntax tree of your SQL, thus it reports a true table name only.
An address in a comment or in a string literal is correct, and the command stays quiet:

```sql
-- This name is correct: mydb.refined.events
SELECT 'mydb.refined.events' AS source_name, * FROM {refined_events}
```

The command examines the SQL of each Trouve and the SQL of each `TestSql`. It reports a
name only when that name is the logical address of a Trouve in your project. A table that
clair does not hold is correct SQL. SQL that the parser cannot read gives no report,
because Snowflake owns the SQL syntax.

## Output

A project with no problems:

```
  environment: dev
  routing file: /home/alice/project/__routing__.py
  entry: DeveloperRouting(environment_name='dev', user_variable='CLAIR_USER')
  Trouves to route: 12

  ✓ Every physical address is valid. No collisions. Each reference is a Trouve.
```

A project with a problem gives exit code 1:

```
  environment: dev
  routing file: /home/alice/project/__routing__.py
  entry: DeveloperRouting(environment_name='dev', user_variable='CLAIR_USER')
  Trouves to route: 12

  ✗ analytics.finance.revenue
    The routing entry `DeveloperRouting(environment_name='dev', user_variable='CLAIR_USER')` failed on 'analytics.finance.revenue': KeyError: 'CLAIR_USER'

  1 problem found.
```

`clair compile` and `clair run` stop at the first routing problem, because they must not write to a wrong target. `clair validate` instead reports every problem, so that you correct them together.

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Every physical address is valid, no two Trouves collide, and each reference is a Trouve. |
| 1 | clair found one problem or more. |

## In CI

```yaml
- name: Validate clair routing
  run: |
    uv run clair validate --env prod
```

See [Routing](../topics/routing.md) for the entry types and how to write a `route` method.
See [The Anatomy of a Run](../topics/anatomy-of-a-run.md) for the address that each Trouve
reads.
