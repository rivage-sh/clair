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

## Output

A project with no problems:

```
  environment: dev
  routing file: /home/alice/project/__routing__.py
  entry: DeveloperRouting(environment_name='dev', user_variable='CLAIR_USER')
  Trouves to route: 12

  ✓ Every routed name is valid. No collisions.
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
| 0 | Every routed name is valid, and no two Trouves collide. |
| 1 | clair found one problem or more. |

## In CI

```yaml
- name: Validate clair routing
  run: |
    uv run clair validate --env prod
```

See the [routing guide](../guides/routing.md) for the entry types and how to write a `route` method.
