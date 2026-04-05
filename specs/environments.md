# Environments

Environments replace profiles. Each environment carries connection credentials plus an optional routing policy that remaps where TABLE/VIEW Trouves materialize. SOURCEs are never routed.

**Implementation:** `src/clair/environments/environments.py`, `src/clair/environments/routing.py`

## Key renames

| Old | New |
|-----|-----|
| `~/.clair/profiles.yml` | `~/.clair/environments.yml` |
| `--profile` CLI flag | `--env` CLI flag |
| `CLAIR_PROFILE` env var | `CLAIR_ENV` env var |
| Default name `"default"` | Default name `"dev"` |

No backwards compatibility. `profiles.yml` is not read. Users re-run `clair init`.

## YAML schema

```yaml
<env_name>:
  account: <str>
  user: <str>
  warehouse: <str>
  authenticator: externalbrowser   # OR password: / OR private_key_path: + private_key_passphrase:
  role: <str>                      # optional

  routing:                         # optional
    policy: database_override | schema_isolation
    database: <str>                # required for both policies
    schema: <str>                  # required for schema_isolation only
```

## Routing transforms

**`database_override`:** `(db, schema, table)` → `(config.database, schema, table)`

**`schema_isolation`:** `(db, schema, table)` → `(config.database, config.schema, "{db}_{schema}_{table}")`
All names uppercased.

## What routing affects vs. doesn't

- Routing applies to TABLE/VIEW materialization targets and their SQL references.
- SOURCEs: always use original location regardless of routing.
- `clair dag` and `clair docs`: show logical (unrouted) names.
- `clair compile --env <name>`: applies routing to compiled SQL output.
- `--select` filtering: matches on logical full_names, routing-agnostic.

## Collisions

Two Trouves routing to the same target = prominent **warning** (not error). Run proceeds. All collisions reported upfront. `--strict` to escalate to error is v2.

## Environment resolution order

1. `--env` CLI flag
2. `CLAIR_ENV` env var
3. `"dev"` (hardcoded default)

## Accessing the environment name

`load_environment()` returns `(name, environment)`. The name is also set on `environment.name`:

```python
env_name, env = load_environment()
assert env.name == env_name  # e.g. "dev"
```
