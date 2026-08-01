# Routing Policies

Routing remaps the logical Snowflake names derived from your filesystem to different physical targets. Configure routing per environment in `~/.clair/environments.yml`.

The most common use case: run a production project against a dev Snowflake database without changing any Trouve files.

## SOURCE passthrough

Regardless of the active routing policy, SOURCE Trouves always use their logical name. Routing only applies to TABLE and VIEW Trouves.

## `database_override`

Replaces the database component of every non-SOURCE Trouve's name.

```yaml
# ~/.clair/environments.yml
dev:
  account: myorg-myaccount
  user: alice@example.com
  authenticator: externalbrowser
  warehouse: dev_warehouse
  routing:
    policy: database_override
    database_name: dev
```

With this policy active, a Trouve at `refined/orders/daily.py` (logical name `refined.orders.daily`) is written to `dev.orders.daily` in Snowflake. The source at `source/orders/raw.py` is still read from `source.orders.raw`.

**Example mapping:**

| Logical name | Physical target |
|---|---|
| `source.orders.raw` | `source.orders.raw` (SOURCE — passthrough) |
| `refined.orders.daily` | `dev.orders.daily` |
| `derived.orders.summary` | `dev.orders.summary` |

## `schema_isolation`

Collapses `database.schema.table` into a single table token (`DATABASE_SCHEMA_TABLE`) under a fixed database and schema. Use this to run multiple developers' projects in one shared Snowflake schema without conflicts — each developer gets their own prefixed table names.

```yaml
# alice's dev environment
dev:
  account: myorg-myaccount
  user: alice@example.com
  authenticator: externalbrowser
  warehouse: dev_warehouse
  routing:
    policy: schema_isolation
    database_name: dev
    schema_name: alice
```

**Example mapping:**

| Logical name | Physical target |
|---|---|
| `source.orders.raw` | `source.orders.raw` (SOURCE — passthrough) |
| `refined.orders.daily` | `dev.alice.REFINED_ORDERS_DAILY` |
| `derived.orders.summary` | `dev.alice.DERIVED_ORDERS_SUMMARY` |

!!! warning
    `schema_isolation` produces identifiers by concatenating `database_schema_table` with underscores. Snowflake enforces a 255-character limit on identifiers, so very long Trouve names may exceed this limit. clair raises `InvalidRoutingConfigError` if the generated identifier is too long.

## Collision detection

When two TABLE/VIEW Trouves route to the same physical target, clair prints a warning before running:

```
Warning: routing 2 collisions detected (env: dev, policy: database_override → dev)

  dev.orders.daily
    ↳ refined.orders.daily
    ↳ analytics.orders.daily

  Fix: rename a colliding Trouve, adjust the routing policy in
  environments.yml, or use --select to exclude one from this run.
```

## No routing

Omit the `routing` block to use logical names as physical targets (suitable for production):

```yaml
prod:
  account: myorg-myaccount
  user: ci_user
  private_key_path: ~/.clair/snowflake_key.p8
  warehouse: prod_warehouse
  # no routing block — logical names are used as-is
```
