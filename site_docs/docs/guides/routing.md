# Routing Policies

Routing remaps logical Snowflake names to different physical targets. clair reads the logical names from your file system. Configure routing for each environment in `~/.clair/environments.yml`.

The usual use: run a production project against a dev Snowflake database. You do not change a Trouve file.

## SOURCE passthrough

SOURCE Trouves always use their logical name. The active routing policy has no effect on them. Routing applies to TABLE and VIEW Trouves only.

## `database_override`

This policy replaces the database part of the name of each TABLE and VIEW Trouve.

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

If this policy is active, clair writes the Trouve at `refined/orders/daily.py` to `dev.orders.daily` in Snowflake. Its logical name stays `refined.orders.daily`. clair still reads the source at `source/orders/raw.py` from `source.orders.raw`.

**Example mapping:**

| Logical name | Physical target |
|---|---|
| `source.orders.raw` | `source.orders.raw` (SOURCE — passthrough) |
| `refined.orders.daily` | `dev.orders.daily` |
| `derived.orders.summary` | `dev.orders.summary` |

## `schema_isolation`

This policy joins `database.schema.table` into one table name (`DATABASE_SCHEMA_TABLE`), in a database and a schema that you set. Use it to run the projects of many developers in one shared Snowflake schema. Each developer gets different table names, thus the projects do not collide.

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
    `schema_isolation` joins `database_schema_table` with underscores to make an identifier. Snowflake gives identifiers a limit of 255 characters. A very long Trouve name can go above this limit. clair then raises `InvalidRoutingConfigError`.

## Collision detection

If two TABLE or VIEW Trouves route to the same physical target, clair shows a warning before the run:

```
Warning: Clair found 2 routing collisions (env: dev, policy: database_override → dev)

  dev.orders.daily
    ↳ refined.orders.daily
    ↳ analytics.orders.daily

  Fix: give one Trouve a different name, change the routing policy in environments.yml,
  or use --select to remove one Trouve from this run.
```

## No routing

Omit the `routing` block. clair then uses the logical names as the physical targets. This is correct for production:

```yaml
prod:
  account: myorg-myaccount
  user: ci_user
  private_key_path: ~/.clair/snowflake_key.p8
  warehouse: prod_warehouse
  # no routing block — clair uses the logical names
```
