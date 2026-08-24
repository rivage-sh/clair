# Routing

Routing remaps logical Snowflake names to different physical targets. clair reads the logical names from your file system. You write the routing rules in `__routing__.py`, at the root of your project.

The usual use: run a production project against a dev Snowflake database. You do not change a Trouve file.

## Where routing lives

Routing is in the project, not in `~/.clair/environments.yml`:

| File | Holds | Commit it? |
|---|---|---|
| `~/.clair/environments.yml` | Connection settings and credentials | No |
| `<project>/__routing__.py` | The routing rules | Yes |

The environment name joins the two files. An entry with `environment_name = "dev"` applies when clair loads the `dev` environment from `environments.yml`.

The routing file holds no credentials, so you commit it and your team reviews it like other code. Because it is Python, one committed rule gives each developer a separate target.

!!! warning
    A `routing:` block in `environments.yml` is now an error. Move the rule to `__routing__.py`.

## The three types

```python
from clair import RoutingEntry, RoutingTable, TrouveAddress
```

`TrouveAddress` holds a `database_name`, a `schema_name`, and a `table_name`. It validates each name when you make it, so an address that exists is a valid Snowflake identifier.

`RoutingEntry` is the base class for one environment's rule. You write a subclass with an `environment_name` and a `route` method.

`RoutingTable` holds the entries. Your `__routing__.py` makes one and gives it the name `routing`.

## A routing file

```python
# <project>/__routing__.py
"""Clair routing -- gives each environment its physical write target."""

import os
from enum import StrEnum

from clair import RoutingEntry, RoutingTable, TrouveAddress


class EnvironmentName(StrEnum):
    """The environments of this project."""

    DEV = "dev"
    PROD = "prod"


class DeveloperRouting(RoutingEntry):
    """Each person writes to a separate database."""

    environment_name: str = EnvironmentName.DEV.value
    user_variable: str = "CLAIR_USER"

    def route(self, trouve_address: TrouveAddress) -> TrouveAddress:
        user_name = os.environ[self.user_variable].upper()
        return trouve_address.model_copy(
            update={"database_name": f"{trouve_address.database_name}_{user_name}"}
        )


class ProductionRouting(RoutingEntry):
    """Production writes to the logical names, so the address stays the same."""

    environment_name: str = EnvironmentName.PROD.value

    def route(self, trouve_address: TrouveAddress) -> TrouveAddress:
        return trouve_address


routing = RoutingTable(entries=[DeveloperRouting(), ProductionRouting()])
```

With `CLAIR_USER=alice` and the `dev` environment:

| Logical name | Physical target |
|---|---|
| `source.orders.raw` | `source.orders.raw` (SOURCE — passthrough) |
| `refined.orders.daily` | `refined_ALICE.orders.daily` |
| `derived.orders.summary` | `derived_ALICE.orders.summary` |

An environment name joins `__routing__.py` to `environments.yml`, thus the name is easy to
misspell. A `StrEnum` gives each name one definition, and your editor completes it.

## Write a route method

`route` accepts one `TrouveAddress` and gives one `TrouveAddress`. To change one part, call `model_copy`:

```python
def route(self, trouve_address: TrouveAddress) -> TrouveAddress:
    return trouve_address.model_copy(update={"database_name": "DEV"})
```

To build a new address, name all three parts:

```python
def route(self, trouve_address: TrouveAddress) -> TrouveAddress:
    collapsed_table_name = (
        f"{trouve_address.database_name}_{trouve_address.schema_name}_"
        f"{trouve_address.table_name}"
    ).upper()
    return TrouveAddress(
        database_name="DEV",
        schema_name="alice",
        table_name=collapsed_table_name,
    )
```

This second shape puts the projects of many developers in one shared schema. Each developer gets different table names, thus the projects do not collide.

Add a field for each value that the rule needs. Pydantic validates the fields, and the field values show in the CLI messages.

## SOURCE passthrough

SOURCE Trouves always use their logical name. clair never calls `route` for a SOURCE Trouve. Routing applies to TABLE and VIEW Trouves only.

## No entry for an environment

An environment with no entry in the table gets passthrough routing: clair writes to the logical names. Those are the production names, so clair warns you first:

```
Warning: __routing__.py does not name the environment 'staging'.
  Trouves write to their logical (production) names.
  The file names: dev, prod
```

To make the passthrough deliberate, write an entry that gives the address back, as `ProductionRouting` does above. clair then stays quiet.

## Validation

`TrouveAddress` validates every name that it holds. A name must start with a letter or an underscore, hold only letters, digits, underscores or dollar signs, and stay under 255 characters. This applies to the logical names that your directories give, and to the physical names that your rules build.

Run [`clair validate`](../cli/validate.md) to apply your rules to every Trouve without a Snowflake connection.

## Collision detection

If two TABLE or VIEW Trouves route to the same physical target, clair shows a warning before the run:

```
Warning: Clair found 2 routing collisions (env: dev, entry: DeveloperRouting(environment_name='dev', user_variable='CLAIR_USER'))

  dev.orders.daily
    ↳ refined.orders.daily
    ↳ analytics.orders.daily

  Fix: give one Trouve a different name, change the routing entry in __routing__.py,
  or use --select to remove one Trouve from this run.
```

## One entry for each environment

Two entries with one environment name are an error. Only one can win, and a silent choice sends the writes to a target that you do not expect:

```
Invalid routing file at __routing__.py: ValidationError: ...
the routing table has more than one entry for: dev. Give each environment one entry.
```
