# clair init

Create a new clair project with example Trouves and a Snowflake connection profile.

```bash
clair init [--project PATH]
```

## What it does

1. Prompts for a project directory (default: current directory)
2. Prompts for an environment name (default: `dev`). If `~/.clair/environments.yml` does
   not exist, asks for your Snowflake connection details and writes the file. If the file
   exists, clair keeps it, and the name must be a top-level key of that file.
3. Prompts for an example source table name in the format `database.schema.table`
4. Creates a starter project with a SOURCE Trouve for that table
5. Writes a `__routing__.py` file with one passthrough entry for that environment. The
   entry changes no address, thus a run writes to the logical addresses. See
   [Routing](../topics/routing.md).
6. Writes a `.gitignore` file that excludes `_clairtifacts/`
7. Prints the next steps, with `--env` set to that environment

## Interactive session

```
Project directory [.]: ./my_project

Environment name [dev]: dev

  hint: select concat(current_organization_name(), '-', current_account_name()) as account;
Snowflake account (e.g. myorg-myaccount): myorg-myaccount

  hint: select current_user() as user;
Snowflake user: alice@example.com

Authentication method:
  1. Private key
  2. Password
  3. SSO (externalbrowser)
Enter choice (1, 2, 3) [1]: 3

  hint: select current_warehouse() as warehouse;
Warehouse: dev_warehouse

Role (leave empty to use the default role of the user): analyst

  hint: select current_region() as region;
Region (e.g. us-east-1): us-east-1

  hint: select current_account() as account_locator;
Account locator (e.g. abc12345): abc12345

Trouves that run at one time (threads) [4]: 4

Give an example Snowflake table that contains source data (for example source.orders.raw) [source]: source.orders.raw

  created  /path/to/my_project/source/orders/raw.py
  created  /path/to/my_project/__routing__.py

  created  /path/to/my_project/.gitignore

✓ Project ready.

Next steps:
  1. cd /path/to/my_project
  2. clair compile --env dev
  3. clair run --env dev
```

## Files created

```
my_project/
├── __routing__.py       ← one passthrough entry for the environment
├── source/
│   └── orders/
│       └── raw.py       ← SOURCE Trouve stub
└── .gitignore           ← excludes /_clairtifacts

~/.clair/
└── environments.yml     ← connection profile
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--project` | interactive prompt | Directory to initialise |

## See also

- [Quickstart](../quickstart.md)
- [Environments](../topics/environments.md)
