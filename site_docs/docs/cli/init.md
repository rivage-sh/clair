# clair init

Create a new clair project with example Trouves and a Snowflake connection profile.

```bash
clair init [--project PATH]
```

## What it does

1. Prompts for a project directory (default: current directory)
2. If `~/.clair/environments.yml` doesn't exist, walks through an interactive Snowflake connection setup
3. Prompts for an example source table name in the format `database.schema.table`
4. Creates a starter project with a SOURCE Trouve for that table
5. Writes a `.gitignore` excluding `_clairtifacts/`

## Interactive session

```
Project directory [.]: ./my_project

  hint: select concat(current_organization_name(), '-', current_account_name()) as account;
Snowflake account (e.g. myorg-myaccount): myorg-myaccount

  hint: select current_user() as user;
Snowflake user: alice@example.com

Authentication method:
  1. Private key
  2. Password
  3. SSO (externalbrowser)
Enter choice [1]: 3

  hint: select current_warehouse() as warehouse;
Warehouse: dev_warehouse

Role (leave blank to use user default): analyst

  hint: select current_region() as region;
Region (e.g. us-east-1): us-east-1

  hint: select current_account() as account_locator;
Account locator (e.g. abc12345): abc12345

What is an example Snowflake table that contains source data? (eg source.orders.raw) [source]: source.orders.raw

✓ Project ready.

Next steps:
  1. clair compile --project /path/to/my_project
  2. clair run    --project /path/to/my_project
```

## Files created

```
my_project/
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
- [Environments](../concepts/environments.md)
