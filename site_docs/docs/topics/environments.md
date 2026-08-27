# Environments

An environment is a named Snowflake connection profile. clair keeps the environments in `~/.clair/environments.yml`. This file is outside the project directory, thus you do not commit your credentials.

## `~/.clair/environments.yml`

```yaml
dev:
  account: myorg-myaccount
  user: alice@example.com
  authenticator: externalbrowser
  warehouse: dev_warehouse
  role: analyst
  region: us-east-1
  account_locator: abc12345
  threads: 4

prod:
  account: myorg-myaccount
  user: ci_service_user
  private_key_path: ~/.clair/snowflake_key.p8
  warehouse: prod_warehouse
  role: transformer
  region: us-east-1
  account_locator: abc12345
  threads: 16
```

## Authentication methods

=== "Private key (recommended for CI)"

    ```yaml
    dev:
      account: myorg-myaccount
      user: alice@example.com
      private_key_path: ~/.clair/snowflake_key.p8
      warehouse: my_warehouse
    ```

    For an encrypted key, add:

    ```yaml
      private_key_passphrase: your-passphrase
    ```

=== "Password"

    ```yaml
    dev:
      account: myorg-myaccount
      user: alice@example.com
      password: your-password
      warehouse: my_warehouse
    ```

=== "SSO (externalbrowser)"

    ```yaml
    dev:
      account: myorg-myaccount
      user: alice@example.com
      authenticator: externalbrowser
      warehouse: my_warehouse
    ```

    This method opens a browser window for the Okta/SSO login. Do not use it in CI.

## Field reference

| Field | Required | Description |
|-------|----------|-------------|
| `account` | Yes | Snowflake account identifier (e.g. `myorg-myaccount`) |
| `user` | Yes | Snowflake username |
| `warehouse` | Yes | Default warehouse |
| `authenticator` | — | Set to `externalbrowser` for SSO |
| `password` | — | Plain-text password |
| `private_key_path` | — | Path to PEM private key file |
| `private_key_passphrase` | — | Passphrase for encrypted private keys |
| `role` | — | Default role. If you omit it, Snowflake uses the default role of the user. |
| `region` | — | AWS/Azure region. clair needs it for the query URLs in the logs. |
| `account_locator` | — | Classic account locator. clair needs it for the query URLs. |
| `threads` | — | The number of Trouves that clair runs at one time. The default is `4`. See [Parallel execution](#parallel-execution). |

## Parallel execution

`threads` gives the number of Trouves that clair runs at one time. Each thread
holds a private Snowflake connection, thus the value is also the number of
sessions that clair opens. `clair run --threads` and `clair test --threads`
replace the value for one command, and so does the `threads` argument of
[`clair.run()` and `clair.test()`](../reference/python-api.md).

The value belongs to the environment, because the correct number comes from the
warehouse. The default is 4, which is also the dbt default.

A thread makes no compute of its own. Each thread sends its queries to the one
warehouse of the environment, thus the warehouse decides how many of them run:

| The warehouse | The result of a high thread count |
|---|---|
| One cluster | It runs `MAX_CONCURRENCY_LEVEL` statements at one time, 8 by default. It queues the others. A count above 8 gives no more parallel work. |
| Many clusters, auto-scale mode | It starts another cluster when the queue grows. Snowflake bills each cluster that runs, thus a high count can raise your bill. |

A connection costs no credits. Snowflake bills the warehouse per second while it
runs, and a session that sends no query starts no warehouse. Raise the count
against a warehouse that queues your Trouves, and not against one that is idle.

```yaml
dev:
  # ...
  threads: 4

prod:
  # ...
  threads: 16
```

clair opens each connection before the first Trouve starts. With SSO
(`authenticator: externalbrowser`), clair asks the Snowflake connector to keep
the login token in the credential store of your operating system. The second
connection reads that token, thus clair opens one browser window and not one for
each thread.

## Select an environment

clair resolves the environment name in this order:

1. `--env` CLI flag
2. `CLAIR_ENV` environment variable
3. `"dev"` (default)

```bash
clair run --project . --env prod
CLAIR_ENV=prod clair run --project .
```

## CI usage

In CI, set `CLAIR_ENV` and use key-pair authentication. This method does not need a browser:

```yaml
# GitHub Actions example
- name: Run clair
  env:
    CLAIR_ENV: prod
    SNOWFLAKE_PRIVATE_KEY: ${{ secrets.SNOWFLAKE_PRIVATE_KEY }}
  run: clair run --project .
```

## Routing

An environment holds connection settings only. The routing rules are in `__routing__.py`, at the root of your project. clair joins the two files by the environment name.

An unknown key in an environment block is an error. A `routing:` block from an older version of clair therefore stops the run, and the message tells you to move the rule. See [Routing](routing.md).
