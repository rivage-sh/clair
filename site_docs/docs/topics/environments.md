# Environments

An environment is a named Snowflake connection profile. clair keeps the environments in `~/.clair/environments.yml`. This file is outside the project directory, thus you do not commit your credentials.

The CLI reads this file. The Python API does not: `clair.run()` and `clair.test()` take an `Environment` object. A notebook, a test, or a service that keeps its secrets in a vault makes the object, and writes no file. See [Python API](../reference/python-api.md).

## `~/.clair/environments.yml`

Each environment holds two blocks. `connection` holds the account and the session defaults. `authentication` holds one login method.

```yaml
dev:
  connection:
    account: myorg-myaccount
    user: alice@example.com
    warehouse: dev_warehouse
    role: analyst
    region: us-east-1
    account_locator: abc12345
  authentication:
    authenticator: externalbrowser
  threads: 4

prod:
  connection:
    account: myorg-myaccount
    user: ci_service_user
    warehouse: prod_warehouse
    role: transformer
    region: us-east-1
    account_locator: abc12345
  authentication:
    private_key_path: ~/.clair/snowflake_key.p8
  threads: 16
```

## Authentication methods

The key in the `authentication` block selects the method. A block that holds the keys of two methods is an error, and so is a block that holds no method. clair stops before it connects, and it names the key.

=== "Private key (recommended for CI)"

    ```yaml
    authentication:
      private_key_path: ~/.clair/snowflake_key.p8
      private_key_passphrase: your-passphrase   # only for an encrypted key
    ```

=== "Password"

    ```yaml
    authentication:
      password: your-password
    ```

=== "SSO (externalbrowser)"

    ```yaml
    authentication:
      authenticator: externalbrowser
    ```

    This method opens a browser window for the Okta/SSO login. Do not use it in CI.

    `authenticator` accepts each value of the Snowflake connector, for example `oauth`.

## Field reference

`connection`:

| Field | Required | Description |
|-------|----------|-------------|
| `account` | Yes | Snowflake account identifier (e.g. `myorg-myaccount`) |
| `user` | Yes | Snowflake username |
| `warehouse` | Yes | Default warehouse |
| `role` | — | Default role. If you omit it, Snowflake uses the default role of the user. |
| `region` | — | AWS/Azure region. clair needs it for the query URLs in the logs. |
| `account_locator` | — | Classic account locator. clair needs it for the query URLs. |

`authentication`, one of:

| Method | Fields |
|--------|--------|
| Private key | `private_key_path` (required), `private_key_passphrase` (only for an encrypted key) |
| Password | `password` |
| Authenticator | `authenticator`, for example `externalbrowser` |

Top level:

| Field | Required | Description |
|-------|----------|-------------|
| `connection` | Yes | See above. |
| `authentication` | Yes | See above. |
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
(`authentication: {authenticator: externalbrowser}`), clair asks the Snowflake connector to keep
the login token in the credential store of your operating system. The second
connection reads that token, thus clair opens one browser window and not one for
each thread.

## Select an environment

clair resolves the environment name in this order:

1. `--env` CLI flag
2. `CLAIR_ENV` environment variable
3. `"dev"` (default)

```bash
clair run --env prod
CLAIR_ENV=prod clair run
```

## CI usage

In CI, set `CLAIR_ENV` and use key-pair authentication. This method does not need a browser:

```yaml
# GitHub Actions example
- name: Run clair
  env:
    CLAIR_ENV: prod
    SNOWFLAKE_PRIVATE_KEY: ${{ secrets.SNOWFLAKE_PRIVATE_KEY }}
  run: clair run
```

## Routing

An environment holds connection settings only. The routing rules are in `__routing__.py`, at the root of your project. clair joins the two files by the environment name.

An unknown key in an environment block is an error. A `routing:` block from an older version of clair therefore stops the run, and the message tells you to move the rule. See [Routing](routing.md).
