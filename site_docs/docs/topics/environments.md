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

prod:
  account: myorg-myaccount
  user: ci_service_user
  private_key_path: ~/.clair/snowflake_key.p8
  warehouse: prod_warehouse
  role: transformer
  region: us-east-1
  account_locator: abc12345
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
