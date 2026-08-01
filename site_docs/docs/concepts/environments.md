# Environments

An environment is a named Snowflake connection profile. Environments are stored in `~/.clair/environments.yml` — outside the project directory so credentials are never committed.

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

    Opens a browser window for Okta/SSO login. Not suitable for CI.

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
| `role` | — | Default role (falls back to user's default if omitted) |
| `region` | — | AWS/Azure region (required for query URLs in logs) |
| `account_locator` | — | Classic account locator (required for query URLs) |
| `routing` | — | Routing policy. See [Routing Policies](../guides/routing.md). |

## Selecting an environment

The environment name is resolved in this order:

1. `--env` CLI flag
2. `CLAIR_ENV` environment variable
3. `"dev"` (default)

```bash
clair run --project . --env prod
CLAIR_ENV=prod clair run --project .
```

## CI usage

In CI, set `CLAIR_ENV` and use key-pair authentication (no browser interaction required):

```yaml
# GitHub Actions example
- name: Run clair
  env:
    CLAIR_ENV: prod
    SNOWFLAKE_PRIVATE_KEY: ${{ secrets.SNOWFLAKE_PRIVATE_KEY }}
  run: clair run --project .
```

## Routing policies

Each environment can optionally include a routing policy that remaps logical Snowflake names to physical targets. See [Routing Policies](../guides/routing.md).
