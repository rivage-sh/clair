# CLI Overview

All commands share two common flags:

- `--project` — path to the clair project root (default: `.`)
- `--env` — environment name. It names a key in `~/.clair/environments.yml` and an entry in `__routing__.py` (default: `CLAIR_ENV` or `dev`)

## Commands

| Command | Description | Snowflake connection? |
|---------|-------------|----------------------|
| [`clair init`](init.md) | Create a new project and configure Snowflake connection | No |
| [`clair compile`](compile.md) | Resolve DAG and write SQL to `_clairtifacts/` | Optional (for routing) |
| [`clair run`](run.md) | Run Trouves against Snowflake in dependency order | **Yes** |
| [`clair test`](test.md) | Run data quality tests against Snowflake | **Yes** |
| [`clair validate`](validate.md) | Apply the routing entries to every Trouve | No |
| [`clair dag`](dag.md) | Print the dependency graph as an indented tree | No |
| [`clair docs`](docs.md) | Start a local web UI for the DAG and the documentation | No |
| [`clair clean`](clean.md) | Remove compiled artifacts from `_clairtifacts/` | No |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CLAIR_ENV` | `dev` | Active environment name. This is the same as `--env` on every command. `--env` wins if you set both. |
| `CLAIR_USER` | — | The `clair init` routing template reads this name. Give each variable that a routing entry reads the `CLAIR_` prefix. |
| `CLAIR_LOG_FORMAT` | _(text)_ | Set to `json` to write structured JSON logs. Use this in CI/CD pipelines and in container environments that read JSON logs. |

## Help

```bash
clair --help
clair run --help
clair compile --help
```
