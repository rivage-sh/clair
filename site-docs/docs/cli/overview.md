# CLI Overview

All commands share two common flags:

- `--project` — path to the clair project root (default: `.`)
- `--env` — environment name from `~/.clair/environments.yml` (default: `CLAIR_ENV` or `dev`)

## Commands

| Command | Description | Snowflake connection? |
|---------|-------------|----------------------|
| [`clair init`](init.md) | Create a new project and configure Snowflake connection | No |
| [`clair compile`](compile.md) | Resolve DAG and write SQL to `_clairtifacts/` | Optional (for routing) |
| [`clair run`](run.md) | Execute Trouves against Snowflake in dependency order | **Yes** |
| [`clair test`](test.md) | Run data quality tests against Snowflake | **Yes** |
| [`clair dag`](dag.md) | Print the dependency graph as an indented tree | No |
| [`clair docs`](docs.md) | Start a local web UI showing the DAG and documentation | No |
| [`clair clean`](clean.md) | Remove compiled artifacts from `_clairtifacts/` | No |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CLAIR_ENV` | `dev` | Active environment name. Equivalent to passing `--env` on every command. `--env` takes precedence if both are set. |
| `CLAIR_LOG_FORMAT` | _(text)_ | Set to `json` to emit structured JSON logs. Useful in CI/CD pipelines and container environments that ingest JSON logs. |

## Help

```bash
clair --help
clair run --help
clair compile --help
```
