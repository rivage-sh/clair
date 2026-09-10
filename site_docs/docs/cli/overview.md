# CLI Overview

Each command parses the arguments and calls one function of the
[Python API](../reference/python-api.md). `clair run` calls `clair.run()`,
`clair compile` calls `clair.compile()`, and so on. A notebook or a program does
the same work with no `subprocess` call.

Each command finds the project root itself: it walks up from the working directory to the
first `__routing__.py`, in the same way that git finds `.git`. Thus you run a command from
any directory of the project. `clair init` is the exception, because it makes the root.
See [Project Layout](../topics/project-layout.md#the-project-root).

All commands share one common flag:

- `--env` — environment name. It names a key in `~/.clair/environments.yml` and an entry in `__routing__.py` (default: `CLAIR_ENV` or `dev`)

## Commands

| Command | Description | Snowflake connection? |
|---------|-------------|----------------------|
| [`clair init`](init.md) | Create a new project and configure Snowflake connection | No |
| [`clair compile`](compile.md) | Resolve DAG and write SQL to `_clairtifacts/` | No |
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
