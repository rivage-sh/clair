# clair prune

Drop the objects that one run left in Snowflake.

```bash
clair prune --env ENV --run-id RUN_ID [--dry-run] [--yes]
```

A run writes two kinds of object for itself, and the name of each ends with the run id:

| The object | Its name | When it stays |
|------------|----------|---------------|
| The staging object of a Trouve | `<table>__clair_<run_id>` | A build, a test or a promotion failed. See [Staging](../topics/staging.md#after-a-failure). |
| The new rows table of an `Upsert` or a `DeleteInsert` | `<table>__clair_new_rows_<run_id>` | The run stopped in the middle of the node. |

A staging table from a full refresh is a complete copy of its table, thus it costs
storage until you drop it. `clair prune` finds each object of the run in each database
that the role sees, also for a Trouve that the project no longer holds, and drops it with
the `DROP` of its type. It needs no project directory.

Run it after the run ends. A run that continues needs its objects.

## Example

```bash
# Print the DROP statements, and execute none of them
clair prune --env prod --run-id 01a1c8936b5175e3b0622ff0eb45d9cc --dry-run

# Drop after you confirm
clair prune --env prod --run-id 01a1c8936b5175e3b0622ff0eb45d9cc

# Skip confirmation, for example in an orchestrator
clair prune --env prod --run-id 01a1c8936b5175e3b0622ff0eb45d9cc --yes
```

## Find the run id

- The error of a failed Trouve names the staging object, and its name ends with the run id.
- `clair run` writes the artifacts of a run to `_clairtifacts/<run_id>/`.
- `run_summary.json` holds `run_id`.
- The query tag of each statement holds `run_id`.

The run id is 32 hexadecimal characters. Snowflake shows the name of an object in upper
case, and clair accepts the run id in each case. A text that is not a run id stops the
command before it connects, because a short text would match the names of many objects.

## Options

| Option | Description |
|--------|-------------|
| `--env` | The environment, which gives the connection. `CLAIR_ENV` gives it when you omit it. |
| `--run-id` | The id of the run. |
| `--dry-run` | Print the DROP statements, and execute none of them. Execute them yourself, if you prefer. |
| `--yes` | Do not ask for confirmation. |

The status code is 1 if a `DROP` fails.
