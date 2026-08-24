# Integration tests

These tests run the clair CLI as a subprocess against a real Snowflake account.
They prove what the unit tests cannot prove: that `clair init` makes a project
that runs, that the compiled SQL is valid Snowflake SQL, and that the routing
entry sends each write to the target that you expect.

| File | What it proves |
|---|---|
| `test_init_cli.py` | `clair init` writes a project and a connection profile. **No Snowflake account.** |
| `test_init_project_runs.py` | A project from `clair init` runs against Snowflake. |
| `test_pipeline.py` | A full refresh, an incremental run, `--select`, and the data quality tests. |
| `pipeline_project/` | The clair project that `test_pipeline.py` runs. |
| `ci_snowflake.py` | The seed data, the schema setup, and the cleanup. |
| `snowflake_setup.sql` | The one-time Snowflake setup. Run it with ACCOUNTADMIN. |

## How an isolated run works

All runs share one database, `CLAIR_CI`. Each run gets a **schema prefix**:

| Trigger | Prefix |
|---|---|
| A pull request | `PR_<number>` |
| A push to main, or a manual start | `CI_<run_id>` |
| Your machine | `LOCAL_<user>_<pid>` |

`pipeline_project/__routing__.py` reads `CLAIR_CI_SCHEMA_PREFIX` and puts the
prefix in front of each logical schema name. The logical Trouve
`clair_ci.refined.events` therefore writes to `CLAIR_CI.PR_42_REFINED.EVENTS`.
Two pull requests never write one object.

The workflow drops the schemas of the run after the job, and
`.github/workflows/integration-janitor.yml` drops each schema that a cancelled
job left behind.

### The seed tables are shared

A SOURCE Trouve **never** routes: clair always reads its logical name. The seed
tables must therefore live at `CLAIR_CI.SEED.EVENTS` and `CLAIR_CI.SEED.ORDERS`,
and every run reads the same two tables.

This is safe, because the seed statements in `ci_snowflake.py` write the same
rows on each run, and `CREATE OR REPLACE TABLE` is atomic in Snowflake. A run
therefore always reads a complete table with the content that it expects.

One consequence: a pull request that changes the seed data changes the data that
a parallel pull request reads. Start such a pull request again after you merge
it.

### clair does not make the schema

`clair run` sends `CREATE OR REPLACE TABLE database.schema.table`. It does not
make the schema. The `snowflake_workspace` fixture makes the schemas of the run
before the first test.

## Run the tests on your machine

1. Run `snowflake_setup.sql` once, with ACCOUNTADMIN.
2. Export the variables below, then:

```bash
uv run pytest tests/integration -m integration -v
```

| Variable | Mandatory | Meaning |
|---|---|---|
| `CLAIR_CI_SNOWFLAKE_ACCOUNT` | Yes | The account identifier, for example `myorg-myaccount`. |
| `CLAIR_CI_SNOWFLAKE_USER` | Yes | The service user, for example `CLAIR_CI_USER`. |
| `CLAIR_CI_SNOWFLAKE_ROLE` | Yes | The role, for example `CLAIR_CI_ROLE`. |
| `CLAIR_CI_SNOWFLAKE_WAREHOUSE` | Yes | The warehouse, for example `CLAIR_CI_WH`. |
| `CLAIR_CI_SNOWFLAKE_PRIVATE_KEY_PATH` | One of the two | The path of the PEM private key. |
| `CLAIR_CI_SNOWFLAKE_PASSWORD` | One of the two | A password. Use the key pair in CI. |
| `CLAIR_CI_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` | No | The passphrase of an encrypted key. |
| `CLAIR_CI_SCHEMA_PREFIX` | No | The default is `LOCAL_<user>_<pid>`. |

Without these variables, `uv run pytest` skips the Snowflake tests. It does not
fail them, thus a contributor with no account still runs the complete unit test
suite.

To drop your schemas by hand:

```bash
uv run python -m tests.integration.ci_snowflake drop-schemas --prefix LOCAL_ALICE_1234
```

## The GitHub Actions setup

Make an environment named `snowflake-integration`
(Settings → Environments → New environment). Put these secrets **on that
environment**, not on the repository:

| Secret | Value |
|---|---|
| `CLAIR_CI_SNOWFLAKE_ACCOUNT` | The account identifier. |
| `CLAIR_CI_SNOWFLAKE_USER` | `CLAIR_CI_USER` |
| `CLAIR_CI_SNOWFLAKE_ROLE` | `CLAIR_CI_ROLE` |
| `CLAIR_CI_SNOWFLAKE_WAREHOUSE` | `CLAIR_CI_WH` |
| `CLAIR_CI_SNOWFLAKE_PRIVATE_KEY_BASE64` | `base64 -i clair_ci_key.p8 \| tr -d '\n'` |
| `CLAIR_CI_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` | Only for an encrypted key. |

Then, on the same environment page, add **Required reviewers** and add yourself.
GitHub then asks for your approval before a job reaches the credentials.

## Why this is safe in a public repository

| Risk | The control |
|---|---|
| A pull request from a fork reads the secrets. | The trigger is `pull_request`, and GitHub gives no secret to a fork run. The `if:` condition also stops the job. **Never** change the trigger to `pull_request_target`. |
| A new collaborator starts a job that steals the credentials. | The `snowflake-integration` environment holds the secrets, and required reviewers gate it. |
| A test drops the wrong schema. | `ci_snowflake.py` reads the schema names from `INFORMATION_SCHEMA` first, and it compares them in Python. It refuses `SEED`, `PUBLIC` and `INFORMATION_SCHEMA`, and it refuses a prefix with an unusual character. |
| The credentials reach another database. | `CLAIR_CI_ROLE` has a grant on the `CLAIR_CI` database only. |
| A workflow uses too many credits. | The `CLAIR_CI_MONITOR` resource monitor suspends the warehouse at 5 credits each month. |
| The private key stays on the runner. | The workflow writes it to `RUNNER_TEMP` with mode 600, and it removes the file in the last step. |
| A leaked key. | Run `ALTER USER CLAIR_CI_USER UNSET RSA_PUBLIC_KEY;` and make a new key pair. The user reaches one database, thus the damage stays small. |

Do not make the `Integration / Snowflake` job a required status check: a pull
request from a fork always skips it, and a required check would block the merge.
