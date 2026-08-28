# Integration tests

These tests call the Python API of clair -- `clair.run()`, `clair.test()` --
against a real Snowflake account, in the process of the test. They prove what
the unit tests cannot prove: that the compiled SQL is valid Snowflake SQL, and
that the routing entry sends each write to the target that you expect.

The call gives a `RunSummary`. A test therefore asks the run what happened: the
statements that Snowflake ran, the staging address of each Trouve, the effective
run mode, the index of the statement that failed, and each data quality test
result. An earlier version ran the CLI as a subprocess and read the JSON log
lines. A test could then see the exit code only, and an assertion about one step
of the run was not possible.

`tests/unit/test_cli_commands.py` covers the CLI itself: the arguments that each
command gives to the API, and the status code that it gives back to the shell.

The fixtures are the projects in `examples/projects/`. A change that breaks an
example therefore breaks the build.

| File | Holds |
|---|---|
| `conftest.py` | The schema of the run, the connection, and the environment variables that clair reads. |
| `config.py` | The connection settings, and the schema name rules. |
| `warehouse.py` | The Snowflake helpers: connect, execute, count. |
| `projects.py` | The example projects, and the test routing entry. |
| `setup.py` | Makes the schema of the run, and loads the source tables. |
| `clean_up.py` | Drops the schema of one run. |
| `test_examples.py` | Runs each example project. |
| `staging_project.py` | Makes a project whose data quality test fails on demand. |
| `test_staging.py` | Runs the staging steps: build, test, promote or keep. |
| `test_staging_incremental.py` | Runs the clone and the MERGE of an incremental run. |
| `scripts/` | The one-time Snowflake setup, for ACCOUNTADMIN. |

## How one run is isolated

Everything goes to one schema of the `clair_pr_testing` database:

| Trigger | Schema |
|---|---|
| A pull request | `pr_<number>` |
| A manual start | `run_<run_id>` |
| Your machine | The name that you give in `CLAIR_PR_TESTING_SCHEMA_NAME` |

The routing entry in `projects.py` sends **every** Trouve, a SOURCE too, to
`clair_pr_testing.<schema>.<database>__<schema>__<table>`. The logical Trouve
`example_1_database.refined.events` therefore becomes
`clair_pr_testing.pr_42.example_1_database__refined__events`. Two pull requests
never write one object, and the three logical parts stay visible in the name.

The example projects in the repository keep their own `__routing__.py`. A test
copies the project to a temporary directory and writes the CI entry there.

## The staging tests

`test_staging.py` needs a run that **fails**, and each example project passes.
`staging_project.py` therefore writes a small project with one `TestRowCount`.
A low limit passes, and a limit above the row count fails.

Each test class gives its own database name, for example
`staging_fail_database`, thus the tests never write one table. The test makes
the SOURCE table itself, so these tests need no golden schema.

An incremental candidate makes two objects, and a full refresh makes one:

| Object | Holds |
|---|---|
| `<table>__clair_<run_id>` | The staging table. A zero copy clone seeds it. |
| `<table>__clair_merge_<run_id>` | The new rows that the MERGE of an UPSERT reads. An APPEND makes no such table. |

The candidate Trouve runs on one of the two engines, because they write in
different ways. A SQL Trouve runs `CREATE OR REPLACE TABLE` at the staging
address. A pandas Trouve calls `write_pandas`, and that function makes the
staging table itself. Clair then promotes both with the same clone.

### The grants test

`GRANT SELECT ON FUTURE TABLES IN DATABASE` gives SELECT to the test role on
each new table. The promotion makes a new object, so Snowflake gives SELECT
again on its own. A test that asks for SELECT therefore passes even when
`COPY GRANTS` does nothing.

Give a privilege with no future grant, for example INSERT. Then the promotion
is the one path that can carry the privilege over, and the test can fail.

## The source tables

`tests/integration/scripts/clair_pr_testing_setup.sql` makes one golden schema
for each project, for example `clair_pr_testing.example_1`. A run clones each
golden table into its own schema. A clone is a zero copy operation, thus it is
fast, and the run can write to its copy.

Each date in a golden table is fixed. `example_3_database.derived.recent_orders`
selects `created_at > dateadd('day', -3, current_timestamp())`, and no golden
row reaches that window. The incremental test inserts its own rows with
`current_timestamp()`, thus it knows the exact number of new rows.

## The schema of a run

The run **starts** with a drop of its own schema, then it makes the schema
again. A second commit of one pull request reuses the schema name of the first,
and a Trouve that the commit deleted would otherwise stay behind and give a
false pass.

The run does **not** drop the schema at the end. You can therefore read the
tables of a failed run. `.github/workflows/integration-clean-up.yml` drops the
schema when the pull request closes, merged or not.

To drop one schema by hand:

```bash
uv run python -m tests.integration.clean_up --schema-name pr_42
```

## Run the tests on your machine

```bash
export CLAIR_PR_TESTING_SNOWFLAKE_ACCOUNT=...
export CLAIR_PR_TESTING_SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/clair_pr_testing_f.p8
export CLAIR_PR_TESTING_SCHEMA_NAME=local_<you>_<branch>
uv run pytest tests/integration -m integration -v
```

`CLAIR_PR_TESTING_SCHEMA_NAME` is mandatory, and it has no default. The run
drops that schema before it starts, thus two runs that share one name delete the
tables of each other. Give a different name to each run that you start at one
time: two worktrees, or two agents, need two names.

| Variable | Mandatory | Meaning |
|---|---|---|
| `CLAIR_PR_TESTING_SNOWFLAKE_ACCOUNT` | Yes | The account identifier. |
| `CLAIR_PR_TESTING_SNOWFLAKE_PRIVATE_KEY_PATH` | One of the two | The path of the PEM private key. |
| `CLAIR_PR_TESTING_SNOWFLAKE_PASSWORD` | One of the two | A password. The workflow uses the key pair. |
| `CLAIR_PR_TESTING_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` | No | For an encrypted key. |
| `CLAIR_PR_TESTING_SNOWFLAKE_USER` | No | The default is `clair_pr_testing_user`. |
| `CLAIR_PR_TESTING_SNOWFLAKE_ROLE` | No | The default is `clair_pr_testing_f`. |
| `CLAIR_PR_TESTING_SNOWFLAKE_WAREHOUSE` | No | The default is `clair_pr_testing_wh`. |
| `CLAIR_PR_TESTING_SCHEMA_NAME` | Yes | The schema of the run. It has no default. |

The user, the role and the warehouse are names inside the account, and
`tests/integration/scripts/clair_pr_testing_setup.sql` makes them. They are not secrets.

The `clair_environment` fixture sets HOME, `CLAIR_ENV` and
`CLAIR_PR_TESTING_SCHEMA_NAME` for the session. Clair reads them in this
process, thus a test that calls the API asks for that fixture first.

Without the account and the credentials, the tests **skip**. The integration
workflow sets `CLAIR_PR_TESTING_REQUIRE_SNOWFLAKE=1`, and the tests then **fail**
instead. A job with no credentials would otherwise report success after it ran
nothing.

## The GitHub Actions setup

The `snowflake-integration` environment holds two secrets:

| Secret | Value |
|---|---|
| `CLAIR_PR_TESTING_SNOWFLAKE_ACCOUNT` | The account identifier. |
| `CLAIR_PR_TESTING_SNOWFLAKE_PRIVATE_KEY_BASE64` | `base64 -i clair_pr_testing_f.p8 \| tr -d '\n'` |

An environment secret reaches a job that names that environment only. A future
workflow therefore cannot read the Snowflake key by accident.

## Why this is safe in a public repository

| Risk | The control |
|---|---|
| A pull request from a fork reads the secrets. | The trigger is `pull_request`, and GitHub gives no secret to a fork run. The `if:` condition also stops the job, thus it reports "skipped". **Never** change the trigger to `pull_request_target`. |
| A person with write access steals the credentials. | Write access to the repository **is** access to the credentials: the job runs the code of the branch, thus a new test can print a secret. Put required reviewers on the `snowflake-integration` environment when a second person gets write access. |
| A test drops the wrong schema. | `normalise_schema_name` refuses `public`, `information_schema` and each golden schema, and it refuses a name with an unusual character. |
| The credentials reach another database. | `clair_pr_testing_f` has a grant on the `clair_pr_testing` database only. |
| A workflow uses too many credits. | `clair_pr_testing_monitor` suspends the warehouse at 10 credits each month, and it sends an email at 8 credits. The warehouse suspends after 60 seconds. A draft pull request starts no run. |
| The private key stays on the runner. | The workflow writes it to `RUNNER_TEMP` with mode 600, and it removes the file in the last step. |
| A leaked key. | `alter user clair_pr_testing_user unset rsa_public_key;`, then make a new key pair. The user reaches one database. |

Do not make the `Integration / Snowflake` job a required status check: a pull
request from a fork always skips it, and a required check would block the merge.
