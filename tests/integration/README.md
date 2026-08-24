# Integration tests

These tests run the clair CLI as a subprocess against a real Snowflake account.
They prove what the unit tests cannot prove: that the compiled SQL is valid
Snowflake SQL, and that the routing entry sends each write to the target that
you expect.

The fixtures are the projects in `examples/projects/`. A change that breaks an
example therefore breaks the build.

| File | Holds |
|---|---|
| `config.py` | The connection settings, and the schema name rules. |
| `warehouse.py` | The Snowflake helpers: connect, execute, count. |
| `projects.py` | The example projects, and the CI routing entry. |
| `setup.py` | Makes the schema of the run, and loads the source tables. |
| `clean_up.py` | Drops the schema of one run. |
| `test_examples.py` | Runs each example project. |
| `seeds/` | The SQL for a source table that needs a young timestamp. |

## How one run is isolated

Everything goes to one schema of the `clair_pr_testing` database:

| Trigger | Schema |
|---|---|
| A pull request | `pr_<number>` |
| A manual start | `run_<run_id>` |
| Your machine | `local_<user>_<pid>` |

The routing entry in `projects.py` sends **every** Trouve, a SOURCE too, to
`clair_pr_testing.<schema>.<database>__<schema>__<table>`. The logical Trouve
`example_1_database.refined.events` therefore becomes
`clair_pr_testing.pr_42.example_1_database__refined__events`. Two pull requests
never write one object, and the three logical parts stay visible in the name.

The example projects in the repository keep their own `__routing__.py`. A test
copies the project to a temporary directory and writes the CI entry there.

## The source tables

`scripts/snowflake_ci_setup.sql` makes one golden schema for each project, for
example `clair_pr_testing.example_1`. A run clones each golden table into its
own schema. A clone is a zero copy operation, thus it is fast, and the run can
write to its copy.

`example_3` is the exception. Its `derived.recent_orders` Trouve selects
`created_at > dateadd('day', -3, current_timestamp())`, thus the rows must be
young at each run. A golden table holds the timestamps of the day that you made
it. `seeds/example_3_orders.sql` therefore makes those rows again in the schema
of the run.

## The schemas stay after the tests

The tests do not drop the schema at the end of a run in CI. You can therefore
read the tables of a failed run. `.github/workflows/integration-cleanup.yml`
drops the schema when the pull request closes, merged or not.

To drop one schema by hand:

```bash
uv run python -m tests.integration.clean_up --schema-name pr_42
```

## Run the tests on your machine

```bash
export CLAIR_CI_SNOWFLAKE_ACCOUNT=...
export CLAIR_CI_SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/clair_pr_testing_f.p8
uv run pytest tests/integration -m integration -v
```

| Variable | Mandatory | Meaning |
|---|---|---|
| `CLAIR_CI_SNOWFLAKE_ACCOUNT` | Yes | The account identifier. |
| `CLAIR_CI_SNOWFLAKE_PRIVATE_KEY_PATH` | One of the two | The path of the PEM private key. |
| `CLAIR_CI_SNOWFLAKE_PASSWORD` | One of the two | A password. CI uses the key pair. |
| `CLAIR_CI_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` | No | For an encrypted key. |
| `CLAIR_CI_SNOWFLAKE_USER` | No | The default is `clair_pr_testing_user`. |
| `CLAIR_CI_SNOWFLAKE_ROLE` | No | The default is `clair_pr_testing_f`. |
| `CLAIR_CI_SNOWFLAKE_WAREHOUSE` | No | The default is `clair_pr_testing_wh`. |
| `CLAIR_CI_SCHEMA_NAME` | No | The default is `local_<user>_<pid>`. |

The user, the role and the warehouse are names inside the account, and
`scripts/snowflake_ci_setup.sql` makes them. They are not secrets.

Without the account and the credentials, the tests **skip**. The integration
workflow sets `CLAIR_CI_REQUIRE_SNOWFLAKE=1`, and the tests then **fail**
instead. A job with no credentials would otherwise report success after it ran
nothing.

## The GitHub Actions setup

The `snowflake-integration` environment holds two secrets:

| Secret | Value |
|---|---|
| `CLAIR_CI_SNOWFLAKE_ACCOUNT` | The account identifier. |
| `CLAIR_CI_SNOWFLAKE_PRIVATE_KEY_BASE64` | `base64 -i clair_pr_testing_f.p8 \| tr -d '\n'` |

An environment secret reaches a job that names that environment only. A future
workflow therefore cannot read the Snowflake key by accident.

## Why this is safe in a public repository

| Risk | The control |
|---|---|
| A pull request from a fork reads the secrets. | The trigger is `pull_request`, and GitHub gives no secret to a fork run. The `if:` condition also stops the job, thus it reports "skipped". **Never** change the trigger to `pull_request_target`. |
| A person with write access steals the credentials. | Write access to the repository **is** access to the credentials: the job runs the code of the branch, thus a new test can print a secret. Put required reviewers on the `snowflake-integration` environment when a second person gets write access. |
| A test drops the wrong schema. | `normalise_schema_name` refuses `public`, `information_schema` and each golden schema, and it refuses a name with an unusual character. |
| The credentials reach another database. | `clair_pr_testing_f` has a grant on the `clair_pr_testing` database only. |
| A workflow uses too many credits. | `clair_pr_testing_monitor` suspends the warehouse at 5 credits each month, and the warehouse suspends after 20 seconds. |
| The private key stays on the runner. | The workflow writes it to `RUNNER_TEMP` with mode 600, and it removes the file in the last step. |
| A leaked key. | `alter user clair_pr_testing_user unset rsa_public_key;`, then make a new key pair. The user reaches one database. |

Do not make the `Integration / Snowflake` job a required status check: a pull
request from a fork always skips it, and a required check would block the merge.
