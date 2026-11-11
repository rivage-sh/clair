# Retry the failed Trouves from an orchestrator

clair has no retry of its own. The orchestrator that runs clair — Airflow, Dagster, a
cron job — decides when to try again. This guide shows how the orchestrator retries only
the Trouves that did not succeed, from the structured result of the run.

Do not retry the complete command. A second `clair run` builds each Trouve again: a full
refresh costs the build a second time, and an `Append` Trouve that succeeded the first
time inserts its rows two times.

## From Python

`clair.run()` gives a `RunSummary`. Each result holds its status and its logical address,
which is the name that `select` matches.

```python
import clair
from clair import RunMode
from clair.core.runner import RunStatus

environment = clair.load_environment("prod")
summary = clair.run("~/projects/analytics", env=environment, run_mode=RunMode.INCREMENTAL)

retry_selection = [
    str(result.addresses.logical)
    for result in summary.results
    if result.status != RunStatus.SUCCESS
]
if retry_selection:
    summary = clair.run(
        "~/projects/analytics",
        env=environment,
        run_mode=RunMode.INCREMENTAL,
        select=retry_selection,
    )
```

The selection holds each Trouve that failed, and each Trouve that clair skipped because a
Trouve upstream of it failed. It holds no Trouve that succeeded.

## From the command line

`clair run` writes the complete `RunSummary` to `_clairtifacts/<run_id>/run_summary.json`
at the end of the run. A run id starts with the time, thus the last directory in name
order is the newest run.

```bash
clair run --env prod --run-mode incremental || {
    summary="$(ls -d _clairtifacts/*/ | sort | tail -1)run_summary.json"
    selection=$(jq -r '.results[]
        | select(.status != "success")
        | .addresses.logical
        | "--select=\(.database_name).\(.schema_name).\(.table_name)"' "$summary")
    clair run --env prod --run-mode incremental $selection
}
```

`RunSummary.model_validate_json()` reads the file back into Python.

## What a retry reads

A Trouve that the retry does not select reads its logical address. See
[The anatomy of a run](../topics/anatomy-of-a-run.md#6-resolve-the-addresses).

- In production, routing gives each Trouve its logical address. The retry thus reads the
  tables that the first run promoted.
- In an environment that routes the address, for example a `dev` database for each person,
  an upstream Trouve that succeeded the first time is not in the retry. Its dependents
  read the production table, and not the copy of the first run. Select it too, with a `+`
  in front of the pattern, if they must read that copy.

## When there is no summary

The summary file comes at the end of the run. If the process stopped before the end, for
example the orchestrator killed it, the file is not there. Retry the complete selection of
the first run. The file of each Trouve that completed is in the run directory, thus you
can see where the run stopped.

The staging objects of a failed Trouve stay in Snowflake for your examination. Drop them
after the retry. See [Staging](../topics/staging.md#after-a-failure).
