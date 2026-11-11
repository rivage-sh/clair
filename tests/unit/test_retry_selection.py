"""The recipe of the guide "Retry the failed Trouves" selects only what did not succeed.

An orchestrator reads ``run_summary.json``, and gives each Trouve whose status
is not success to ``select``. The retry must build each failed Trouve and each
Trouve that clair skipped, and no Trouve that succeeded: a second build of an
``Append`` Trouve inserts its rows two times.
"""

from __future__ import annotations

import json
from pathlib import Path

import clair
from clair.core.artifacts import write_run_summary
from clair.core.runner import RunStatus, RunSummary
from tests.helpers import RecordingAdapter, make_environment, write_project_marker

# raw.events -> derived.daily -> derived.weekly, and derived.lookup alone.
FAILING = "db.derived.daily"
SKIPPED = "db.derived.weekly"
SUCCEEDED = "db.derived.lookup"


def _write_project(project_root: Path) -> Path:
    write_project_marker(project_root)
    for schema_name in ("raw", "derived"):
        (project_root / "db" / schema_name).mkdir(parents=True)
    (project_root / "db" / "raw" / "events.py").write_text("from clair import Source\ntrouve = Source()\n")
    (project_root / "db" / "derived" / "daily.py").write_text(
        "from clair import Append, Incremental, Table\n"
        "from db.raw.events import trouve as events\n"
        'trouve = Table(sql=f"select * from {events}", run_config=Incremental(strategy=Append()))\n'
    )
    (project_root / "db" / "derived" / "weekly.py").write_text(
        "from clair import Table\n"
        "from db.derived.daily import trouve as daily\n"
        'trouve = Table(sql=f"select * from {daily}")\n'
    )
    (project_root / "db" / "derived" / "lookup.py").write_text(
        "from clair import Append, Incremental, Table\n"
        'trouve = Table(sql="select 1 as id", run_config=Incremental(strategy=Append()))\n'
    )
    return project_root


def test_the_retry_of_the_guide_builds_only_what_did_not_succeed(tmp_path):
    project_root = _write_project(tmp_path / "project")
    first_summary = clair.run(
        project_root,
        env=make_environment(),
        run_mode=clair.RunMode.INCREMENTAL,
        adapter=RecordingAdapter(fail_on=[f"INSERT INTO {FAILING}"]),
    )
    summary_file = write_run_summary(tmp_path / "_clairtifacts", first_summary)

    # The Python recipe of the guide, on the file that clair run writes.
    summary = RunSummary.model_validate_json(summary_file.read_text())
    retry_selection = [
        str(result.addresses.logical)
        for result in summary.results
        if result.status != RunStatus.SUCCESS
    ]
    assert sorted(retry_selection) == [FAILING, SKIPPED]

    retry_adapter = RecordingAdapter()
    retry_summary = clair.run(
        project_root,
        env=make_environment(),
        run_mode=clair.RunMode.INCREMENTAL,
        adapter=retry_adapter,
        select=retry_selection,
    )

    assert sorted(str(result.addresses.logical) for result in retry_summary.results) == [FAILING, SKIPPED]
    assert SUCCEEDED not in " ".join(retry_adapter.record.statements)


def test_the_jq_recipe_gives_the_address_that_select_matches(tmp_path):
    # The guide joins the three parts of the JSON address with jq. The text
    # must be the logical address, which select matches.
    project_root = _write_project(tmp_path / "project")
    summary = clair.run(
        project_root, env=make_environment(), adapter=RecordingAdapter(fail_on=[FAILING])
    )
    summary_file = write_run_summary(tmp_path / "_clairtifacts", summary)

    patterns = [
        "{database_name}.{schema_name}.{table_name}".format(**result["addresses"]["logical"])
        for result in json.loads(summary_file.read_text())["results"]
        if result["status"] != "success"
    ]

    assert sorted(patterns) == [FAILING, SKIPPED]
