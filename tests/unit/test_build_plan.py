"""The runner executes a BuildPlan: the statements in order, then the cleanup.

The runner stops at the first statement that fails, and it executes no
statement after it. The cleanup statements run after the statements, when the
statements pass and when one of them fails. A subclass that overrides
``build_sql()`` gets the same treatment as a built-in mode.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from structlog.testing import capture_logs

import clair
from clair.adapters.base import StatementStatus
from clair.core.dag import build_dag, get_executable_nodes
from clair.core.runner import RunResult, RunStatus, run_project
from clair.environments.routing import TrouveAddress
from clair.trouves.column import Column, ColumnType
from clair.trouves.run_config import IncrementalMode, RunConfig, RunMode
from clair.trouves.trouve import BuildPlan, Trouve
from tests.helpers import RecordingAdapter, make_compiled_trouve

PHYSICAL_ADDRESS = "db.s.planned"
STATEMENTS = ["select 'step one'", "select 'step two'", "select 'step three'"]
# RecordingAdapter gives a SELECT the select row count, and any other statement
# the write row count. Thus a row count tells a statement from a cleanup.
CLEANUP_STATEMENTS = ["drop table if exists db.s.cleanup_one", "drop table if exists db.s.cleanup_two"]


class PlannedTrouve(Trouve):
    """A Trouve that gives the plan of the test, as a subclass of an author does."""

    build_plan: BuildPlan

    def build_sql(
        self,
        effective_mode: RunMode,
        run_id: str,
        staging_address: TrouveAddress | None = None,
    ) -> BuildPlan:
        return self.build_plan


def executed_plan_statements(adapter: RecordingAdapter) -> list[str]:
    """Give the statements of the plan that the adapter executed, without the DDL of the schema."""
    plan_statements = {*STATEMENTS, *CLEANUP_STATEMENTS}
    return [sql for sql in adapter.record.statements if sql in plan_statements]


def run_plan(build_plan: BuildPlan, adapter: RecordingAdapter) -> RunResult:
    trouve = PlannedTrouve(sql="select 1", build_plan=build_plan)
    trouve.compiled = make_compiled_trouve(PHYSICAL_ADDRESS).compiled
    dag = build_dag([trouve])
    (result,) = run_project(dag, get_executable_nodes(dag), adapter, run_id="test")
    return result


@pytest.mark.parametrize("failed_index", [0, 1, 2])
def test_no_statement_runs_after_the_one_that_fails(failed_index: int) -> None:
    adapter = RecordingAdapter(fail_on=[STATEMENTS[failed_index]])

    result = run_plan(BuildPlan(statements=STATEMENTS), adapter)

    assert executed_plan_statements(adapter) == STATEMENTS[: failed_index + 1]
    assert result.status == RunStatus.FAILURE
    assert [statement.status for statement in result.statements[failed_index + 1 :]] == [
        StatementStatus.NOT_RUN
    ] * (len(STATEMENTS) - failed_index - 1)


@pytest.mark.parametrize("failed_statement", [None, *STATEMENTS])
def test_the_cleanup_runs_after_the_statements_pass_or_fail(
    failed_statement: str | None,
) -> None:
    adapter = RecordingAdapter(fail_on=[failed_statement] if failed_statement else [])

    run_plan(BuildPlan(statements=STATEMENTS, cleanup_statements=CLEANUP_STATEMENTS), adapter)

    assert executed_plan_statements(adapter)[-len(CLEANUP_STATEMENTS) :] == CLEANUP_STATEMENTS


def test_a_failed_cleanup_runs_the_next_cleanup_and_keeps_the_node_a_success() -> None:
    adapter = RecordingAdapter(fail_on=[CLEANUP_STATEMENTS[0]])

    with capture_logs() as logs:
        result = run_plan(
            BuildPlan(statements=STATEMENTS, cleanup_statements=CLEANUP_STATEMENTS), adapter
        )

    assert executed_plan_statements(adapter) == STATEMENTS + CLEANUP_STATEMENTS
    assert result.status == RunStatus.SUCCESS
    assert [log["event"] for log in logs].count("run.node.cleanup_failure") == 1


def test_the_row_count_comes_from_the_last_statement_and_not_from_the_cleanup() -> None:
    adapter = RecordingAdapter(select_row_count=7, write_row_count=42)

    result = run_plan(
        BuildPlan(statements=STATEMENTS, cleanup_statements=CLEANUP_STATEMENTS), adapter
    )

    assert result.row_count == 7


@pytest.mark.parametrize("failed_text", ["CREATE OR REPLACE TABLE", "MERGE INTO"])
def test_a_staged_upsert_drops_the_merge_source_after_a_failure(failed_text: str) -> None:
    trouve = Trouve(
        sql="select 1 as id, 'a' as name",
        columns=[
            Column(name="id", type=ColumnType.INTEGER),
            Column(name="name", type=ColumnType.STRING),
        ],
        run_config=RunConfig(
            run_mode=RunMode.INCREMENTAL,
            incremental_mode=IncrementalMode.UPSERT,
            primary_key_columns=["id"],
        ),
    )
    compiled = make_compiled_trouve(PHYSICAL_ADDRESS).compiled
    assert compiled is not None
    trouve.compiled = compiled.model_copy(update={"resolved_sql": trouve.sql})
    dag = build_dag([trouve])
    # The clone statement also holds CREATE OR REPLACE TABLE, thus the failure
    # text names the merge source for that case.
    fail_on = f"{failed_text} {PHYSICAL_ADDRESS}__clair_merge" if failed_text.startswith("CREATE") else failed_text
    adapter = RecordingAdapter(fail_on=[fail_on])

    (result,) = run_project(
        dag,
        get_executable_nodes(dag),
        adapter,
        run_mode=RunMode.INCREMENTAL,
        run_id="test",
        after_node_success=lambda node_name, address, adapter: True,
        use_staging=True,
    )

    assert result.status == RunStatus.FAILURE
    assert adapter.record.statements[-1].endswith(
        f"DROP TABLE IF EXISTS {PHYSICAL_ADDRESS}__clair_merge_test"
    )


EXAMPLE_3 = Path(__file__).parents[2] / "examples" / "projects" / "example_3"


def test_the_example_override_compiles_its_window_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """examples/projects/example_3 holds the subclass that the build_sql() guide shows."""
    # The dev routing of the example reads the name of the developer.
    monkeypatch.setenv("CLAIR_USER", "tester")
    project_root = tmp_path / "example_3"
    shutil.copytree(EXAMPLE_3, project_root)

    output = clair.compile(project_root, run_mode=RunMode.INCREMENTAL)

    node = output.node("example_3_database.derived.daily_revenue")
    assert node is not None
    window_rows = f"{node.addresses.physical}__window_{output.run_id}"
    # A statement can start with a comment line, and a comment can stand alone.
    without_comments = [
        "\n".join(line for line in sql.splitlines() if not line.startswith("--"))
        for sql in node.sql
    ]
    plan_statements = [sql for sql in without_comments if sql]
    assert plan_statements[0].startswith(f"CREATE OR REPLACE TABLE {node.addresses.staging} CLONE")
    assert plan_statements[1].startswith(f"CREATE OR REPLACE TABLE {window_rows} AS")
    assert plan_statements[2].startswith(f"DELETE FROM {node.addresses.staging}")
    assert plan_statements[3].startswith(f"INSERT INTO {node.addresses.staging}")
    assert plan_statements[4] == f"DROP TABLE IF EXISTS {window_rows}"
