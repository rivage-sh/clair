"""The runner executes the plan of a node: each phase in order, then the cleanup.

The runner stops at the first statement of prepare, build or configure that
fails, and it executes no statement after it. The cleanup runs last, when the
statements pass, when one fails, and after a cancel. A staged run calls
should_promote() with the test results, and promote and retire run only in a
staged run. A subclass that overrides a phase gets the same treatment as the
default plan.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from structlog.testing import capture_logs

import clair
from clair.adapters.objects import StatementStatus
from clair.core.dag import build_dag, get_executable_nodes
from clair.core.runner import RunCancellation, RunFailure, RunResult, RunSuccess, RunSummary, run_project
from clair.phase import Phase
from clair.trouves.column import Column, ColumnType
from clair.trouves.plan import NodePlan, PlanContext, TestQuery, TestResult
from clair.trouves.run_config import DeleteInsert, Incremental, RunMode, Upsert
from clair.trouves.trouve import Table
from tests.helpers import TEST_RUN_ID, RecordingAdapter, compile_trouve

PHYSICAL_ADDRESS = "db.s.planned"
STAGING_ADDRESS = f"{PHYSICAL_ADDRESS}__clair_{TEST_RUN_ID}"

# Each phase holds statements that name the phase, thus a test reads the order
# of the phases from the record of the adapter. RecordingAdapter gives a SELECT
# the select row count, and any other statement the write row count.
PLAN = NodePlan(
    prepare=["select 'prepare'"],
    build=["select 'build one'", "select 'build two'"],
    configure=["select 'configure'"],
    promote=["select 'promote'"],
    document=["select 'document'"],
    retire=["select 'retire'"],
    cleanup=["drop table if exists db.s.cleanup_one", "drop table if exists db.s.cleanup_two"],
)
WRITE_STATEMENTS = PLAN.write_statements
PLAN_STATEMENTS = [
    *PLAN.write_statements,
    *PLAN.promote,
    *PLAN.document,
    *PLAN.retire,
    *PLAN.cleanup,
]


class PlannedTrouve(Table):
    """A Trouve that gives the plan of the test, as a subclass of an author does."""

    node_plan: NodePlan
    promote_decision: bool = True

    def plan(self, context: PlanContext) -> NodePlan:
        if context.staged:
            return self.node_plan
        return self.node_plan.model_copy(update={"promote": [], "retire": []})

    def should_promote(self, context: PlanContext, test_results: list[TestResult]) -> bool:
        return self.promote_decision


def executed_plan_statements(adapter: RecordingAdapter) -> list[str]:
    """Give the statements of the plan that the adapter executed, without the DDL of the schema."""
    return [sql for sql in adapter.record.statements if sql in PLAN_STATEMENTS]


def run_plan(
    adapter: RecordingAdapter,
    *,
    node_plan: NodePlan = PLAN,
    staged: bool = True,
    promote_decision: bool = True,
    cancellation: RunCancellation | None = None,
    test_queries: list[list[TestQuery]] | None = None,
) -> RunResult:
    """Run one PlannedTrouve, and give its result.

    *test_queries* receives the queries that the runner gives to the test callback.
    """
    trouve = compile_trouve(
        PlannedTrouve(sql="select 1", node_plan=node_plan, promote_decision=promote_decision),
        PHYSICAL_ADDRESS,
    )
    dag = build_dag([trouve])

    def test_node(name: str, queries: list[TestQuery], _adapter: object) -> list[TestResult]:
        if test_queries is not None:
            test_queries.append(queries)
        return []

    (result,) = run_project(
        dag,
        get_executable_nodes(dag),
        adapter,
        run_id=TEST_RUN_ID,
        test_node=test_node if staged else None,
        use_staging=staged,
        cancellation=cancellation,
    )
    return result


def test_a_staged_run_executes_each_phase_in_order() -> None:
    adapter = RecordingAdapter()

    result = run_plan(adapter)

    assert isinstance(result, RunSuccess)
    assert executed_plan_statements(adapter) == PLAN_STATEMENTS


def test_each_statement_of_the_result_names_its_phase() -> None:
    result = run_plan(RecordingAdapter())

    assert isinstance(result, RunSuccess)
    plan_statements = [statement for statement in result.statements if statement.sql in PLAN_STATEMENTS]
    assert [statement.phase for statement in plan_statements] == [
        Phase.PREPARE,
        Phase.BUILD,
        Phase.BUILD,
        Phase.CONFIGURE,
        Phase.PROMOTE,
        Phase.DOCUMENT,
        Phase.RETIRE,
        Phase.CLEANUP,
        Phase.CLEANUP,
    ]


def test_a_statement_that_did_not_run_keeps_its_phase() -> None:
    result = run_plan(RecordingAdapter(fail_on=[PLAN.build[0]]))

    assert isinstance(result, RunFailure)
    assert result.failed_phase == Phase.BUILD
    not_run = [statement for statement in result.statements if statement.status == StatementStatus.NOT_RUN]
    assert [statement.phase for statement in not_run] == [Phase.BUILD, Phase.CONFIGURE]


def test_the_summary_names_the_phase_that_failed() -> None:
    result = run_plan(RecordingAdapter(fail_on=PLAN.promote))

    assert isinstance(result, RunFailure)
    assert result.failed_phase == Phase.PROMOTE
    assert f"{PHYSICAL_ADDRESS} ... FAILED in promote (" in RunSummary.render_node(result, 1, 1)


def test_a_run_without_staging_executes_no_promote_and_no_retire() -> None:
    adapter = RecordingAdapter()

    result = run_plan(adapter, staged=False)

    assert isinstance(result, RunSuccess)
    assert executed_plan_statements(adapter) == [*WRITE_STATEMENTS, *PLAN.document, *PLAN.cleanup]


@pytest.mark.parametrize("failed_index", range(len(WRITE_STATEMENTS)))
def test_no_statement_runs_after_the_write_statement_that_fails(failed_index: int) -> None:
    adapter = RecordingAdapter(fail_on=[WRITE_STATEMENTS[failed_index]])

    result = run_plan(adapter)

    assert executed_plan_statements(adapter) == [
        *WRITE_STATEMENTS[: failed_index + 1],
        *PLAN.cleanup,
    ]
    assert isinstance(result, RunFailure)
    not_run = [statement.sql for statement in result.statements if statement.status == StatementStatus.NOT_RUN]
    assert not_run == WRITE_STATEMENTS[failed_index + 1 :]


@pytest.mark.parametrize("failed_statement", [None, *PLAN_STATEMENTS[:-2]])
def test_the_cleanup_runs_last_after_a_pass_and_after_a_failure(
    failed_statement: str | None,
) -> None:
    adapter = RecordingAdapter(fail_on=[failed_statement] if failed_statement else [])

    run_plan(adapter)

    assert executed_plan_statements(adapter)[-len(PLAN.cleanup) :] == PLAN.cleanup


def test_the_cleanup_runs_after_a_cancel() -> None:
    cancellation = RunCancellation()

    class CancellingAdapter(RecordingAdapter):
        def execute(self, sql: str):  # type: ignore[override]
            statement = super().execute(sql)
            if sql == PLAN.build[0]:
                cancellation.cancel()
            return statement

    adapter = CancellingAdapter()
    cancellation.watch(adapter)
    result = run_plan(adapter, cancellation=cancellation)

    assert isinstance(result, RunFailure)
    assert executed_plan_statements(adapter) == [*PLAN.prepare, PLAN.build[0], *PLAN.cleanup]


def test_should_promote_false_keeps_the_candidate_and_runs_only_the_cleanup() -> None:
    adapter = RecordingAdapter()

    result = run_plan(adapter, promote_decision=False)

    assert isinstance(result, RunFailure)
    assert "should_promote() gave False" in result.error
    assert STAGING_ADDRESS in result.error
    assert executed_plan_statements(adapter) == [*WRITE_STATEMENTS, *PLAN.cleanup]


def test_a_failed_first_promote_statement_keeps_the_candidate() -> None:
    adapter = RecordingAdapter(fail_on=PLAN.promote)

    result = run_plan(adapter)

    assert isinstance(result, RunFailure)
    assert "the promotion failed" in result.error
    assert executed_plan_statements(adapter) == [*WRITE_STATEMENTS, *PLAN.promote, *PLAN.cleanup]


def test_a_failed_retire_keeps_the_node_a_success() -> None:
    adapter = RecordingAdapter(fail_on=PLAN.retire)

    with capture_logs() as logs:
        result = run_plan(adapter)

    assert isinstance(result, RunSuccess)
    assert [log["event"] for log in logs].count("run.node.staging_drop_failed") == 1


def test_a_failed_cleanup_runs_the_next_cleanup_and_keeps_the_node_a_success() -> None:
    adapter = RecordingAdapter(fail_on=[PLAN.cleanup[0]])

    with capture_logs() as logs:
        result = run_plan(adapter)

    assert executed_plan_statements(adapter) == PLAN_STATEMENTS
    assert isinstance(result, RunSuccess)
    assert [log["event"] for log in logs].count("run.node.cleanup_failure") == 1


def test_the_row_count_comes_from_the_last_build_statement() -> None:
    adapter = RecordingAdapter(select_row_count=7, write_row_count=42)

    result = run_plan(adapter, node_plan=PLAN.model_copy(update={"configure": ["alter table x"]}))

    assert isinstance(result, RunSuccess)
    assert result.row_count == 7


def test_the_runner_gives_the_queries_of_the_test_phase_to_the_tests() -> None:
    query = TestQuery(test_index=0, label="custom", sql="select 'my test'")
    test_queries: list[list[TestQuery]] = []

    run_plan(RecordingAdapter(), node_plan=PLAN.model_copy(update={"test": [query]}), test_queries=test_queries)

    assert test_queries == [[query]]


def test_a_plan_that_raises_fails_its_node_only() -> None:
    class BrokenTrouve(Table):
        def plan(self, context: PlanContext) -> NodePlan:
            raise ValueError("no plan today")

    trouve = compile_trouve(BrokenTrouve(sql="select 1"), PHYSICAL_ADDRESS)
    dag = build_dag([trouve])

    (result,) = run_project(dag, get_executable_nodes(dag), RecordingAdapter(), run_id=TEST_RUN_ID)

    assert isinstance(result, RunFailure)
    assert result.error == "BrokenTrouve.plan() raised ValueError: no plan today"


NEW_ROWS_TABLE = f"{PHYSICAL_ADDRESS}__clair_new_rows_{TEST_RUN_ID}"


def incremental_table(strategy: Upsert | DeleteInsert, table_class: type[Table] = Table) -> Table:
    trouve = table_class(
        sql="select 1 as id, 'a' as name",
        columns=[
            Column(name="id", type=ColumnType.INTEGER),
            Column(name="name", type=ColumnType.STRING),
        ],
        run_config=Incremental(strategy=strategy),
    )
    return compile_trouve(trouve, PHYSICAL_ADDRESS)


def run_incremental(trouve: Table, adapter: RecordingAdapter) -> RunResult:
    dag = build_dag([trouve])
    (result,) = run_project(
        dag,
        get_executable_nodes(dag),
        adapter,
        run_mode=RunMode.INCREMENTAL,
        run_id=TEST_RUN_ID,
        test_node=lambda name, queries, adapter: [],
        use_staging=True,
    )
    return result


@pytest.mark.parametrize(
    ("strategy", "failed_text"),
    [
        # The clone statement also holds CREATE OR REPLACE TABLE, thus the
        # failure text names the new rows table for that case.
        (Upsert(primary_key_columns=["id"]), f"CREATE OR REPLACE TABLE {NEW_ROWS_TABLE}"),
        (Upsert(primary_key_columns=["id"]), "MERGE INTO"),
        (DeleteInsert(primary_key_columns=["id"]), f"CREATE OR REPLACE TABLE {NEW_ROWS_TABLE}"),
        (DeleteInsert(primary_key_columns=["id"]), "DELETE FROM"),
        (DeleteInsert(primary_key_columns=["id"]), "INSERT INTO"),
    ],
)
def test_a_staged_incremental_run_drops_the_new_rows_table_after_a_failure(
    strategy: Upsert | DeleteInsert, failed_text: str
) -> None:
    adapter = RecordingAdapter(fail_on=[failed_text])

    result = run_incremental(incremental_table(strategy), adapter)

    assert isinstance(result, RunFailure)
    assert adapter.record.statements[-1].endswith(f"DROP TABLE IF EXISTS {NEW_ROWS_TABLE}")


class MirroredUpsert(Table):
    """The double MERGE of the lifecycle guide: test on the clone, then merge into production."""

    def promote(self, context: PlanContext) -> list[str]:
        if context.effective_mode == RunMode.FULL_REFRESH:
            return super().promote(context)
        return [
            *self.add_column_statements(context.physical_address, context.added_columns),
            self.merge_statement(context.physical_address, self.new_rows_address(context.run_id)),
        ]


def test_a_promote_override_merges_into_production_and_does_not_replace_it() -> None:
    adapter = RecordingAdapter()

    result = run_incremental(incremental_table(Upsert(primary_key_columns=["id"]), MirroredUpsert), adapter)

    assert isinstance(result, RunSuccess)
    merges = adapter.record.containing("MERGE INTO")
    assert [merge.splitlines()[1] for merge in merges] == [
        f"MERGE INTO {STAGING_ADDRESS} AS target",
        f"MERGE INTO {PHYSICAL_ADDRESS} AS target",
    ]
    assert not adapter.record.containing(f"CREATE OR REPLACE TABLE {PHYSICAL_ADDRESS} CLONE")
    # The promotion reads the new rows table, thus the cleanup drops it after the promotion.
    statements = adapter.record.statements
    assert statements.index(merges[1]) < statements.index(f"-- drop the new rows table\nDROP TABLE IF EXISTS {NEW_ROWS_TABLE}")


EXAMPLE_3 = Path(__file__).parents[2] / "examples" / "projects" / "example_3"


def test_the_example_override_compiles_its_window_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """examples/projects/example_3 holds the subclass that the lifecycle guide shows."""
    # The dev routing of the example reads the name of the developer.
    monkeypatch.setenv("CLAIR_USER", "tester")
    project_root = tmp_path / "example_3"
    shutil.copytree(EXAMPLE_3, project_root)

    output = clair.compile(project_root, run_mode=RunMode.INCREMENTAL, env="dev")

    node = output.node("example_3_database.derived.daily_revenue")
    assert node is not None
    window_rows = f"{node.addresses.physical}__window_{output.run_id}"
    # A statement can start with comment lines: the header of its phase.
    plan_statements = [
        "\n".join(line for line in sql.splitlines() if not line.startswith("--"))
        for sql in node.sql
    ]
    assert plan_statements[0].startswith(f"CREATE OR REPLACE TABLE {node.addresses.staging} CLONE")
    assert plan_statements[1].startswith(f"CREATE OR REPLACE TABLE {window_rows} AS")
    assert plan_statements[2].startswith(f"DELETE FROM {node.addresses.staging}")
    assert plan_statements[3].startswith(f"INSERT INTO {node.addresses.staging}")
    assert plan_statements[4] == f"ALTER TABLE {node.addresses.staging} DROP CLUSTERING KEY"
    assert plan_statements[-1] == f"DROP TABLE IF EXISTS {window_rows}"
