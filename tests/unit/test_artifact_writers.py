"""The tests of the artifact writers. Each writer puts a result object on disk."""

from __future__ import annotations

from pathlib import Path

from clair.adapters.objects import StatementStatus
from clair.core.artifacts import (
    RUN_SUMMARY_FILE_NAME,
    write_compile_output,
    write_run_result,
    write_run_summary,
)
from clair.core.compiler import CompiledNodeInfo, CompileOutput
from clair.core.runner import RunSummary
from clair.phase import Phase
from clair.trouves.trouve import ExecutionType
from clair.trouves.trouve_type import TrouveType
from tests.helpers import (
    addresses_of_text,
    make_run_failure,
    make_run_skip,
    make_run_success,
    make_statement,
    make_test_result,
)

RUN_ID = "0" * 32
ORDERS = "mydb.analytics.orders"


def _orders_file(artifacts_dir: Path, suffix: str = ".sql") -> Path:
    """The file of ORDERS follows its physical address below the run directory."""
    return artifacts_dir / RUN_ID / "mydb" / "analytics" / f"orders{suffix}"


class TestWriteRunResult:
    def test_the_file_holds_each_statement_with_its_query_id_in_order(self, tmp_path: Path):
        result = make_run_success(
            ORDERS,
            statements=[
                make_statement("create table a as select 1", query_id="01-first"),
                make_statement("alter table a swap with b", query_id="01-second"),
            ],
        )

        path = write_run_result(tmp_path, RUN_ID, result)

        assert path == _orders_file(tmp_path)
        text = path.read_text()
        assert text.index("01-first") < text.index("create table a") < text.index("01-second")
        assert text.index("01-second") < text.index("alter table a swap")

    def test_a_failure_keeps_the_error_and_each_statement_that_did_not_run(self, tmp_path: Path):
        result = make_run_failure(
            ORDERS,
            error="line one\nline two",
            statements=[
                make_statement(
                    "select broken",
                    status=StatementStatus.FAILURE,
                    error="SQL compilation error\nat position 7",
                ),
                make_statement("drop table staging", status=StatementStatus.NOT_RUN),
            ],
        )

        path = write_run_result(tmp_path, RUN_ID, result)

        assert path is not None
        text = path.read_text()

        assert "drop table staging" in text
        assert "not_run" in text
        # A message of many lines stays a SQL comment, thus the file still runs.
        for message_line in ("line two", "at position 7"):
            line = next(line for line in text.splitlines() if message_line in line)
            assert line.startswith("--")

    def test_the_file_gives_each_phase_a_section_and_holds_the_tests(self, tmp_path: Path):
        """The record of the run reads as clair compile: a header for each phase, in order."""
        result = make_run_failure(
            ORDERS,
            error="the tests failed",
            statements=[
                make_statement("create table staging as select 1", phase=Phase.BUILD),
                make_statement("drop table new_rows", phase=Phase.CLEANUP),
            ],
            test_results=[
                make_test_result(ORDERS, column_name="id", sql="select id from staging", row_count=3)
            ],
        )

        path = write_run_result(tmp_path, RUN_ID, result)

        assert path is not None
        text = path.read_text()
        assert "-- failed phase: test" in text
        assert (
            text.index(Phase.BUILD.header)
            < text.index("create table staging")
            < text.index(Phase.TEST.header)
            < text.index("-- unique (id), ERROR: 3 failing rows")
            < text.index("select id from staging")
            < text.index(Phase.CLEANUP.header)
            < text.index("drop table new_rows")
        )
        assert "-- statement 3/3" in text

    def test_a_skipped_trouve_writes_no_file(self, tmp_path: Path):
        result = make_run_skip(ORDERS, skipped_by="mydb.analytics.upstream")

        assert write_run_result(tmp_path, RUN_ID, result) is None
        assert not tmp_path.joinpath(RUN_ID).exists()


class TestWriteRunSummary:
    def test_the_file_reads_back_as_the_same_summary(self, tmp_path: Path):
        summary = RunSummary(
            results=[
                make_run_success(
                    ORDERS,
                    statements=[make_statement(query_id="01-first")],
                    test_results=[make_test_result(ORDERS, test_type="not_null", column_name="id")],
                ),
                make_run_failure("mydb.analytics.refunds", error="the build failed"),
                make_run_skip("mydb.analytics.report", skipped_by="mydb.analytics.refunds"),
            ],
            env_name="dev",
            run_id=RUN_ID,
            project_root=tmp_path,
        )

        path = write_run_summary(tmp_path, summary)

        assert path == tmp_path / RUN_ID / RUN_SUMMARY_FILE_NAME
        assert RunSummary.model_validate_json(path.read_text()) == summary


def _compiled_node(
    execution_type: ExecutionType, sql: list[str], python_source: str | None = None
) -> CompiledNodeInfo:
    return CompiledNodeInfo(
        addresses=addresses_of_text(ORDERS),
        type=TrouveType.TABLE,
        execution_type=execution_type,
        dependencies=[],
        sql=sql,
        python_source=python_source,
    )


class TestWriteCompileOutput:
    def _output(self, node: CompiledNodeInfo) -> CompileOutput:
        return CompileOutput(trouve_count=1, source_count=0, compiled_nodes=[node], run_id=RUN_ID)

    def test_a_sql_trouve_gives_a_sql_file_with_each_statement(self, tmp_path: Path):
        node = _compiled_node(ExecutionType.SNOWFLAKE, sql=["create table a", "drop table b"])

        run_dir = write_compile_output(tmp_path, self._output(node))

        assert run_dir == tmp_path / RUN_ID
        text = _orders_file(tmp_path).read_text()
        assert text.index("create table a") < text.index("drop table b")

    def test_a_pandas_trouve_gives_its_source_and_its_statements(self, tmp_path: Path):
        node = _compiled_node(
            ExecutionType.PANDAS, sql=["select 'promote'"], python_source="def build():\n    pass\n"
        )

        write_compile_output(tmp_path, self._output(node))

        assert _orders_file(tmp_path, ".py").read_text() == "def build():\n    pass\n"
        assert _orders_file(tmp_path).read_text() == "select 'promote'\n"
