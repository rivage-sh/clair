"""The tests of the compiler output for a PandasTable node."""

from __future__ import annotations

import ast
import textwrap
from pathlib import Path

import pytest

import clair
from clair.core.artifacts import write_compile_output
from clair.core.compiler import build_compile_output
from clair.core.dag import build_dag, get_executable_nodes
from clair.core.discovery import discover_project
from clair.phase import Phase
from clair.trouves.trouve import ExecutionType
from clair.trouves.trouve_type import TrouveType

FAKE_RUN_ID = "0" * 32


def _make_pandas_project(tmp_path: Path) -> Path:
    """Make a project with a SOURCE and a PandasTable that has columns."""
    (tmp_path / "mydb" / "source").mkdir(parents=True)
    (tmp_path / "mydb" / "derived").mkdir(parents=True)

    (tmp_path / "mydb" / "source" / "events.py").write_text(textwrap.dedent("""\
        from clair import Source
        trouve = Source()
    """))

    (tmp_path / "mydb" / "derived" / "summary.py").write_text(textwrap.dedent("""\
        import pandas as pd
        from clair import PandasTable, Column, ColumnType
        from mydb.source.events import trouve as source_events

        def summarize(events: pd.DataFrame) -> pd.DataFrame:
            return events

        trouve = PandasTable(
            transform=summarize,
            inputs=[source_events],
            columns=[
                Column(name="event_type", type=ColumnType.STRING),
                Column(name="event_count", type=ColumnType.NUMBER),
            ],
        )
    """))

    return tmp_path


def _make_mixed_project(tmp_path: Path) -> Path:
    """Make a project with a SQL Trouve and a PandasTable."""
    (tmp_path / "mydb" / "source").mkdir(parents=True)
    (tmp_path / "mydb" / "refined").mkdir(parents=True)
    (tmp_path / "mydb" / "derived").mkdir(parents=True)

    (tmp_path / "mydb" / "source" / "events.py").write_text(textwrap.dedent("""\
        from clair import Source
        trouve = Source()
    """))

    (tmp_path / "mydb" / "refined" / "events.py").write_text(textwrap.dedent("""\
        from mydb.source.events import trouve as source_events
        from clair import Table
        trouve = Table(sql=f"SELECT * FROM {source_events}")
    """))

    (tmp_path / "mydb" / "derived" / "summary.py").write_text(textwrap.dedent("""\
        import pandas as pd
        from clair import PandasTable
        from mydb.refined.events import trouve as refined_events

        def summarize(events: pd.DataFrame) -> pd.DataFrame:
            return events

        trouve = PandasTable(transform=summarize, inputs=[refined_events])
    """))

    return tmp_path


def _make_no_columns_project(tmp_path: Path) -> Path:
    """Make a project with a PandasTable that has no columns."""
    (tmp_path / "mydb" / "source").mkdir(parents=True)
    (tmp_path / "mydb" / "derived").mkdir(parents=True)

    (tmp_path / "mydb" / "source" / "events.py").write_text(textwrap.dedent("""\
        from clair import Source
        trouve = Source()
    """))

    (tmp_path / "mydb" / "derived" / "summary.py").write_text(textwrap.dedent("""\
        import pandas as pd
        from clair import PandasTable
        from mydb.source.events import trouve as source_events

        def summarize(events: pd.DataFrame) -> pd.DataFrame:
            return events

        trouve = PandasTable(transform=summarize, inputs=[refined_events])
    """))

    return tmp_path


class TestPandasTableArtifactFile:
    def test_artifact_file_ends_in_py(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        dag = build_dag(discover_project(project))
        selected = get_executable_nodes(dag)
        write_compile_output(tmp_path / "_clairtifacts", build_compile_output(dag, selected, run_id=FAKE_RUN_ID))

        py_file = tmp_path / "_clairtifacts" / FAKE_RUN_ID / "mydb" / "derived" / "summary.py"
        assert py_file.exists()

    def test_one_trouve_gives_one_file(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        dag = build_dag(discover_project(project))
        selected = get_executable_nodes(dag)
        write_compile_output(tmp_path / "_clairtifacts", build_compile_output(dag, selected, run_id=FAKE_RUN_ID))

        sql_file = tmp_path / "_clairtifacts" / FAKE_RUN_ID / "mydb" / "derived" / "summary.sql"
        assert not sql_file.exists()

    def test_no_json_file_for_pandas_table(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        dag = build_dag(discover_project(project))
        selected = get_executable_nodes(dag)
        write_compile_output(tmp_path / "_clairtifacts", build_compile_output(dag, selected, run_id=FAKE_RUN_ID))

        json_file = tmp_path / "_clairtifacts" / FAKE_RUN_ID / "mydb" / "derived" / "summary.json"
        assert not json_file.exists()


class TestPandasTableArtifactContent:
    def _get_artifact_content(self, tmp_path: Path) -> str:
        project = _make_pandas_project(tmp_path)
        dag = build_dag(discover_project(project))
        selected = get_executable_nodes(dag)
        write_compile_output(tmp_path / "_clairtifacts", build_compile_output(dag, selected, run_id=FAKE_RUN_ID))

        py_file = tmp_path / "_clairtifacts" / FAKE_RUN_ID / "mydb" / "derived" / "summary.py"
        return py_file.read_text()

    def test_the_build_comment_names_each_input(self, tmp_path: Path):
        content = self._get_artifact_content(tmp_path)
        assert "#   events  <-  mydb.source.events" in content

    def test_the_function_runs_before_the_promotion(self, tmp_path: Path):
        content = self._get_artifact_content(tmp_path)
        function_line = content.index("def summarize")
        promotion_line = content.index("# CREATE OR REPLACE TABLE mydb.derived.summary CLONE")
        assert content.index("# ── build") < function_line < promotion_line

    def test_contains_function_source(self, tmp_path: Path):
        content = self._get_artifact_content(tmp_path)
        assert "def summarize" in content


class TestPandasTableCompiledNodeInfo:
    def test_compiled_node_type_is_pandas(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        dag = build_dag(discover_project(project))
        selected = get_executable_nodes(dag)
        output = build_compile_output(dag, selected, run_id=FAKE_RUN_ID)

        pandas_node = next(n for n in output.compiled_nodes if n.addresses.matches("mydb.derived.summary"))
        assert pandas_node.execution_type == ExecutionType.PANDAS

    @pytest.mark.parametrize(
        ("use_staging", "expected_phases"),
        [
            (True, [Phase.BUILD, Phase.PROMOTE, Phase.RETIRE]),
            (False, [Phase.BUILD]),
        ],
    )
    def test_compiled_node_shows_each_phase_of_the_plan(
        self, tmp_path: Path, use_staging: bool, expected_phases: list[Phase]
    ):
        project = _make_pandas_project(tmp_path)
        dag = build_dag(discover_project(project))
        selected = get_executable_nodes(dag)
        output = build_compile_output(dag, selected, run_id=FAKE_RUN_ID, use_staging=use_staging)

        pandas_node = next(n for n in output.compiled_nodes if n.addresses.matches("mydb.derived.summary"))
        phase_headers = [phase for phase in Phase for sql in pandas_node.sql if sql.startswith(phase.header)]
        assert phase_headers == expected_phases

    def test_compiled_node_has_dependencies(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        dag = build_dag(discover_project(project))
        selected = get_executable_nodes(dag)
        output = build_compile_output(dag, selected, run_id=FAKE_RUN_ID)

        pandas_node = next(n for n in output.compiled_nodes if n.addresses.matches("mydb.derived.summary"))
        assert "mydb.source.events" in pandas_node.dependencies


class TestSqlTrouveRegression:
    def test_sql_trouve_still_gets_sql_file(self, tmp_path: Path):
        project = _make_mixed_project(tmp_path)
        dag = build_dag(discover_project(project))
        selected = get_executable_nodes(dag)
        write_compile_output(tmp_path / "_clairtifacts", build_compile_output(dag, selected, run_id=FAKE_RUN_ID))

        sql_file = tmp_path / "_clairtifacts" / FAKE_RUN_ID / "mydb" / "refined" / "events.sql"
        assert sql_file.exists()
        assert "source.raw" not in sql_file.read_text() or "mydb.source.events" in sql_file.read_text()

    def test_sql_trouve_node_type_is_table(self, tmp_path: Path):
        project = _make_mixed_project(tmp_path)
        dag = build_dag(discover_project(project))
        selected = get_executable_nodes(dag)
        output = build_compile_output(dag, selected, run_id=FAKE_RUN_ID)

        sql_node = next(n for n in output.compiled_nodes if n.addresses.matches("mydb.refined.events"))
        assert sql_node.type == TrouveType.TABLE

    def test_mixed_project_both_nodes_compiled(self, tmp_path: Path):
        project = _make_mixed_project(tmp_path)
        dag = build_dag(discover_project(project))
        selected = get_executable_nodes(dag)
        output = build_compile_output(dag, selected, run_id=FAKE_RUN_ID)

        node_names = {str(n.addresses.physical) for n in output.compiled_nodes}
        assert "mydb.refined.events" in node_names
        assert "mydb.derived.summary" in node_names


EXAMPLE_4 = Path(__file__).parents[2] / "examples" / "projects" / "example_4"


def test_each_python_artifact_of_a_compile_is_valid_python(tmp_path: Path, monkeypatch):
    # example_4 holds a PandasTable and a SeedTable. The artifact of each one
    # holds its code and its plan as comments, and Python must read the file.
    monkeypatch.setenv("CLAIR_USER", "tester")
    output = clair.compile(EXAMPLE_4, env="dev")
    run_dir = write_compile_output(tmp_path, output)

    python_files = sorted(run_dir.rglob("*.py"))
    assert len(python_files) == 2
    for python_file in python_files:
        ast.parse(python_file.read_text(), filename=str(python_file))
