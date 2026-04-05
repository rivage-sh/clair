"""Tests for compiler output for df_fn Trouve nodes."""

from __future__ import annotations

import textwrap
from pathlib import Path

from clair.core.compiler import write_compile_output
from clair.core.dag import build_dag, get_executable_nodes
from clair.core.discovery import discover_project
from clair.trouves.trouve import ExecutionType

FAKE_RUN_ID = "0" * 32


def _make_pandas_project(tmp_path: Path) -> Path:
    """Build a project with a SOURCE and a df_fn Trouve with columns."""
    (tmp_path / "mydb" / "source").mkdir(parents=True)
    (tmp_path / "mydb" / "derived").mkdir(parents=True)

    (tmp_path / "mydb" / "source" / "events.py").write_text(textwrap.dedent("""\
        from clair import Trouve, TrouveType
        trouve = Trouve(type=TrouveType.SOURCE)
    """))

    (tmp_path / "mydb" / "derived" / "summary.py").write_text(textwrap.dedent("""\
        import pandas as pd
        from clair import Trouve, Column, ColumnType
        from mydb.source.events import trouve as source_events

        def summarize(events: pd.DataFrame = source_events) -> pd.DataFrame:
            return events

        trouve = Trouve(
            df_fn=summarize,
            columns=[
                Column(name="event_type", type=ColumnType.STRING),
                Column(name="event_count", type=ColumnType.NUMBER),
            ],
        )
    """))

    return tmp_path


def _make_mixed_project(tmp_path: Path) -> Path:
    """Build a project with both SQL Trouve and df_fn Trouve."""
    (tmp_path / "mydb" / "source").mkdir(parents=True)
    (tmp_path / "mydb" / "refined").mkdir(parents=True)
    (tmp_path / "mydb" / "derived").mkdir(parents=True)

    (tmp_path / "mydb" / "source" / "events.py").write_text(textwrap.dedent("""\
        from clair import Trouve, TrouveType
        trouve = Trouve(type=TrouveType.SOURCE)
    """))

    (tmp_path / "mydb" / "refined" / "events.py").write_text(textwrap.dedent("""\
        from mydb.source.events import trouve as source_events
        from clair import Trouve, TrouveType
        trouve = Trouve(type=TrouveType.TABLE, sql=f"SELECT * FROM {source_events}")
    """))

    (tmp_path / "mydb" / "derived" / "summary.py").write_text(textwrap.dedent("""\
        import pandas as pd
        from clair import Trouve
        from mydb.refined.events import trouve as refined_events

        def summarize(events: pd.DataFrame = refined_events) -> pd.DataFrame:
            return events

        trouve = Trouve(df_fn=summarize)
    """))

    return tmp_path


def _make_no_columns_project(tmp_path: Path) -> Path:
    """Build a project with a df_fn Trouve that has no columns defined."""
    (tmp_path / "mydb" / "source").mkdir(parents=True)
    (tmp_path / "mydb" / "derived").mkdir(parents=True)

    (tmp_path / "mydb" / "source" / "events.py").write_text(textwrap.dedent("""\
        from clair import Trouve, TrouveType
        trouve = Trouve(type=TrouveType.SOURCE)
    """))

    (tmp_path / "mydb" / "derived" / "summary.py").write_text(textwrap.dedent("""\
        import pandas as pd
        from clair import Trouve
        from mydb.source.events import trouve as source_events

        def summarize(events: pd.DataFrame = source_events) -> pd.DataFrame:
            return events

        trouve = Trouve(df_fn=summarize)
    """))

    return tmp_path


class TestDfFnTrouveArtifactFile:
    def test_artifact_file_ends_in_py(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        dag = build_dag(discover_project(project))
        selected = get_executable_nodes(dag)
        write_compile_output(dag, selected, tmp_path, on_node_compiled=lambda _: None, run_id=FAKE_RUN_ID)

        py_file = tmp_path / "_clairtifacts" / FAKE_RUN_ID / "mydb" / "derived" / "summary.py"
        assert py_file.exists()

    def test_no_sql_file_for_df_fn_trouve(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        dag = build_dag(discover_project(project))
        selected = get_executable_nodes(dag)
        write_compile_output(dag, selected, tmp_path, on_node_compiled=lambda _: None, run_id=FAKE_RUN_ID)

        sql_file = tmp_path / "_clairtifacts" / FAKE_RUN_ID / "mydb" / "derived" / "summary.sql"
        assert not sql_file.exists()

    def test_no_json_file_for_df_fn_trouve(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        dag = build_dag(discover_project(project))
        selected = get_executable_nodes(dag)
        write_compile_output(dag, selected, tmp_path, on_node_compiled=lambda _: None, run_id=FAKE_RUN_ID)

        json_file = tmp_path / "_clairtifacts" / FAKE_RUN_ID / "mydb" / "derived" / "summary.json"
        assert not json_file.exists()


class TestDfFnTrouveArtifactContent:
    def _get_artifact_content(self, tmp_path: Path) -> str:
        project = _make_pandas_project(tmp_path)
        dag = build_dag(discover_project(project))
        selected = get_executable_nodes(dag)
        write_compile_output(dag, selected, tmp_path, on_node_compiled=lambda _: None, run_id=FAKE_RUN_ID)

        py_file = tmp_path / "_clairtifacts" / FAKE_RUN_ID / "mydb" / "derived" / "summary.py"
        return py_file.read_text()

    def test_header_contains_full_name(self, tmp_path: Path):
        content = self._get_artifact_content(tmp_path)
        assert "# clair compiled: mydb.derived.summary" in content

    def test_header_contains_type_pandas(self, tmp_path: Path):
        content = self._get_artifact_content(tmp_path)
        assert "# execution_type: pandas" in content

    def test_header_contains_inputs(self, tmp_path: Path):
        content = self._get_artifact_content(tmp_path)
        assert "# inputs:" in content
        assert "events  ->  mydb.source.events" in content

    def test_contains_function_source(self, tmp_path: Path):
        content = self._get_artifact_content(tmp_path)
        assert "def summarize" in content


class TestDfFnTrouveCompiledNodeInfo:
    def test_compiled_node_type_is_pandas(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        dag = build_dag(discover_project(project))
        selected = get_executable_nodes(dag)
        output = write_compile_output(dag, selected, tmp_path, on_node_compiled=lambda _: None, run_id=FAKE_RUN_ID)

        pandas_node = next(n for n in output.compiled_nodes if n.name == "mydb.derived.summary")
        assert pandas_node.execution_type == ExecutionType.PANDAS

    def test_compiled_node_has_empty_sql(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        dag = build_dag(discover_project(project))
        selected = get_executable_nodes(dag)
        output = write_compile_output(dag, selected, tmp_path, on_node_compiled=lambda _: None, run_id=FAKE_RUN_ID)

        pandas_node = next(n for n in output.compiled_nodes if n.name == "mydb.derived.summary")
        assert pandas_node.sql == []

    def test_compiled_node_has_dependencies(self, tmp_path: Path):
        project = _make_pandas_project(tmp_path)
        dag = build_dag(discover_project(project))
        selected = get_executable_nodes(dag)
        output = write_compile_output(dag, selected, tmp_path, on_node_compiled=lambda _: None, run_id=FAKE_RUN_ID)

        pandas_node = next(n for n in output.compiled_nodes if n.name == "mydb.derived.summary")
        assert "mydb.source.events" in pandas_node.dependencies


class TestSqlTrouveRegression:
    def test_sql_trouve_still_gets_sql_file(self, tmp_path: Path):
        project = _make_mixed_project(tmp_path)
        dag = build_dag(discover_project(project))
        selected = get_executable_nodes(dag)
        write_compile_output(dag, selected, tmp_path, on_node_compiled=lambda _: None, run_id=FAKE_RUN_ID)

        sql_file = tmp_path / "_clairtifacts" / FAKE_RUN_ID / "mydb" / "refined" / "events.sql"
        assert sql_file.exists()
        assert "source.raw" not in sql_file.read_text() or "mydb.source.events" in sql_file.read_text()

    def test_sql_trouve_node_type_is_table(self, tmp_path: Path):
        project = _make_mixed_project(tmp_path)
        dag = build_dag(discover_project(project))
        selected = get_executable_nodes(dag)
        output = write_compile_output(dag, selected, tmp_path, on_node_compiled=lambda _: None, run_id=FAKE_RUN_ID)

        sql_node = next(n for n in output.compiled_nodes if n.name == "mydb.refined.events")
        assert sql_node.type == "TABLE"

    def test_mixed_project_both_nodes_compiled(self, tmp_path: Path):
        project = _make_mixed_project(tmp_path)
        dag = build_dag(discover_project(project))
        selected = get_executable_nodes(dag)
        output = write_compile_output(dag, selected, tmp_path, on_node_compiled=lambda _: None, run_id=FAKE_RUN_ID)

        node_names = {n.name for n in output.compiled_nodes}
        assert "mydb.refined.events" in node_names
        assert "mydb.derived.summary" in node_names
