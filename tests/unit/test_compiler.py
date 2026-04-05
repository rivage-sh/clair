"""Tests for the compiler."""

from __future__ import annotations

from pathlib import Path

from clair.core.compiler import write_compile_output
from clair.core.dag import build_dag, get_executable_nodes
from clair.core.discovery import discover_project

FAKE_RUN_ID = "0" * 32


class TestWriteCompileOutput:
    def test_output_has_correct_counts(self, simple_project: Path, tmp_path: Path):
        dag = build_dag(discover_project(simple_project))
        selected = get_executable_nodes(dag)
        output = write_compile_output(dag, selected, tmp_path, on_node_compiled=lambda _: None, run_id=FAKE_RUN_ID)

        assert output.trouve_count == 1
        assert output.source_count == 1

    def test_compiled_nodes_contain_expected_trouve(self, simple_project: Path, tmp_path: Path):
        dag = build_dag(discover_project(simple_project))
        selected = get_executable_nodes(dag)
        output = write_compile_output(dag, selected, tmp_path, on_node_compiled=lambda _: None, run_id=FAKE_RUN_ID)

        node_names = [n.name for n in output.compiled_nodes]
        assert "analytics.revenue.daily_orders" in node_names

    def test_compiled_node_has_sql_referencing_source(self, simple_project: Path, tmp_path: Path):
        dag = build_dag(discover_project(simple_project))
        selected = get_executable_nodes(dag)
        output = write_compile_output(dag, selected, tmp_path, on_node_compiled=lambda _: None, run_id=FAKE_RUN_ID)

        daily_orders_node = next(
            n for n in output.compiled_nodes if n.name == "analytics.revenue.daily_orders"
        )
        assert any("source.raw.orders" in s for s in daily_orders_node.sql)

    def test_writes_sql_file_to_clairtifacts(self, simple_project: Path, tmp_path: Path):
        dag = build_dag(discover_project(simple_project))
        selected = get_executable_nodes(dag)
        write_compile_output(dag, selected, tmp_path, on_node_compiled=lambda _: None, run_id=FAKE_RUN_ID)

        sql_file = tmp_path / "_clairtifacts" / FAKE_RUN_ID / "analytics" / "revenue" / "daily_orders.sql"
        assert sql_file.exists()
        assert "source.raw.orders" in sql_file.read_text()

    def test_artifacts_dir_set_correctly(self, simple_project: Path, tmp_path: Path):
        dag = build_dag(discover_project(simple_project))
        selected = get_executable_nodes(dag)
        output = write_compile_output(dag, selected, tmp_path, on_node_compiled=lambda _: None, run_id=FAKE_RUN_ID)

        assert output.artifacts_dir == tmp_path / "_clairtifacts" / FAKE_RUN_ID

    def test_empty_selection_produces_no_compiled_nodes(self, simple_project: Path, tmp_path: Path):
        dag = build_dag(discover_project(simple_project))
        output = write_compile_output(dag, [], tmp_path, on_node_compiled=lambda _: None, run_id=FAKE_RUN_ID)

        assert output.compiled_nodes == []
        artifacts = tmp_path / "_clairtifacts" / FAKE_RUN_ID
        assert not any(artifacts.rglob("*.sql")) if artifacts.exists() else True

    def test_compiled_node_has_type_field(self, simple_project: Path, tmp_path: Path):
        dag = build_dag(discover_project(simple_project))
        selected = get_executable_nodes(dag)
        output = write_compile_output(dag, selected, tmp_path, on_node_compiled=lambda _: None, run_id=FAKE_RUN_ID)

        daily_orders_node = next(
            n for n in output.compiled_nodes if n.name == "analytics.revenue.daily_orders"
        )
        assert daily_orders_node.type == "TABLE"

    def test_compiled_node_has_dependencies(self, simple_project: Path, tmp_path: Path):
        dag = build_dag(discover_project(simple_project))
        selected = get_executable_nodes(dag)
        output = write_compile_output(dag, selected, tmp_path, on_node_compiled=lambda _: None, run_id=FAKE_RUN_ID)

        daily_orders_node = next(
            n for n in output.compiled_nodes if n.name == "analytics.revenue.daily_orders"
        )
        assert "source.raw.orders" in daily_orders_node.dependencies

    def test_empty_selection_preserves_dag_counts(self, simple_project: Path, tmp_path: Path):
        dag = build_dag(discover_project(simple_project))
        output = write_compile_output(dag, [], tmp_path, on_node_compiled=lambda _: None, run_id=FAKE_RUN_ID)

        # Even with no selection, the DAG-wide counts reflect the full DAG
        assert output.trouve_count == 1
        assert output.source_count == 1

    def test_output_is_compile_output_instance(self, simple_project: Path, tmp_path: Path):
        from clair.core.compiler import CompileOutput

        dag = build_dag(discover_project(simple_project))
        selected = get_executable_nodes(dag)
        output = write_compile_output(dag, selected, tmp_path, on_node_compiled=lambda _: None, run_id=FAKE_RUN_ID)

        assert isinstance(output, CompileOutput)
