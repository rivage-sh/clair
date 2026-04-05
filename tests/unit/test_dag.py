"""Tests for DAG construction, topological sort, and cycle detection."""

from __future__ import annotations

from pathlib import Path

import pytest

from clair.core.dag import ClairDag, build_dag, get_executable_nodes, get_execution_order
from clair.core.discovery import discover_project
from clair.exceptions import CyclicDependencyError
from clair.trouves.config import ResolvedConfig
from clair.trouves.trouve import CompiledAttributes, ExecutionType, Trouve, TrouveType


def _make_trouve(
    full_name: str,
    trouve_type: TrouveType = TrouveType.TABLE,
    imports: list[str] | None = None,
) -> Trouve:
    """Create a compiled Trouve directly for testing."""
    sql = "select 1" if trouve_type != TrouveType.SOURCE else ""
    t = Trouve(type=trouve_type, sql=sql)
    t.compiled = CompiledAttributes(
        full_name=full_name,
        logical_name=full_name,
        resolved_sql=sql,
        file_path=Path(f"/fake/{full_name.replace('.', '/')}.py"),
        module_name=full_name,
        imports=imports or [],
        config=ResolvedConfig(),
        execution_type=ExecutionType.SNOWFLAKE,
    )
    return t


class TestBuildDag:
    def test_dag_from_simple_project(self, simple_project: Path):
        dag = build_dag(discover_project(simple_project))

        assert isinstance(dag, ClairDag)
        assert len(dag.nodes) == 2
        assert "source.raw.orders" in dag.nodes
        assert "analytics.revenue.daily_orders" in dag.nodes

    def test_edge_direction(self, simple_project: Path):
        dag = build_dag(discover_project(simple_project))
        assert dag.has_edge("source.raw.orders", "analytics.revenue.daily_orders")

    def test_topological_order(self, simple_project: Path):
        dag = build_dag(discover_project(simple_project))
        order = get_execution_order(dag)

        source_idx = order.index("source.raw.orders")
        table_idx = order.index("analytics.revenue.daily_orders")
        assert source_idx < table_idx

    def test_executable_nodes_excludes_sources(self, simple_project: Path):
        dag = build_dag(discover_project(simple_project))
        executable = get_executable_nodes(dag)

        assert "analytics.revenue.daily_orders" in executable
        assert "source.raw.orders" not in executable


class TestCycleDetection:
    def test_cycle_detected_at_import_time(self, cyclic_project: Path, capsys):
        """Circular imports between Trouve files fail at import time."""
        trouves = discover_project(cyclic_project)
        assert len(trouves) == 0
        captured = capsys.readouterr()
        assert "discovery.load_error" in captured.out

    def test_dag_level_cycle_detection(self):
        """Manually constructed cycle raises CyclicDependencyError."""
        trouves = [
            _make_trouve("db.s.a", imports=["db.s.b"]),
            _make_trouve("db.s.b", imports=["db.s.a"]),
        ]
        with pytest.raises(CyclicDependencyError, match="Cyclic dependency"):
            build_dag(trouves)


class TestDagWithSyntheticData:
    def test_diamond_dependency(self):
        d = _make_trouve("db.s.d", TrouveType.SOURCE)
        b = _make_trouve("db.s.b", imports=["db.s.d"])
        c = _make_trouve("db.s.c", imports=["db.s.d"])
        a = _make_trouve("db.s.a", imports=["db.s.b", "db.s.c"])

        dag = build_dag([a, b, c, d])
        order = get_execution_order(dag)

        assert order.index("db.s.d") < order.index("db.s.b")
        assert order.index("db.s.d") < order.index("db.s.c")
        assert order.index("db.s.b") < order.index("db.s.a")
        assert order.index("db.s.c") < order.index("db.s.a")

    def test_disconnected_components(self):
        s1 = _make_trouve("db.s.s1", TrouveType.SOURCE)
        t1 = _make_trouve("db.s.t1", imports=["db.s.s1"])
        s2 = _make_trouve("db.s.s2", TrouveType.SOURCE)
        t2 = _make_trouve("db.s.t2", imports=["db.s.s2"])

        dag = build_dag([s1, t1, s2, t2])
        assert len(dag.nodes) == 4
        assert set(get_executable_nodes(dag)) == {"db.s.t1", "db.s.t2"}


class TestClairDagValidate:
    def test_validate_passes_on_valid_dag(self):
        dag = ClairDag()
        dag.add_trouve(_make_trouve("db.s.a", TrouveType.SOURCE))
        dag.add_trouve(_make_trouve("db.s.b"))
        dag.add_dependency("db.s.a", "db.s.b")
        dag.validate()

    def test_validate_raises_on_cycle(self):
        dag = ClairDag()
        dag.add_trouve(_make_trouve("db.s.a"))
        dag.add_trouve(_make_trouve("db.s.b"))
        dag.add_edge("db.s.a", "db.s.b")
        dag.add_edge("db.s.b", "db.s.a")

        with pytest.raises(CyclicDependencyError, match="Cyclic dependency"):
            dag.validate()

    def test_validate_raises_when_node_lacks_trouve(self):
        dag = ClairDag()
        dag.add_node("db.s.orphan")

        with pytest.raises(AssertionError, match="missing the 'trouve' attribute"):
            dag.validate()

    def test_validate_raises_when_trouve_is_wrong_type(self):
        dag = ClairDag()
        dag.add_node("db.s.bad", trouve="not a Trouve")

        with pytest.raises(AssertionError, match="expected Trouve"):
            dag.validate()


class TestClairDagGetTrouve:
    def test_returns_trouve(self):
        dag = ClairDag()
        t = _make_trouve("db.s.a")
        dag.add_trouve(t)

        assert dag.get_trouve("db.s.a") is t

    def test_raises_key_error_for_missing_node(self):
        dag = ClairDag()
        with pytest.raises(KeyError, match="not found in the DAG"):
            dag.get_trouve("db.s.nonexistent")


class TestClairDagTrouves:
    def test_returns_all_trouves(self):
        dag = ClairDag()
        ta = _make_trouve("db.s.a", TrouveType.SOURCE)
        tb = _make_trouve("db.s.b")
        dag.add_trouve(ta)
        dag.add_trouve(tb)

        assert len(dag.trouves) == 2
        assert ta in dag.trouves
        assert tb in dag.trouves

    def test_empty_dag_returns_empty_list(self):
        assert ClairDag().trouves == []
