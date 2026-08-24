"""The tests of the DAG render code, which draws a ClairDag as a tree."""

from __future__ import annotations

from pathlib import Path

import pytest

from clair.core.dag import ClairDag
from clair.core.dag_render import (
    DagRenderOutput,
    _compute_depths,
    _compute_visible_nodes,
    _format_header,
    render_dag,
)
from clair.trouves.config import ResolvedConfig
from clair.trouves.trouve import CompiledAttributes, ExecutionType, Trouve, TrouveType

# ---------------------------------------------------------------------------
# The helper functions.
# ---------------------------------------------------------------------------


def _make_trouve(
    physical_name: str,
    trouve_type: TrouveType = TrouveType.TABLE,
    imports: list[str] | None = None,
) -> Trouve:
    """Make a small compiled Trouve for a render test."""
    sql = "select 1" if trouve_type != TrouveType.SOURCE else ""
    t = Trouve(type=trouve_type, sql=sql)
    t.compiled = CompiledAttributes(
        physical_name=physical_name,
        logical_name=physical_name,
        resolved_sql=sql,
        file_path=Path(f"/fake/{physical_name.replace('.', '/')}.py"),
        module_name=physical_name,
        imports=imports or [],
        config=ResolvedConfig(),
        execution_type=ExecutionType.SNOWFLAKE,
    )
    return t


def _build_dag(
    nodes: list[tuple[str, TrouveType]],
    edges: list[tuple[str, str]] | None = None,
) -> ClairDag:
    """Make a ClairDag from the (physical_name, type) pairs and the (src, dst) edges."""
    dag = ClairDag()
    for physical_name, ttype in nodes:
        dag.add_trouve(_make_trouve(physical_name, ttype))
    for src, dst in edges or []:
        dag.add_dependency(src, dst)
    return dag


def _node_name(line: str) -> str:
    """Read the node physical_name from one line of the output.

    The function removes each tree character and each other prefix. It gives the
    full name alone.
    """
    for i, ch in enumerate(line):
        if ch.isalpha():
            return line[i:].split("  [")[0].rstrip()
    return ""


# ---------------------------------------------------------------------------
# The basic output
# ---------------------------------------------------------------------------


class TestEmptyDag:
    def test_empty_dag_has_zero_counts(self):
        dag = _build_dag([])
        output = render_dag(dag)
        assert output.model_count == 0
        assert output.source_count == 0

    def test_empty_dag_has_no_visible_nodes(self):
        dag = _build_dag([])
        output = render_dag(dag)
        assert output.visible_nodes == []


class TestSingleNode:
    def test_single_source_is_visible(self):
        dag = _build_dag([("source.raw.orders", TrouveType.SOURCE)])
        output = render_dag(dag)
        assert "source.raw.orders" in output.visible_nodes

    def test_single_source_counts(self):
        dag = _build_dag([("source.raw.orders", TrouveType.SOURCE)])
        output = render_dag(dag)
        assert output.model_count == 0
        assert output.source_count == 1

    def test_single_source_no_match_is_false(self):
        dag = _build_dag([("source.raw.orders", TrouveType.SOURCE)])
        output = render_dag(dag)
        assert output.no_match is False

    def test_single_source_selector_is_none(self):
        dag = _build_dag([("source.raw.orders", TrouveType.SOURCE)])
        output = render_dag(dag)
        assert output.selector is None

    def test_single_table_is_visible(self):
        dag = _build_dag([("db.schema.my_table", TrouveType.TABLE)])
        output = render_dag(dag)
        assert "db.schema.my_table" in output.visible_nodes

    def test_single_table_counts(self):
        dag = _build_dag([("db.schema.my_table", TrouveType.TABLE)])
        output = render_dag(dag)
        assert output.model_count == 1
        assert output.source_count == 0


class TestLinearChain:
    @pytest.fixture
    def linear_dag(self) -> ClairDag:
        return _build_dag(
            [
                ("source.raw.orders", TrouveType.SOURCE),
                ("analytics.staging.clean", TrouveType.TABLE),
                ("analytics.revenue.daily", TrouveType.TABLE),
            ],
            [
                ("source.raw.orders", "analytics.staging.clean"),
                ("analytics.staging.clean", "analytics.revenue.daily"),
            ],
        )

    def test_linear_chain_all_nodes_visible(self, linear_dag: ClairDag):
        output = render_dag(linear_dag)
        assert "source.raw.orders" in output.visible_nodes
        assert "analytics.staging.clean" in output.visible_nodes
        assert "analytics.revenue.daily" in output.visible_nodes

    def test_linear_chain_renders_in_topological_order(self, linear_dag: ClairDag):
        output = render_dag(linear_dag)
        rendered = output.render()
        lines = [ln for ln in rendered.split("\n") if ln.strip() and "===" not in ln]
        names = [_node_name(ln) for ln in lines]
        assert names == [
            "source.raw.orders",
            "analytics.staging.clean",
            "analytics.revenue.daily",
        ]

    def test_linear_chain_depth_indentation(self, linear_dag: ClairDag):
        output = render_dag(linear_dag)
        rendered = output.render()
        lines = [ln for ln in rendered.split("\n") if ln.strip() and "===" not in ln]
        # Depth 0: the root, with no connector.
        assert lines[0].startswith("source.raw.orders")
        # Depth 1: a child, with a connector.
        assert lines[1].startswith("└── ") or lines[1].startswith("├── ")
        # Depth 2: a child, with an indent of 4 characters and a connector.
        assert lines[2].startswith("    └── ") or lines[2].startswith("    ├── ")


# ---------------------------------------------------------------------------
# The depth and the indent
# ---------------------------------------------------------------------------


class TestDepthAndIndentation:
    def test_two_level_dag_depths(self):
        dag = _build_dag(
            [
                ("db.s.root", TrouveType.SOURCE),
                ("db.s.child", TrouveType.TABLE),
            ],
            [("db.s.root", "db.s.child")],
        )
        output = render_dag(dag)
        rendered = output.render()
        lines = [ln for ln in rendered.split("\n") if ln.strip() and "===" not in ln]
        assert lines[0].startswith("db.s.root")
        assert lines[1].startswith("└── db.s.child") or lines[1].startswith("├── db.s.child")

    def test_three_level_dag_depths(self):
        dag = _build_dag(
            [
                ("db.s.a", TrouveType.SOURCE),
                ("db.s.b", TrouveType.TABLE),
                ("db.s.c", TrouveType.TABLE),
            ],
            [("db.s.a", "db.s.b"), ("db.s.b", "db.s.c")],
        )
        output = render_dag(dag)
        rendered = output.render()
        lines = [ln for ln in rendered.split("\n") if ln.strip() and "===" not in ln]
        assert lines[0].startswith("db.s.a")
        assert lines[1].startswith("└── db.s.b") or lines[1].startswith("├── db.s.b")
        assert lines[2].startswith("    └── db.s.c") or lines[2].startswith("    ├── db.s.c")

    def test_diamond_node_appears_at_longest_path_depth(self):
        """In a diamond A->B->D and A->C->D, node D is at depth 2."""
        dag = _build_dag(
            [
                ("db.s.a", TrouveType.SOURCE),
                ("db.s.b", TrouveType.TABLE),
                ("db.s.c", TrouveType.TABLE),
                ("db.s.d", TrouveType.TABLE),
            ],
            [
                ("db.s.a", "db.s.b"),
                ("db.s.a", "db.s.c"),
                ("db.s.b", "db.s.d"),
                ("db.s.c", "db.s.d"),
            ],
        )
        output = render_dag(dag)
        rendered = output.render()
        for line in rendered.split("\n"):
            if "db.s.d" in line and "(^)" not in line:
                assert line.startswith(
                    ("│   └── db.s.d", "    └── db.s.d")
                ), f"db.s.d should be at depth 2, got: {line!r}"
                break
        else:
            pytest.fail("db.s.d not found in output")


# ---------------------------------------------------------------------------
# A diamond, where two parents give one child
# ---------------------------------------------------------------------------


class TestDiamondFanIn:
    def test_node_with_two_parents_real_entry_appears_once(self):
        """The node merged occurs one time as a complete entry and one time as a (^) reference."""
        dag = _build_dag(
            [
                ("db.s.src1", TrouveType.SOURCE),
                ("db.s.src2", TrouveType.SOURCE),
                ("db.s.merged", TrouveType.TABLE),
            ],
            [
                ("db.s.src1", "db.s.merged"),
                ("db.s.src2", "db.s.merged"),
            ],
        )
        output = render_dag(dag)
        rendered = output.render()
        real_count = sum(
            1 for ln in rendered.split("\n") if "db.s.merged" in ln and "(^)" not in ln
        )
        back_ref_count = sum(
            1 for ln in rendered.split("\n") if "db.s.merged" in ln and "(^)" in ln
        )
        assert real_count == 1
        assert back_ref_count == 1

    def test_fan_in_direct_child_of_source(self):
        """The node merged is at depth 1, below its two source parents."""
        dag = _build_dag(
            [
                ("db.s.src1", TrouveType.SOURCE),
                ("db.s.src2", TrouveType.SOURCE),
                ("db.s.merged", TrouveType.TABLE),
            ],
            [
                ("db.s.src1", "db.s.merged"),
                ("db.s.src2", "db.s.merged"),
            ],
        )
        output = render_dag(dag)
        rendered = output.render()
        for line in rendered.split("\n"):
            if "db.s.merged" in line and "(^)" not in line:
                assert line.startswith(("└── db.s.merged", "├── db.s.merged"))
                break
        else:
            pytest.fail("db.s.merged not found in output")


# ---------------------------------------------------------------------------
# More than one root
# ---------------------------------------------------------------------------


class TestMultipleRoots:
    def test_two_independent_roots_both_visible(self):
        dag = _build_dag(
            [
                ("db.s.alpha", TrouveType.SOURCE),
                ("db.s.beta", TrouveType.SOURCE),
            ],
        )
        output = render_dag(dag)
        assert "db.s.alpha" in output.visible_nodes
        assert "db.s.beta" in output.visible_nodes

    def test_two_roots_separated_by_blank_line(self):
        dag = _build_dag(
            [
                ("db.s.alpha", TrouveType.SOURCE),
                ("db.s.beta", TrouveType.SOURCE),
            ],
        )
        output = render_dag(dag)
        rendered = output.render()
        lines = rendered.split("\n")
        root_indices = [
            i
            for i, ln in enumerate(lines)
            if ln.strip() and "===" not in ln and "db.s." in ln
        ]
        assert len(root_indices) == 2
        # An empty line separates the two root nodes.
        assert root_indices[1] - root_indices[0] == 2
        assert lines[root_indices[0] + 1].strip() == ""


# ---------------------------------------------------------------------------
# The selection filter
# ---------------------------------------------------------------------------


class TestSelectedFiltering:
    @pytest.fixture
    def chain_dag(self) -> ClairDag:
        """A chain: SOURCE -> staging -> daily_orders."""
        return _build_dag(
            [
                ("source.raw.orders", TrouveType.SOURCE),
                ("analytics.staging.clean_orders", TrouveType.TABLE),
                ("analytics.reporting.daily_orders", TrouveType.TABLE),
            ],
            [
                ("source.raw.orders", "analytics.staging.clean_orders"),
                ("analytics.staging.clean_orders", "analytics.reporting.daily_orders"),
            ],
        )

    @pytest.fixture
    def chain_with_unrelated(self) -> ClairDag:
        """The chain, and one more branch that is separate."""
        return _build_dag(
            [
                ("source.raw.orders", TrouveType.SOURCE),
                ("analytics.staging.clean_orders", TrouveType.TABLE),
                ("analytics.reporting.daily_orders", TrouveType.TABLE),
                ("source.raw.events", TrouveType.SOURCE),
                ("analytics.events.counts", TrouveType.TABLE),
            ],
            [
                ("source.raw.orders", "analytics.staging.clean_orders"),
                ("analytics.staging.clean_orders", "analytics.reporting.daily_orders"),
                ("source.raw.events", "analytics.events.counts"),
            ],
        )

    def test_selected_none_renders_all_nodes(self, chain_dag: ClairDag):
        output = render_dag(chain_dag, selected=None)
        assert "source.raw.orders" in output.visible_nodes
        assert "analytics.staging.clean_orders" in output.visible_nodes
        assert "analytics.reporting.daily_orders" in output.visible_nodes

    def test_selected_renders_node_and_ancestors(self, chain_dag: ClairDag):
        output = render_dag(
            chain_dag, selected=["analytics.reporting.daily_orders"]
        )
        assert "source.raw.orders" in output.visible_nodes
        assert "analytics.staging.clean_orders" in output.visible_nodes
        assert "analytics.reporting.daily_orders" in output.visible_nodes

    def test_selected_node_is_in_matched_nodes(self, chain_dag: ClairDag):
        output = render_dag(
            chain_dag, selected=["analytics.reporting.daily_orders"]
        )
        assert "analytics.reporting.daily_orders" in output.matched_nodes

    def test_ancestor_nodes_not_in_matched_nodes(self, chain_dag: ClairDag):
        output = render_dag(
            chain_dag, selected=["analytics.reporting.daily_orders"]
        )
        assert "source.raw.orders" not in output.matched_nodes
        assert "analytics.staging.clean_orders" not in output.matched_nodes

    def test_unrelated_nodes_excluded_when_selected(
        self, chain_with_unrelated: ClairDag
    ):
        output = render_dag(
            chain_with_unrelated,
            selected=["analytics.reporting.daily_orders"],
        )
        assert "source.raw.events" not in output.visible_nodes
        assert "analytics.events.counts" not in output.visible_nodes

    def test_selected_empty_list_renders_all(self, chain_dag: ClairDag):
        """An empty selected=[] means no selection. The tree shows each node."""
        output = render_dag(chain_dag, selected=[])
        assert len(output.visible_nodes) > 0

    def test_selected_no_match_returns_no_match(self, chain_dag: ClairDag):
        output = render_dag(chain_dag, selected=["nonexistent.*"])
        assert output.no_match is True
        assert output.selector == "nonexistent.*"

    def test_selected_glob_pattern(self, chain_with_unrelated: ClairDag):
        output = render_dag(
            chain_with_unrelated, selected=["analytics.reporting.*"]
        )
        assert "analytics.reporting.daily_orders" in output.visible_nodes
        assert "source.raw.orders" in output.visible_nodes
        assert "analytics.events.counts" not in output.visible_nodes

    def test_filtered_header_includes_selector(self, chain_dag: ClairDag):
        output = render_dag(
            chain_dag, selected=["analytics.reporting.daily_orders"]
        )
        assert output.selector == "analytics.reporting.daily_orders"


# ---------------------------------------------------------------------------
# The node data and the type tags
# ---------------------------------------------------------------------------


class TestNodeMetadata:
    def test_source_only_dag_has_zero_models(self):
        dag = _build_dag([("db.s.src", TrouveType.SOURCE)])
        output = render_dag(dag)
        assert output.model_count == 0
        assert output.source_count == 1

    def test_table_only_dag_has_one_model(self):
        dag = _build_dag([("db.s.tbl", TrouveType.TABLE)])
        output = render_dag(dag)
        assert output.model_count == 1
        assert output.source_count == 0

    def test_view_only_dag_has_one_model(self):
        dag = _build_dag([("db.s.vw", TrouveType.VIEW)])
        output = render_dag(dag)
        assert output.model_count == 1
        assert output.source_count == 0

    def test_all_three_types_in_one_dag(self):
        dag = _build_dag(
            [
                ("db.s.src", TrouveType.SOURCE),
                ("db.s.tbl", TrouveType.TABLE),
                ("db.s.vw", TrouveType.VIEW),
            ],
        )
        output = render_dag(dag)
        assert output.model_count == 2  # One TABLE and one VIEW.
        assert output.source_count == 1

    def test_all_three_types_visible(self):
        dag = _build_dag(
            [
                ("db.s.src", TrouveType.SOURCE),
                ("db.s.tbl", TrouveType.TABLE),
                ("db.s.vw", TrouveType.VIEW),
            ],
        )
        output = render_dag(dag)
        assert "db.s.src" in output.visible_nodes
        assert "db.s.tbl" in output.visible_nodes
        assert "db.s.vw" in output.visible_nodes


# ---------------------------------------------------------------------------
# The output format
# ---------------------------------------------------------------------------


class TestOutputFormat:
    def test_output_is_dag_render_output(self):
        dag = _build_dag([("db.s.a", TrouveType.SOURCE)])
        output = render_dag(dag)
        assert isinstance(output, DagRenderOutput)

    def test_render_is_nonempty_string(self):
        dag = _build_dag([("db.s.a", TrouveType.SOURCE)])
        output = render_dag(dag)
        rendered = output.render()
        assert isinstance(rendered, str)
        assert len(rendered) > 0

    def test_each_node_is_visible(self):
        dag = _build_dag(
            [
                ("db.s.alpha", TrouveType.SOURCE),
                ("db.s.beta", TrouveType.TABLE),
            ],
            [("db.s.alpha", "db.s.beta")],
        )
        output = render_dag(dag)
        assert "db.s.alpha" in output.visible_nodes
        assert "db.s.beta" in output.visible_nodes

    def test_header_counts_models_and_sources(self):
        dag = _build_dag(
            [
                ("db.s.src1", TrouveType.SOURCE),
                ("db.s.src2", TrouveType.SOURCE),
                ("db.s.tbl1", TrouveType.TABLE),
                ("db.s.tbl2", TrouveType.TABLE),
                ("db.s.vw1", TrouveType.VIEW),
            ],
        )
        output = render_dag(dag)
        # 3 models: 2 TABLE and 1 VIEW. Also 2 sources.
        assert output.model_count == 3
        assert output.source_count == 2

    def test_singular_model_and_source_counts(self):
        dag = _build_dag(
            [
                ("db.s.src", TrouveType.SOURCE),
                ("db.s.tbl", TrouveType.TABLE),
            ],
            [("db.s.src", "db.s.tbl")],
        )
        output = render_dag(dag)
        assert output.model_count == 1
        assert output.source_count == 1

    def test_alphabetical_ordering_within_depth(self):
        """The nodes at one depth are in alphabetic order."""
        dag = _build_dag(
            [
                ("db.s.root", TrouveType.SOURCE),
                ("db.s.zebra", TrouveType.TABLE),
                ("db.s.apple", TrouveType.TABLE),
                ("db.s.mango", TrouveType.TABLE),
            ],
            [
                ("db.s.root", "db.s.zebra"),
                ("db.s.root", "db.s.apple"),
                ("db.s.root", "db.s.mango"),
            ],
        )
        output = render_dag(dag)
        rendered = output.render()
        lines = [
            ln for ln in rendered.split("\n")
            if ln.strip() and "===" not in ln and "db.s.root" not in ln
        ]
        names = [_node_name(ln) for ln in lines]
        assert names == ["db.s.apple", "db.s.mango", "db.s.zebra"]


# ---------------------------------------------------------------------------
# The header function
# ---------------------------------------------------------------------------


class TestFormatHeader:
    def test_unfiltered_header(self):
        result = _format_header(3, 2, None)
        assert result == "=== Clair DAG: 3 models, 2 sources ==="

    def test_filtered_header(self):
        result = _format_header(1, 1, "analytics.*")
        assert result == "=== Clair DAG (filtered: analytics.*): 1 model, 1 source ==="

    def test_zero_counts(self):
        result = _format_header(0, 0, None)
        assert result == "=== Clair DAG: 0 models, 0 sources ==="

    def test_singular_forms(self):
        result = _format_header(1, 1, None)
        assert "1 model," in result
        assert "1 source" in result


# ---------------------------------------------------------------------------
# The _compute_depths function
# ---------------------------------------------------------------------------


class TestComputeDepths:
    def test_single_node_depth_zero(self):
        dag = _build_dag([("db.s.a", TrouveType.SOURCE)])
        depths = _compute_depths(dag, {"db.s.a"})
        assert depths == {"db.s.a": 0}

    def test_linear_chain_depths(self):
        dag = _build_dag(
            [
                ("db.s.a", TrouveType.SOURCE),
                ("db.s.b", TrouveType.TABLE),
                ("db.s.c", TrouveType.TABLE),
            ],
            [("db.s.a", "db.s.b"), ("db.s.b", "db.s.c")],
        )
        depths = _compute_depths(dag, {"db.s.a", "db.s.b", "db.s.c"})
        assert depths == {"db.s.a": 0, "db.s.b": 1, "db.s.c": 2}

    def test_diamond_longest_path(self):
        dag = _build_dag(
            [
                ("db.s.a", TrouveType.SOURCE),
                ("db.s.b", TrouveType.TABLE),
                ("db.s.c", TrouveType.TABLE),
                ("db.s.d", TrouveType.TABLE),
            ],
            [
                ("db.s.a", "db.s.b"),
                ("db.s.a", "db.s.c"),
                ("db.s.b", "db.s.d"),
                ("db.s.c", "db.s.d"),
            ],
        )
        depths = _compute_depths(
            dag, {"db.s.a", "db.s.b", "db.s.c", "db.s.d"}
        )
        assert depths["db.s.d"] == 2

    def test_empty_visible_set(self):
        dag = _build_dag([("db.s.a", TrouveType.SOURCE)])
        depths = _compute_depths(dag, set())
        assert depths == {}


class TestComputeVisibleNodes:
    def test_selected_none_returns_all_nodes(self):
        dag = _build_dag(
            [("db.s.a", TrouveType.SOURCE), ("db.s.b", TrouveType.TABLE)],
            [("db.s.a", "db.s.b")],
        )
        visible, matched = _compute_visible_nodes(dag, None)
        assert visible == {"db.s.a", "db.s.b"}
        assert matched == set()

    def test_selected_returns_match_and_ancestors(self):
        dag = _build_dag(
            [
                ("db.s.a", TrouveType.SOURCE),
                ("db.s.b", TrouveType.TABLE),
                ("db.s.c", TrouveType.TABLE),
            ],
            [("db.s.a", "db.s.b"), ("db.s.b", "db.s.c")],
        )
        visible, matched = _compute_visible_nodes(dag, ["db.s.c"])
        assert visible == {"db.s.a", "db.s.b", "db.s.c"}
        assert matched == {"db.s.c"}

    def test_selected_excludes_unrelated(self):
        dag = _build_dag(
            [
                ("db.s.a", TrouveType.SOURCE),
                ("db.s.b", TrouveType.TABLE),
                ("db.s.unrelated", TrouveType.TABLE),
            ],
            [("db.s.a", "db.s.b")],
        )
        visible, _matched = _compute_visible_nodes(dag, ["db.s.b"])
        assert "db.s.unrelated" not in visible


# ---------------------------------------------------------------------------
# A limit case: selected=[] caused an IndexError. These tests record the fix.
# ---------------------------------------------------------------------------


class TestSelectedEmptyList:
    def test_empty_selected_list_renders_all(self):
        """An empty selected=[] means no selection. The tree shows each node."""
        dag = _build_dag([("db.s.a", TrouveType.SOURCE)])
        output = render_dag(dag, selected=[])
        assert "db.s.a" in output.visible_nodes


# ---------------------------------------------------------------------------
# The marker refactor: the complete limit cases and the hostile inputs
# ---------------------------------------------------------------------------


class TestSentinelRefactor:
    """These tests show the change from None to [], and each limit condition."""

    @pytest.fixture
    def three_node_chain(self) -> ClairDag:
        """A chain: SOURCE(src) -> TABLE(mid) -> TABLE(leaf)."""
        return _build_dag(
            [
                ("db.s.src", TrouveType.SOURCE),
                ("db.s.mid", TrouveType.TABLE),
                ("db.s.leaf", TrouveType.TABLE),
            ],
            [
                ("db.s.src", "db.s.mid"),
                ("db.s.mid", "db.s.leaf"),
            ],
        )

    @pytest.fixture
    def two_branch_dag(self) -> ClairDag:
        """Two independent branches: a.s.x -> a.s.y  and  b.s.p -> b.s.q"""
        return _build_dag(
            [
                ("a.s.x", TrouveType.SOURCE),
                ("a.s.y", TrouveType.TABLE),
                ("b.s.p", TrouveType.SOURCE),
                ("b.s.q", TrouveType.TABLE),
            ],
            [
                ("a.s.x", "a.s.y"),
                ("b.s.p", "b.s.q"),
            ],
        )

    # --- 1. selected=None shows each node. This is the reference case. ---

    def test_selected_none_renders_all_nodes(self, three_node_chain: ClairDag):
        output = render_dag(three_node_chain, selected=None)
        assert "db.s.src" in output.visible_nodes
        assert "db.s.mid" in output.visible_nodes
        assert "db.s.leaf" in output.visible_nodes

    def test_selected_none_selector_is_none(self, three_node_chain: ClairDag):
        output = render_dag(three_node_chain, selected=None)
        assert output.selector is None

    def test_selected_none_no_matched_nodes(self, three_node_chain: ClairDag):
        output = render_dag(three_node_chain, selected=None)
        assert output.matched_nodes == []

    # --- 2. selected=[] must show each node, as None does. ---

    def test_selected_empty_list_renders_all_nodes(self, three_node_chain: ClairDag):
        output = render_dag(three_node_chain, selected=[])
        assert "db.s.src" in output.visible_nodes
        assert "db.s.mid" in output.visible_nodes
        assert "db.s.leaf" in output.visible_nodes

    def test_selected_empty_list_selector_is_none(self, three_node_chain: ClairDag):
        output = render_dag(three_node_chain, selected=[])
        assert output.selector is None

    def test_selected_empty_list_no_matched_nodes(self, three_node_chain: ClairDag):
        output = render_dag(three_node_chain, selected=[])
        assert output.matched_nodes == []

    def test_selected_none_and_empty_list_produce_identical_output(
        self, three_node_chain: ClairDag
    ):
        output_none = render_dag(three_node_chain, selected=None)
        output_empty = render_dag(three_node_chain, selected=[])
        assert output_none.render() == output_empty.render()

    # --- 3. selected=["*.*.*"] agrees with each node. ---

    def test_star_star_star_matches_all_dotted_names(self, three_node_chain: ClairDag):
        output = render_dag(three_node_chain, selected=["*.*.*"])
        assert "db.s.src" in output.visible_nodes
        assert "db.s.mid" in output.visible_nodes
        assert "db.s.leaf" in output.visible_nodes

    def test_star_star_star_marks_all_nodes(self, three_node_chain: ClairDag):
        output = render_dag(three_node_chain, selected=["*.*.*"])
        assert set(output.matched_nodes) == {"db.s.src", "db.s.mid", "db.s.leaf"}

    def test_star_star_star_selector_shows_pattern(self, three_node_chain: ClairDag):
        output = render_dag(three_node_chain, selected=["*.*.*"])
        assert output.selector == "*.*.*"

    # --- 4. selected=["nonexistent.*"] gives the no-match message. ---

    def test_nonexistent_pattern_returns_no_match(
        self, three_node_chain: ClairDag
    ):
        output = render_dag(three_node_chain, selected=["nonexistent.*"])
        assert output.no_match is True

    def test_nonexistent_pattern_no_visible_nodes(self, three_node_chain: ClairDag):
        output = render_dag(three_node_chain, selected=["nonexistent.*"])
        assert output.visible_nodes == []

    def test_nonexistent_pattern_selector_preserved(self, three_node_chain: ClairDag):
        output = render_dag(three_node_chain, selected=["nonexistent.*"])
        assert output.selector == "nonexistent.*"

    # --- 5. Many patterns: clair joins the results. ---

    def test_multi_pattern_union_shows_both_branches(self, two_branch_dag: ClairDag):
        output = render_dag(two_branch_dag, selected=["a.s.*", "b.s.*"])
        assert "a.s.x" in output.visible_nodes
        assert "a.s.y" in output.visible_nodes
        assert "b.s.p" in output.visible_nodes
        assert "b.s.q" in output.visible_nodes

    def test_multi_pattern_marks_all_matched_nodes(self, two_branch_dag: ClairDag):
        output = render_dag(two_branch_dag, selected=["a.s.y", "b.s.q"])
        assert "a.s.y" in output.matched_nodes
        assert "b.s.q" in output.matched_nodes
        # A parent node is not a match.
        assert "a.s.x" not in output.matched_nodes
        assert "b.s.p" not in output.matched_nodes

    def test_multi_pattern_selector_shows_first_pattern(
        self, two_branch_dag: ClairDag
    ):
        output = render_dag(two_branch_dag, selected=["a.s.*", "b.s.*"])
        assert output.selector == "a.s.*"

    def test_multi_pattern_one_matching_one_not(self, three_node_chain: ClairDag):
        """The first pattern agrees with no node. The second pattern agrees with a leaf."""
        output = render_dag(three_node_chain, selected=["nope.*", "db.s.leaf"])
        assert "db.s.leaf" in output.visible_nodes
        assert "db.s.mid" in output.visible_nodes  # A parent node.
        assert "db.s.src" in output.visible_nodes  # A parent node.

    # --- 6. selected=[""], an empty pattern. ---

    def test_empty_string_pattern_matches_nothing(self, three_node_chain: ClairDag):
        """fnmatch(name, '') is False for each name that is not empty."""
        output = render_dag(three_node_chain, selected=[""])
        assert output.no_match is True

    def test_empty_string_pattern_selector_is_empty_string(
        self, three_node_chain: ClairDag
    ):
        output = render_dag(three_node_chain, selected=[""])
        assert output.selector == ""

    # --- 7. selected=["*"], one wildcard. ---

    def test_single_star_matches_dotted_names(self, three_node_chain: ClairDag):
        """For fnmatch, '.' is a usual character. Thus '*' agrees with 'db.s.leaf'."""
        output = render_dag(three_node_chain, selected=["*"])
        assert "db.s.src" in output.visible_nodes
        assert "db.s.mid" in output.visible_nodes
        assert "db.s.leaf" in output.visible_nodes

    def test_single_star_marks_all_nodes(self, three_node_chain: ClairDag):
        output = render_dag(three_node_chain, selected=["*"])
        assert set(output.matched_nodes) == {"db.s.src", "db.s.mid", "db.s.leaf"}

    # --- 8. A direct call to _compute_visible_nodes. ---

    def test_compute_visible_nodes_empty_list_returns_all(
        self, three_node_chain: ClairDag
    ):
        visible, matched = _compute_visible_nodes(three_node_chain, [])
        assert visible == {"db.s.src", "db.s.mid", "db.s.leaf"}
        assert matched == set()

    def test_compute_visible_nodes_none_returns_all(
        self, three_node_chain: ClairDag
    ):
        visible, matched = _compute_visible_nodes(three_node_chain, None)
        assert visible == {"db.s.src", "db.s.mid", "db.s.leaf"}
        assert matched == set()

    def test_compute_visible_nodes_none_and_empty_identical(
        self, three_node_chain: ClairDag
    ):
        vis_none, mat_none = _compute_visible_nodes(three_node_chain, None)
        vis_empty, mat_empty = _compute_visible_nodes(three_node_chain, [])
        assert vis_none == vis_empty
        assert mat_none == mat_empty

    # --- 9. The tree holds the correct parent nodes. ---

    def test_leaf_selected_ancestors_visible_but_unmarked(
        self, three_node_chain: ClairDag
    ):
        visible, matched = _compute_visible_nodes(
            three_node_chain, ["db.s.leaf"]
        )
        assert visible == {"db.s.src", "db.s.mid", "db.s.leaf"}
        assert matched == {"db.s.leaf"}

    def test_leaf_selected_only_leaf_is_matched(
        self, three_node_chain: ClairDag
    ):
        output = render_dag(three_node_chain, selected=["db.s.leaf"])
        assert output.matched_nodes == ["db.s.leaf"]

    def test_mid_node_selected_includes_ancestor_excludes_descendant(
        self, three_node_chain: ClairDag
    ):
        """A selection of the middle node shows src, the parent, but not leaf, the child."""
        visible, matched = _compute_visible_nodes(
            three_node_chain, ["db.s.mid"]
        )
        assert "db.s.src" in visible
        assert "db.s.mid" in visible
        assert "db.s.leaf" not in visible
        assert matched == {"db.s.mid"}

    # --- 10. A pattern that agrees with a SOURCE only. ---

    def test_selected_source_only_shows_source(self):
        dag = _build_dag(
            [
                ("src.raw.orders", TrouveType.SOURCE),
                ("db.s.model", TrouveType.TABLE),
            ],
            [("src.raw.orders", "db.s.model")],
        )
        output = render_dag(dag, selected=["src.raw.orders"])
        assert "src.raw.orders" in output.visible_nodes
        assert "db.s.model" not in output.visible_nodes

    def test_selected_source_is_matched(self):
        dag = _build_dag([("src.raw.orders", TrouveType.SOURCE)])
        output = render_dag(dag, selected=["src.raw.orders"])
        assert "src.raw.orders" in output.matched_nodes

    def test_selected_source_visible_set_is_singleton(self):
        dag = _build_dag(
            [
                ("src.raw.orders", TrouveType.SOURCE),
                ("db.s.model", TrouveType.TABLE),
            ],
            [("src.raw.orders", "db.s.model")],
        )
        visible, matched = _compute_visible_nodes(dag, ["src.raw.orders"])
        assert visible == {"src.raw.orders"}
        assert matched == {"src.raw.orders"}


class TestSentinelAdversarial:
    """Hostile inputs for the marker path."""

    @pytest.fixture
    def simple_dag(self) -> ClairDag:
        return _build_dag(
            [
                ("db.s.t", TrouveType.SOURCE),
                ("db.s.m", TrouveType.TABLE),
            ],
            [("db.s.t", "db.s.m")],
        )

    def test_whitespace_only_pattern_matches_nothing(self, simple_dag: ClairDag):
        """fnmatch('db.s.t', ' ') gives False."""
        output = render_dag(simple_dag, selected=[" "])
        assert output.no_match is True

    def test_newline_pattern_matches_nothing(self, simple_dag: ClairDag):
        output = render_dag(simple_dag, selected=["\n"])
        assert output.no_match is True

    def test_special_glob_chars_in_pattern(self, simple_dag: ClairDag):
        """A bracket and a question mark are correct fnmatch syntax."""
        output = render_dag(simple_dag, selected=["[d]b.s.t"])
        assert "db.s.t" in output.matched_nodes

    def test_question_mark_pattern(self, simple_dag: ClairDag):
        """A question mark agrees with one character. Thus 'db.s.?' agrees with 'db.s.t'."""
        output = render_dag(simple_dag, selected=["db.s.?"])
        assert "db.s.t" in output.matched_nodes
        assert "db.s.m" in output.matched_nodes

    def test_multiple_empty_strings_in_selected(self, simple_dag: ClairDag):
        """Each pattern is an empty string. Thus no node is a match."""
        output = render_dag(simple_dag, selected=["", ""])
        assert output.no_match is True

    def test_pattern_with_trailing_spaces(self, simple_dag: ClairDag):
        """A space at the end stops the match, because fnmatch compares the exact text."""
        output = render_dag(simple_dag, selected=["db.s.t "])
        assert output.no_match is True

    def test_sql_injection_in_pattern_is_harmless(self, simple_dag: ClairDag):
        """Clair gives the pattern to fnmatch only, not to the SQL. Thus no node is a match."""
        output = render_dag(simple_dag, selected=["'; DROP TABLE --"])
        assert output.no_match is True

    def test_very_long_pattern_does_not_crash(self, simple_dag: ClairDag):
        long_pattern = "a" * 10000
        output = render_dag(simple_dag, selected=[long_pattern])
        assert output.no_match is True

    def test_unicode_pattern_does_not_crash(self, simple_dag: ClairDag):
        output = render_dag(simple_dag, selected=["\u00e9\u00e8\u00ea.*"])
        assert output.no_match is True

    def test_null_byte_in_pattern(self, simple_dag: ClairDag):
        output = render_dag(simple_dag, selected=["db\x00.s.t"])
        assert output.no_match is True


class TestCLISentinelFlow:
    """These tests show the selection logic of the CLI dag command."""

    def test_cli_none_select_produces_none(self):
        """When click gives select=None, the dag command gives selected=None."""
        select = None
        selected = [select] if select else None
        assert selected is None

    def test_cli_string_select_produces_singleton_list(self):
        select = "analytics.*"
        selected = [select] if select else None
        assert selected == ["analytics.*"]

    def test_cli_empty_string_select_produces_none(self):
        """An empty string from click is false, and thus it becomes None."""
        select = ""
        selected = [select] if select else None
        assert selected is None

    def test_none_through_render_dag_normalizes_to_full_render(self):
        """None goes to render_dag. There `selected or []` gives [], and the tree shows each node."""
        dag = _build_dag(
            [("db.s.a", TrouveType.SOURCE), ("db.s.b", TrouveType.TABLE)],
            [("db.s.a", "db.s.b")],
        )
        output = render_dag(dag, selected=None)
        assert "db.s.a" in output.visible_nodes
        assert "db.s.b" in output.visible_nodes
        assert output.selector is None
