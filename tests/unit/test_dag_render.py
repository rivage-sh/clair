"""The tests of the DAG render code, which draws a ClairDag as a tree.

``render_dag`` gives the tree as data. Each test here reads that data: the
counts, the depth of a node, the children of a node, the repeat flag. One class
at the end covers the text, because the text is the contract of `clair dag`.
"""

from __future__ import annotations

from typing import cast

import pytest

from clair.core.dag_render import (
    MATCH_MARKER,
    REPEAT_MARKER,
    DagNodeTag,
    DagTreeNode,
    format_header,
    node_tag,
    render_dag,
)
from clair.exceptions import ClairError
from clair.trouves.trouve import ExecutionType, TrouveType
from tests.helpers import build_dag_of, make_compiled_seed_trouve

# The shapes that the tests share. A name says what the shape proves.
CHAIN_NODES = [
    ("db.s.a", TrouveType.SOURCE),
    ("db.s.b", TrouveType.TABLE),
    ("db.s.c", TrouveType.TABLE),
]
CHAIN_EDGES = [("db.s.a", "db.s.b"), ("db.s.b", "db.s.c")]

DIAMOND_NODES = [
    ("db.s.a", TrouveType.SOURCE),
    ("db.s.b", TrouveType.TABLE),
    ("db.s.c", TrouveType.TABLE),
    ("db.s.d", TrouveType.TABLE),
]
DIAMOND_EDGES = [
    ("db.s.a", "db.s.b"),
    ("db.s.a", "db.s.c"),
    ("db.s.b", "db.s.d"),
    ("db.s.c", "db.s.d"),
]


@pytest.fixture
def chain_dag():
    """A -> B -> C, where A is a SOURCE."""
    return build_dag_of(CHAIN_NODES, CHAIN_EDGES)


@pytest.fixture
def diamond_dag():
    """A -> B -> D and A -> C -> D. D has two parents."""
    return build_dag_of(DIAMOND_NODES, DIAMOND_EDGES)


class TestCounts:
    """The header counts the models and the sources of the visible nodes."""

    @pytest.mark.parametrize(
        ("nodes", "expected_model_count", "expected_source_count"),
        [
            ([], 0, 0),
            ([("db.s.a", TrouveType.SOURCE)], 0, 1),
            ([("db.s.a", TrouveType.TABLE)], 1, 0),
            ([("db.s.a", TrouveType.VIEW)], 1, 0),
            (CHAIN_NODES, 2, 1),
            (
                [
                    ("db.s.a", TrouveType.SOURCE),
                    ("db.s.b", TrouveType.SOURCE),
                    ("db.s.c", TrouveType.TABLE),
                ],
                1,
                2,
            ),
        ],
    )
    def test_each_shape_gives_its_counts(
        self, nodes, expected_model_count, expected_source_count
    ):
        output = render_dag(build_dag_of(nodes))
        assert output.model_count == expected_model_count
        assert output.source_count == expected_source_count

    def test_an_empty_dag_has_no_node(self):
        output = render_dag(build_dag_of([]))
        assert output.visible_nodes == []
        assert output.roots == []
        assert output.no_match is False

    def test_the_counts_follow_the_selection(self, chain_dag):
        """A selector hides db.s.c, thus the model count falls to one."""
        output = render_dag(chain_dag, ["db.s.b"])
        assert output.model_count == 1
        assert output.source_count == 1


class TestTreeShape:
    """The tree holds the parents above the children."""

    def test_a_chain_gives_one_root_and_one_child_at_each_level(self, chain_dag):
        output = render_dag(chain_dag)
        assert [root.physical_address for root in output.roots] == ["db.s.a"]
        assert output.depth_of("db.s.a") == 0
        assert output.depth_of("db.s.b") == 1
        assert output.depth_of("db.s.c") == 2

    def test_a_child_hangs_below_its_parent(self, chain_dag):
        output = render_dag(chain_dag)
        node_a = output.find("db.s.a")
        assert node_a is not None
        assert [child.physical_address for child in node_a.children] == ["db.s.b"]

    def test_two_independent_roots_both_show(self):
        dag = build_dag_of(
            [
                ("db.s.a", TrouveType.SOURCE),
                ("db.s.b", TrouveType.TABLE),
                ("db.s.x", TrouveType.SOURCE),
                ("db.s.y", TrouveType.TABLE),
            ],
            [("db.s.a", "db.s.b"), ("db.s.x", "db.s.y")],
        )
        output = render_dag(dag)
        assert [root.physical_address for root in output.roots] == ["db.s.a", "db.s.x"]

    def test_the_children_of_one_node_are_in_alphabetic_order(self):
        dag = build_dag_of(
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
        root = render_dag(dag).roots[0]
        assert [child.physical_address for child in root.children] == [
            "db.s.apple",
            "db.s.mango",
            "db.s.zebra",
        ]

    def test_a_node_with_two_parents_holds_its_children_one_time(self, diamond_dag):
        """D hangs below B and below C. Only one of the two holds the subtree."""
        output = render_dag(diamond_dag)
        places = [
            node for _depth, node in output.walk() if node.physical_address == "db.s.d"
        ]
        assert len(places) == 2
        assert [place.is_repeat for place in places] == [False, True]

    def test_a_repeat_place_holds_no_child(self):
        dag = build_dag_of(
            DIAMOND_NODES + [("db.s.e", TrouveType.TABLE)],
            DIAMOND_EDGES + [("db.s.d", "db.s.e")],
        )
        output = render_dag(dag)
        repeat_places = [
            node
            for _depth, node in output.walk()
            if node.physical_address == "db.s.d" and node.is_repeat
        ]
        assert repeat_places[0].children == []

    def test_the_first_place_of_a_shared_node_owns_the_subtree(self, diamond_dag):
        """B comes before C in alphabetic order, thus D hangs below B."""
        output = render_dag(diamond_dag)
        node_b = output.find("db.s.b")
        assert node_b is not None
        assert [child.physical_address for child in node_b.children] == ["db.s.d"]
        assert output.depth_of("db.s.d") == 2

    def test_depth_of_refuses_an_absent_node(self, chain_dag):
        output = render_dag(chain_dag)
        with pytest.raises(ClairError):
            output.depth_of("db.s.absent")

    def test_find_gives_none_for_an_absent_node(self, chain_dag):
        assert render_dag(chain_dag).find("db.s.absent") is None


class TestSelection:
    """A selector keeps the nodes that agree, and the parents above them."""

    def test_no_selector_shows_each_node_and_matches_none(self, chain_dag):
        output = render_dag(chain_dag)
        assert output.visible_nodes == ["db.s.a", "db.s.b", "db.s.c"]
        assert output.matched_nodes == []
        assert output.selector is None

    @pytest.mark.parametrize("selected", [None, []])
    def test_an_absent_selector_shows_each_node(self, chain_dag, selected):
        assert len(render_dag(chain_dag, selected).visible_nodes) == 3

    def test_a_selector_keeps_the_node_and_its_parents(self, chain_dag):
        output = render_dag(chain_dag, ["db.s.b"])
        assert output.visible_nodes == ["db.s.a", "db.s.b"]
        assert output.matched_nodes == ["db.s.b"]

    def test_a_parent_of_a_match_is_visible_and_not_matched(self, chain_dag):
        output = render_dag(chain_dag, ["db.s.b"])
        node_a = output.find("db.s.a")
        node_b = output.find("db.s.b")
        assert node_a is not None and node_b is not None
        assert node_a.is_matched is False
        assert node_b.is_matched is True

    def test_an_unrelated_node_goes_away(self):
        dag = build_dag_of(
            CHAIN_NODES + [("db.s.other", TrouveType.TABLE)], CHAIN_EDGES
        )
        assert "db.s.other" not in render_dag(dag, ["db.s.b"]).visible_nodes

    def test_a_glob_pattern_matches_more_than_one_node(self, chain_dag):
        output = render_dag(chain_dag, ["db.s.*"])
        assert output.matched_nodes == ["db.s.a", "db.s.b", "db.s.c"]

    def test_a_selector_that_matches_nothing_reports_no_match(self, chain_dag):
        output = render_dag(chain_dag, ["db.s.absent"])
        assert output.no_match is True
        assert output.visible_nodes == []
        assert output.roots == []
        assert output.selector == "db.s.absent"

    def test_the_output_keeps_the_first_pattern(self, chain_dag):
        assert render_dag(chain_dag, ["db.s.b", "db.s.c"]).selector == "db.s.b"


class TestNodeTag:
    """The tag names the Trouve type, and PANDAS wins against the type."""

    @pytest.mark.parametrize(
        ("trouve_type", "execution_type", "expected_tag"),
        [
            (TrouveType.SOURCE, ExecutionType.SNOWFLAKE, DagNodeTag.SOURCE),
            (TrouveType.TABLE, ExecutionType.SNOWFLAKE, DagNodeTag.TABLE),
            (TrouveType.VIEW, ExecutionType.SNOWFLAKE, DagNodeTag.VIEW),
            (TrouveType.TABLE, ExecutionType.PANDAS, DagNodeTag.PANDAS),
            (TrouveType.VIEW, ExecutionType.PANDAS, DagNodeTag.PANDAS),
        ],
    )
    def test_each_pair_gives_its_tag(self, trouve_type, execution_type, expected_tag):
        assert node_tag(trouve_type, execution_type) == expected_tag

    def test_a_seed_gives_seed_and_not_pandas(self):
        """A seed executes in pandas, but SEED tells the reader more."""
        assert (
            node_tag(TrouveType.TABLE, ExecutionType.PANDAS, is_seed=True)
            == DagNodeTag.SEED
        )

    def test_an_unknown_execution_type_raises(self):
        """A new member of the enum must not fall through to a wrong tag.

        The cast gives a value that the enum does not hold. The type checker
        stops a caller from writing this, and the run time must stop it too.
        """
        with pytest.raises(ClairError):
            node_tag(TrouveType.TABLE, cast(ExecutionType, "duckdb"))

    def test_an_unknown_trouve_type_raises(self):
        with pytest.raises(ClairError):
            node_tag(cast(TrouveType, "materialised_view"), ExecutionType.SNOWFLAKE)

    def test_the_tree_carries_the_type_of_each_trouve(self, chain_dag):
        output = render_dag(chain_dag)
        node_a = output.find("db.s.a")
        assert node_a is not None
        assert node_a.trouve_type == TrouveType.SOURCE
        assert node_a.tag == DagNodeTag.SOURCE
        assert node_a.is_seed is False

    def test_the_tree_marks_a_seed(self):
        """A seed and a PandasTrouve both execute in pandas. The tags differ."""
        dag = build_dag_of([("db.s.a", TrouveType.TABLE)])
        dag.add_trouve(make_compiled_seed_trouve("db.s.countries"))

        output = render_dag(dag)
        seed_node = output.find("db.s.countries")
        assert seed_node is not None
        assert seed_node.is_seed is True
        assert seed_node.tag == DagNodeTag.SEED


class TestHeaderText:
    """The header says the counts, and it names the selector."""

    @pytest.mark.parametrize(
        ("model_count", "source_count", "pattern", "expected"),
        [
            (0, 0, None, "=== Clair DAG: 0 models, 0 sources ==="),
            (1, 1, None, "=== Clair DAG: 1 model, 1 source ==="),
            (2, 3, None, "=== Clair DAG: 2 models, 3 sources ==="),
            (
                1,
                1,
                "db.s.*",
                "=== Clair DAG (filtered: db.s.*): 1 model, 1 source ===",
            ),
        ],
    )
    def test_each_count_gives_its_header(
        self, model_count, source_count, pattern, expected
    ):
        assert format_header(model_count, source_count, pattern) == expected


class TestRenderedText:
    """The text of `clair dag`. The data tests above cover the semantics."""

    def test_a_chain_draws_one_branch_at_each_level(self, chain_dag):
        assert render_dag(chain_dag).render() == (
            "=== Clair DAG: 2 models, 1 source ===\n"
            "\n"
            "db.s.a  [SOURCE]\n"
            "└── db.s.b  [TABLE]\n"
            "    └── db.s.c  [TABLE]"
        )

    def test_a_diamond_marks_the_second_place_of_the_shared_node(self, diamond_dag):
        assert render_dag(diamond_dag).render() == (
            "=== Clair DAG: 3 models, 1 source ===\n"
            "\n"
            "db.s.a  [SOURCE]\n"
            "├── db.s.b  [TABLE]\n"
            "│   └── db.s.d  [TABLE]\n"
            "└── db.s.c  [TABLE]\n"
            "    └── db.s.d  [TABLE]  (^)"
        )

    def test_one_blank_line_separates_two_roots(self):
        dag = build_dag_of(
            [("db.s.a", TrouveType.SOURCE), ("db.s.x", TrouveType.SOURCE)]
        )
        assert render_dag(dag).render() == (
            "=== Clair DAG: 0 models, 2 sources ===\n"
            "\n"
            "db.s.a  [SOURCE]\n"
            "\n"
            "db.s.x  [SOURCE]"
        )

    def test_a_matched_node_holds_the_match_marker(self, chain_dag):
        rendered = render_dag(chain_dag, ["db.s.b"]).render()
        assert rendered.endswith(f"db.s.b  [TABLE]  {MATCH_MARKER}")

    def test_a_repeat_place_holds_the_repeat_marker(self, diamond_dag):
        assert REPEAT_MARKER in render_dag(diamond_dag).render()

    def test_no_match_gives_one_sentence(self, chain_dag):
        assert render_dag(chain_dag, ["db.s.absent"]).render() == (
            "Clair found no Trouves for the selector 'db.s.absent'."
        )


class TestTreeNodeWalk:
    """DagTreeNode.walk gives the parent first, then each child."""

    def test_walk_gives_the_parent_before_the_child(self, chain_dag):
        addresses = [
            node.physical_address for _depth, node in render_dag(chain_dag).walk()
        ]
        assert addresses == ["db.s.a", "db.s.b", "db.s.c"]

    def test_walk_gives_the_depth_of_each_node(self, chain_dag):
        depths = {
            node.physical_address: depth
            for depth, node in render_dag(chain_dag).walk()
        }
        assert depths == {"db.s.a": 0, "db.s.b": 1, "db.s.c": 2}

    def test_a_leaf_walks_to_itself_only(self):
        leaf = DagTreeNode(
            physical_address="db.s.a",
            trouve_type=TrouveType.TABLE,
            execution_type=ExecutionType.SNOWFLAKE,
            is_matched=False,
            is_repeat=False,
            children=[],
        )
        assert list(leaf.walk()) == [(0, leaf)]
