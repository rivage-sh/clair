"""The tests of `clair.lineage`, the public entry point to the project DAG.

`get_dag` is one short function, and it holds no logic of its own: it calls
`discover_project` and then `build_dag`. The tests below therefore do not
examine the DAG algorithms again. They hold the contract that
`examples/notebooks/02_lineage_and_impact.ipynb` and
`examples/notebooks/04_author_trouves.ipynb` depend on, because the notebooks
are documentation that the build does not execute.

That contract is:

* `get_dag(project_root)` accepts a `str` and a `Path`.
* The result is a `ClairDag`, thus each networkx algorithm applies to it --
  `descendants`, `topological_sort`, `subgraph`, `out_degree`.
* Each node key is a physical address.
* `dag.get_trouve(node).type.value` gives the text of the type, and a notebook
  compares it to "source".
"""

from __future__ import annotations

from pathlib import Path

import networkx as nx
import pytest

from clair.core.dag import ClairDag
from clair.lineage import get_dag
from clair.trouves.trouve import TrouveType

SOURCE_ADDRESS = "source.raw.orders"
TABLE_ADDRESS = "analytics.revenue.daily_orders"


class TestTheGraphOfASimpleProject:
    """`get_dag` gives the nodes and the edges of the project on disk."""

    def test_the_result_is_a_clair_dag(self, simple_project: Path):
        assert isinstance(get_dag(simple_project), ClairDag)

    def test_each_trouve_is_a_node(self, simple_project: Path):
        dag = get_dag(simple_project)

        assert set(dag.nodes) == {SOURCE_ADDRESS, TABLE_ADDRESS}

    def test_an_import_becomes_an_edge(self, simple_project: Path):
        """daily_orders imports orders, thus orders comes first."""
        dag = get_dag(simple_project)

        assert list(dag.edges) == [(SOURCE_ADDRESS, TABLE_ADDRESS)]

    def test_each_node_holds_its_compiled_trouve(self, simple_project: Path):
        dag = get_dag(simple_project)

        assert dag.get_trouve(SOURCE_ADDRESS).type == TrouveType.SOURCE
        assert dag.get_trouve(TABLE_ADDRESS).type == TrouveType.TABLE

    def test_the_type_value_is_the_text_that_a_notebook_reads(
        self, simple_project: Path
    ):
        """Notebook 02 selects the sources with `type.value == "source"`."""
        dag = get_dag(simple_project)

        sources = [
            node for node in dag.nodes if dag.get_trouve(node).type.value == "source"
        ]

        assert sources == [SOURCE_ADDRESS]

    def test_a_node_of_the_dag_is_compiled(self, simple_project: Path):
        """A caller reads the SQL, thus the discovery step must compile it."""
        trouve = get_dag(simple_project).get_trouve(TABLE_ADDRESS)

        assert trouve.is_compiled
        assert trouve.compiled is not None
        assert SOURCE_ADDRESS in trouve.compiled.resolved_sql

    def test_an_unknown_node_raises(self, simple_project: Path):
        with pytest.raises(KeyError):
            get_dag(simple_project).get_trouve("no.such.table")


class TestTheProjectRootArgument:
    """The signature accepts the two path types that a notebook writes."""

    def test_a_path_gives_the_graph(self, simple_project: Path):
        assert get_dag(simple_project).number_of_nodes() == 2

    def test_a_string_gives_the_same_graph(self, simple_project: Path):
        assert set(get_dag(str(simple_project)).nodes) == set(
            get_dag(simple_project).nodes
        )

    def test_a_directory_that_holds_no_trouve_gives_an_empty_graph(
        self, tmp_path: Path
    ):
        dag = get_dag(tmp_path)

        assert dag.number_of_nodes() == 0
        assert dag.number_of_edges() == 0

    def test_a_cyclic_project_gives_an_empty_graph(self, cyclic_project: Path):
        """A cycle between two Trouve files stops at the Python import step.

        `a.py` imports `b.py`, and `b.py` imports `a.py`. Python raises an
        ImportError, thus the discovery step loads no Trouve and it writes a
        warning. `get_dag` therefore gives an empty graph, and it raises no
        CyclicDependencyError: `build_dag` sees no node, so it finds no cycle.

        A caller that wants the error must call `clair.validate()`, which reads
        the discovery errors.
        """
        dag = get_dag(cyclic_project)

        assert dag.number_of_nodes() == 0


class TestTheNetworkxAlgorithms:
    """Notebook 02 answers each of its questions with a networkx call."""

    def test_descendants_give_the_impact_of_a_source(self, simple_project: Path):
        dag = get_dag(simple_project)

        assert nx.descendants(dag, SOURCE_ADDRESS) == {TABLE_ADDRESS}

    def test_ancestors_give_what_clair_builds_first(self, simple_project: Path):
        dag = get_dag(simple_project)

        assert nx.ancestors(dag, TABLE_ADDRESS) == {SOURCE_ADDRESS}

    def test_out_degree_gives_the_direct_children(self, simple_project: Path):
        dag = get_dag(simple_project)

        assert dag.out_degree(SOURCE_ADDRESS) == 1
        assert dag.out_degree(TABLE_ADDRESS) == 0

    def test_topological_sort_puts_the_dependency_first(self, simple_project: Path):
        dag = get_dag(simple_project)

        assert list(nx.topological_sort(dag)) == [SOURCE_ADDRESS, TABLE_ADDRESS]

    def test_a_subgraph_of_the_downstream_nodes_sorts(self, simple_project: Path):
        """Notebook 02 sorts the blast radius, thus subgraph must keep the type."""
        dag = get_dag(simple_project)
        downstream = nx.descendants(dag, SOURCE_ADDRESS)

        assert list(nx.topological_sort(dag.subgraph(downstream))) == [TABLE_ADDRESS]
