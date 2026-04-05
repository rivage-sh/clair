"""DAG construction, cycle detection, and topological sort."""

from __future__ import annotations

import networkx as nx

from clair.exceptions import CyclicDependencyError
from clair.trouves.trouve import Trouve, TrouveType


class ClairDag(nx.DiGraph):
    """A directed acyclic graph of Clair Trouves.

    Nodes are full_name strings (e.g. "analytics.revenue.daily_orders").
    Each node has a "trouve" attribute holding the compiled Trouve.
    Edges are (dependency, dependent) -- inferred from Trouve imports.
    """

    def add_trouve(self, trouve: Trouve) -> None:
        """Add a compiled Trouve as a node. The node key is its full_name."""
        self.add_node(trouve.full_name, trouve=trouve)

    def add_dependency(self, dependency: str, dependent: str) -> None:
        """Add an edge from *dependency* to *dependent*.

        Raises:
            KeyError: If either node is missing.
        """
        if dependency not in self:
            raise KeyError(
                f"Dependency node '{dependency}' not found in the DAG. "
                "Add it with add_trouve() first."
            )
        if dependent not in self:
            raise KeyError(
                f"Dependent node '{dependent}' not found in the DAG. "
                "Add it with add_trouve() first."
            )
        self.add_edge(dependency, dependent)

    def get_trouve(self, full_name: str) -> Trouve:
        """Return the Trouve for a node.

        Raises:
            KeyError: If *full_name* is not in the graph.
        """
        if full_name not in self:
            raise KeyError(
                f"Node '{full_name}' not found in the DAG. "
                f"Known nodes: {sorted(self.nodes)}"
            )
        return self.nodes[full_name]["trouve"]

    def validate(self) -> None:
        """Assert structural invariants of the DAG.

        Raises:
            AssertionError: If a node is missing a valid Trouve.
            CyclicDependencyError: If the graph contains a cycle.
        """
        for node in self.nodes:
            trouve = self.nodes[node].get("trouve")
            assert trouve is not None, (
                f"Node '{node}' is missing the 'trouve' attribute"
            )
            assert isinstance(trouve, Trouve), (
                f"Node '{node}' has a 'trouve' attribute of type "
                f"{type(trouve).__name__}, expected Trouve"
            )

        if not nx.is_directed_acyclic_graph(self):
            cycle = nx.find_cycle(self)
            raise CyclicDependencyError(list(cycle))

        for source, target in self.edges:
            assert source in self.nodes, (
                f"Edge ({source}, {target}) references missing source node '{source}'"
            )
            assert target in self.nodes, (
                f"Edge ({source}, {target}) references missing target node '{target}'"
            )

    @property
    def trouves(self) -> list[Trouve]:
        """Return all compiled Trouve objects in the graph."""
        return [self.nodes[node]["trouve"] for node in self.nodes]


def build_dag(trouves: list[Trouve]) -> ClairDag:
    """Build a directed acyclic graph from compiled Trouves.

    Raises:
        CyclicDependencyError: If the import graph contains a cycle.
    """
    dag = ClairDag()

    for trouve in trouves:
        dag.add_trouve(trouve)

    # compiled.imports holds logical names; DAG nodes are keyed by routed names.
    # Build a mapping so edges resolve correctly when routing is active.
    logical_to_routed = {
        t.compiled.logical_name: t.full_name for t in trouves if t.compiled
    }

    for trouve in trouves:
        assert trouve.compiled is not None
        for dep_name in trouve.compiled.imports:
            routed_dep = logical_to_routed.get(dep_name, dep_name)
            if routed_dep in dag:
                dag.add_dependency(routed_dep, trouve.full_name)

    dag.validate()
    return dag


def get_execution_order(dag: ClairDag) -> list[str]:
    """Return full_names in topological order (dependencies first)."""
    return list(nx.topological_sort(dag))


def get_executable_nodes(dag: ClairDag) -> list[str]:
    """Return non-SOURCE nodes in topological order."""
    return [
        name for name in get_execution_order(dag)
        if dag.get_trouve(name).type != TrouveType.SOURCE
    ]
