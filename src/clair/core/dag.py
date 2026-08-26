"""Clair makes the DAG, finds each cycle, and sorts the nodes."""

from __future__ import annotations

from collections.abc import Sequence

import networkx as nx

from clair.exceptions import CyclicDependencyError
from clair.trouves.trouve import TrouveAbc, TrouveType


class ClairDag(nx.DiGraph):
    """A directed acyclic graph of the Clair Trouves.

    Each node is a physical_address, for example "analytics.revenue.daily_orders". Each
    node has a "trouve" attribute that holds the compiled Trouve. Each edge is a
    (dependency, dependent) pair. Clair reads the pairs from the Trouve imports.
    """

    def add_trouve(self, trouve: TrouveAbc) -> None:
        """Add a compiled Trouve as a node. The key of the node is its physical_address."""
        self.add_node(str(trouve.physical_address), trouve=trouve)

    def add_dependency(self, dependency: str, dependent: str) -> None:
        """Add an edge from *dependency* to *dependent*.

        Raises:
            KeyError: If the graph does not contain one of the two nodes.
        """
        if dependency not in self:
            raise KeyError(
                f"Clair cannot find the dependency node '{dependency}' in the DAG. "
                "Add it with add_trouve() first."
            )
        if dependent not in self:
            raise KeyError(
                f"Clair cannot find the dependent node '{dependent}' in the DAG. "
                "Add it with add_trouve() first."
            )
        self.add_edge(dependency, dependent)

    def get_trouve(self, physical_address: str) -> TrouveAbc:
        """Give the Trouve of a node.

        Raises:
            KeyError: If the graph does not contain *physical_address*.
        """
        if physical_address not in self:
            raise KeyError(
                f"Clair cannot find the node '{physical_address}' in the DAG. "
                f"The DAG contains these nodes: {sorted(self.nodes)}"
            )
        return self.nodes[physical_address]["trouve"]

    def validate(self) -> None:
        """Examine the structure of the DAG.

        Raises:
            AssertionError: If a node has no correct Trouve.
            CyclicDependencyError: If the graph contains a cycle.
        """
        for node in self.nodes:
            trouve = self.nodes[node].get("trouve")
            assert trouve is not None, (
                f"Node '{node}' is missing the 'trouve' attribute"
            )
            assert isinstance(trouve, TrouveAbc), (
                f"Node '{node}' has a 'trouve' attribute of type "
                f"{type(trouve).__name__}, expected TrouveAbc"
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
    def trouves(self) -> list[TrouveAbc]:
        """Give each compiled Trouve object in the graph."""
        return [self.nodes[node]["trouve"] for node in self.nodes]


def build_dag(trouves: Sequence[TrouveAbc]) -> ClairDag:
    """Make a directed acyclic graph from the compiled Trouves.

    Raises:
        CyclicDependencyError: If the import graph contains a cycle.
    """
    dag = ClairDag()

    for trouve in trouves:
        dag.add_trouve(trouve)

    # compiled.imports holds the logical addresses, but each DAG node has the
    # physical address as its key. Make a map, so that each edge is correct with
    # a routing policy active.
    logical_to_physical = {
        str(t.compiled.logical_address): str(t.physical_address)
        for t in trouves
        if t.compiled
    }

    for trouve in trouves:
        assert trouve.compiled is not None
        for dependency in trouve.compiled.imports:
            physical_dependency = logical_to_physical.get(dependency, dependency)
            if physical_dependency in dag:
                dag.add_dependency(physical_dependency, str(trouve.physical_address))

    dag.validate()
    return dag


def get_execution_order(dag: ClairDag) -> list[str]:
    """Give the addresses in topological order. Each dependency comes first."""
    return list(nx.topological_sort(dag))


def get_executable_nodes(dag: ClairDag) -> list[str]:
    """Give each node that is not a SOURCE, in topological order."""
    return [
        name for name in get_execution_order(dag)
        if dag.get_trouve(name).type != TrouveType.SOURCE
    ]


def logical_address_of(dag: ClairDag, physical_address: str) -> str:
    """Give the logical address of the DAG node that *physical_address* keys.

    Give the physical address back if the node holds no compiled attributes.
    """
    trouve = dag.get_trouve(physical_address)
    if trouve.compiled is None:
        return physical_address
    return str(trouve.compiled.logical_address)
