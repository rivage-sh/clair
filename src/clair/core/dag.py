"""Clair makes the DAG, finds each cycle, and sorts the nodes."""

from __future__ import annotations

from collections.abc import Sequence
from enum import StrEnum

import networkx as nx

from clair.exceptions import CyclicDependencyError
from clair.trouves.address import NodeAddresses, TrouveAddress
from clair.trouves.trouve import TrouveAbc
from clair.trouves.trouve_type import TrouveType


class EdgeKind(StrEnum):
    """Why one Trouve waits for another."""

    # The build of the dependent reads the dependency.
    READ = "read"
    # Only a TestSql of the dependent reads the dependency.
    TEST = "test"


class ClairDag(nx.DiGraph):
    """A directed acyclic graph of the Clair Trouves.

    Each node is a physical_address, for example "analytics.revenue.daily_orders". Each
    node has a "trouve" attribute that holds the compiled Trouve. Each edge is a
    (dependency, dependent) pair, with a "kind" attribute that holds an EdgeKind.
    Clair reads the pairs from the references of each Trouve. The run treats each
    kind the same: the dependent waits for the dependency, and a failure of the
    dependency skips the dependent.
    """

    def add_trouve(self, trouve: TrouveAbc) -> None:
        """Add a compiled Trouve as a node. The key of the node is its physical_address."""
        self.add_node(str(trouve.require_compiled().physical_address), trouve=trouve)

    def add_dependency(
        self, dependency: str, dependent: str, kind: EdgeKind = EdgeKind.READ
    ) -> None:
        """Add an edge from *dependency* to *dependent*. A READ edge replaces a TEST edge.

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
        if self.has_edge(dependency, dependent) and kind == EdgeKind.TEST:
            return
        self.add_edge(dependency, dependent, kind=kind)

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
        str(trouve.logical_address): str(trouve.require_compiled().physical_address)
        for trouve in trouves
    }

    for trouve in trouves:
        compiled = trouve.require_compiled()
        edges = [
            *((dependency, EdgeKind.READ) for dependency in compiled.imports),
            *((dependency, EdgeKind.TEST) for dependency in compiled.test_imports),
        ]
        for dependency, kind in edges:
            physical_dependency = logical_to_physical.get(dependency, dependency)
            if physical_dependency in dag:
                dag.add_dependency(physical_dependency, str(compiled.physical_address), kind)

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


def addresses_of(
    dag: ClairDag, physical_address: str, staging_address: TrouveAddress | None = None
) -> NodeAddresses:
    """Give the addresses of the DAG node that *physical_address* keys."""
    trouve = dag.get_trouve(physical_address)
    return NodeAddresses(
        logical=trouve.logical_address,
        physical=TrouveAddress.parse(physical_address),
        staging=staging_address,
    )
