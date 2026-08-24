"""The public API that gives your code access to the Clair project DAG."""

from __future__ import annotations

from pathlib import Path

from clair.core.dag import ClairDag, build_dag
from clair.core.discovery import discover_project


def get_dag(project_root: str | Path) -> ClairDag:
    """Load a Clair project and give its dependency graph.

    This function is the public Python API for the Clair lineage. The result is
    a ClairDag, a subclass of the networkx DiGraph class. In that graph:
    - Each node is a physical_address, for example "analytics.revenue.daily_orders"
    - Each edge is a (dependency, dependent) pair
    - Each node has a "trouve" attribute that holds the compiled Trouve

    Args:
        project_root: The path of the Clair project root directory.

    Returns:
        A ClairDag. It is the dependency graph of the project.
    """
    discovered = discover_project(Path(project_root))
    return build_dag(discovered)
