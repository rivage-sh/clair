"""Public API for programmatic access to the Clair project DAG."""

from __future__ import annotations

from pathlib import Path

from clair.core.dag import ClairDag, build_dag
from clair.core.discovery import discover_project


def get_dag(project_root: str | Path) -> ClairDag:
    """Load a Clair project and return its dependency graph.

    This is the public Python API for Clair's lineage. The returned graph
    is a ClairDag (a networkx DiGraph subclass) where:
    - Nodes are full_name strings (e.g., "analytics.revenue.daily_orders")
    - Edges are (dependency, dependent)
    - Each node has a "trouve" attribute with the compiled Trouve

    Args:
        project_root: Path to the Clair project root directory.

    Returns:
        A ClairDag representing the project's dependency graph.
    """
    discovered = discover_project(Path(project_root))
    return build_dag(discovered)
