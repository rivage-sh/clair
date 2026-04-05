"""DAG rendering -- pure-function tree visualization of a ClairDag."""

from __future__ import annotations

from fnmatch import fnmatch

import networkx as nx
from pydantic import BaseModel, PrivateAttr

from clair.core.dag import ClairDag
from clair.exceptions import ClairError
from clair.trouves.trouve import ExecutionType, TrouveType


class DagRenderOutput(BaseModel):
    """Structured result of rendering a DAG."""

    model_count: int
    source_count: int
    visible_nodes: list[str]
    matched_nodes: list[str]
    selector: str | None
    no_match: bool

    # Internal: the pre-rendered tree string. Built during construction
    # so that .render() is byte-for-byte identical to the old return value.
    _rendered: str = PrivateAttr(default="")

    def render(self) -> str:
        """Produce the formatted tree string for stdout."""
        return self._rendered


def render_dag(dag: ClairDag, selected: list[str] | None = None) -> DagRenderOutput:
    """Render a ClairDag as a tree with top-to-bottom data flow.

    Pure function with no side effects. Returns a DagRenderOutput with
    structured data and a ``.render()`` method that produces the tree string.

    Args:
        dag: The dependency graph to render.
        selected: Optional glob patterns to filter visible nodes.
            When provided, only matched nodes and their transitive
            upstream ancestors are shown.

    Returns:
        A DagRenderOutput with structured fields and a .render() method.
    """
    selected = selected or []
    visible, matched = _compute_visible_nodes(dag, selected)

    if selected and not visible:
        result = DagRenderOutput(
            model_count=0,
            source_count=0,
            visible_nodes=[],
            matched_nodes=[],
            selector=selected[0] if selected else None,
            no_match=True,
        )
        result._rendered = f"No Trouves match the selector '{selected[0]}'."
        return result

    n_sources = sum(
        1 for n in visible if dag.get_trouve(n).type == TrouveType.SOURCE
    )
    n_models = len(visible) - n_sources

    pattern = selected[0] if selected else None
    header = _format_header(n_models, n_sources, pattern)

    subgraph = dag.subgraph(visible)
    roots = sorted(n for n in subgraph.nodes if subgraph.in_degree(n) == 0)

    lines = [header, ""]
    printed: set[str] = set()

    for i, root in enumerate(roots):
        if i > 0:
            lines.append("")
        _render_subtree(
            dag, subgraph, root, matched, printed, lines,
            prefix="", is_last=True, is_root=True,
        )

    result = DagRenderOutput(
        model_count=n_models,
        source_count=n_sources,
        visible_nodes=sorted(visible),
        matched_nodes=sorted(matched),
        selector=pattern,
        no_match=False,
    )
    result._rendered = "\n".join(lines)
    return result


def _render_subtree(
    dag: ClairDag,
    subgraph,
    node: str,
    matched: set[str],
    printed: set[str],
    lines: list[str],
    prefix: str,
    is_last: bool,
    is_root: bool = False,
) -> None:
    """Recursively render a node and its children as a tree.

    Nodes that were already rendered (shared dependencies / fan-in) are
    shown as back-references with a ``(^)`` marker instead of expanding
    their subtree again.
    """
    trouve = dag.get_trouve(node)
    assert trouve.compiled is not None, f"{node} has not been compiled"
    type_tag = None
    if trouve.compiled.execution_type == ExecutionType.PANDAS:
        type_tag = ExecutionType.PANDAS.upper()
    elif trouve.compiled.execution_type == ExecutionType.SNOWFLAKE:
        type_tag = trouve.type.value.upper()
    else:
        raise ClairError(f"Unknown execution_type '{trouve.compiled.execution_type}' for {node}")
    is_matched = node in matched

    if is_root:
        node_prefix = ""
        connector = ""
    else:
        node_prefix = prefix
        connector = "└── " if is_last else "├── "

    if node in printed:
        lines.append(f"{node_prefix}{connector}{node}  [{type_tag}]  (^)")
        return

    marker = "  *" if is_matched else ""
    lines.append(f"{node_prefix}{connector}{node}  [{type_tag}]{marker}")
    printed.add(node)

    children = sorted(subgraph.successors(node))
    child_prefix = prefix if is_root else prefix + ("    " if is_last else "│   ")

    for i, child in enumerate(children):
        _render_subtree(
            dag, subgraph, child, matched, printed, lines,
            child_prefix, i == len(children) - 1,
        )


def _compute_visible_nodes(
    dag: ClairDag, selected: list[str] | None
) -> tuple[set[str], set[str]]:
    """Return (visible_nodes, matched_nodes).

    If *selected* is empty or None, all nodes are visible and matched is empty.
    """
    if not selected:
        return set(dag.nodes), set()

    matched: set[str] = set()
    for node in dag.nodes:
        for pattern in selected:
            if fnmatch(node, pattern):
                matched.add(node)
                break

    visible = set(matched)
    for node in matched:
        visible |= nx.ancestors(dag, node)

    return visible, matched


def _compute_depths(dag: ClairDag, visible: set[str]) -> dict[str, int]:  # noqa: F401
    """Return {node: depth} for all visible nodes.

    Depth is the length of the longest path from any root to the node
    within the visible subgraph.
    """
    subgraph = dag.subgraph(visible)
    depth: dict[str, int] = {}
    for node in nx.topological_sort(subgraph):
        preds = list(subgraph.predecessors(node))
        if not preds:
            depth[node] = 0
        else:
            depth[node] = max(depth[p] for p in preds) + 1
    return depth


def _format_header(n_models: int, n_sources: int, pattern: str | None) -> str:
    """Return the === header line ===."""
    model_word = "model" if n_models == 1 else "models"
    source_word = "source" if n_sources == 1 else "sources"

    if pattern is not None:
        return (
            f"=== Clair DAG (filtered: {pattern}): "
            f"{n_models} {model_word}, {n_sources} {source_word} ==="
        )
    return f"=== Clair DAG: {n_models} {model_word}, {n_sources} {source_word} ==="
