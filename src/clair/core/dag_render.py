"""The DAG render code. A pure function draws a ClairDag as a tree."""

from __future__ import annotations

from fnmatch import fnmatch

import networkx as nx
from pydantic import BaseModel, PrivateAttr

from clair.core.dag import ClairDag
from clair.exceptions import ClairError
from clair.trouves.trouve import ExecutionType, TrouveType


class DagRenderOutput(BaseModel):
    """The result after clair draws a DAG."""

    model_count: int
    source_count: int
    visible_nodes: list[str]
    matched_nodes: list[str]
    selector: str | None
    no_match: bool

    # An internal field: the tree text. The constructor makes the text. Thus
    # .render() gives the same bytes as the initial code gave.
    _rendered: str = PrivateAttr(default="")

    def render(self) -> str:
        """Give the complete tree text for stdout."""
        return self._rendered


def render_dag(dag: ClairDag, selected: list[str] | None = None) -> DagRenderOutput:
    """Draw a ClairDag as a tree. The data flows from the top to the bottom.

    This is a pure function. It changes no state. It gives a DagRenderOutput
    that holds the data and supplies a ``.render()`` method for the tree text.

    Args:
        dag: The dependency graph to draw.
        selected: Optional glob patterns that limit the visible nodes. With
            these patterns, the tree shows only the nodes that agree with a
            pattern, and their parents at each level above.

    Returns:
        A DagRenderOutput with the data fields and a .render() method.
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
    """Draw a node and its children as a tree. This function calls itself.

    A node can occur two times or more, because more than one node depends on
    it. The second time, the tree shows only a reference with a ``(^)`` marker,
    and the function does not draw the subtree again.
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
    """Give a (visible_nodes, matched_nodes) tuple.

    If *selected* is empty or None, each node is visible and matched_nodes is
    empty.
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


def _compute_depths(dag: ClairDag, visible: set[str]) -> dict[str, int]:
    """Give a {node: depth} map for each visible node.

    The depth is the length of the longest path from a root to the node in the
    visible subgraph.
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
    """Give the === header line ===."""
    model_word = "model" if n_models == 1 else "models"
    source_word = "source" if n_sources == 1 else "sources"

    if pattern is not None:
        return (
            f"=== Clair DAG (filtered: {pattern}): "
            f"{n_models} {model_word}, {n_sources} {source_word} ==="
        )
    return f"=== Clair DAG: {n_models} {model_word}, {n_sources} {source_word} ==="
