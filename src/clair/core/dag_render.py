"""The DAG render code.

``build_dag_tree`` gives the data: a tree of :class:`DagTreeNode`, one node for
each place where a Trouve occurs below its parents. ``format_dag_tree`` makes
the text for stdout. The data holds the shape of the tree, thus a test reads the
shape and never reads the text.
"""

from __future__ import annotations

from collections.abc import Iterator
from enum import StrEnum
from fnmatch import fnmatch

import networkx as nx
from pydantic import BaseModel

from clair.core.dag import ClairDag
from clair.exceptions import ClairError
from clair.trouves.trouve import ExecutionType, TrouveType

# The text that marks a node that the tree already showed below another parent.
REPEAT_MARKER = "(^)"

# The text that marks a node that agrees with a selector pattern.
MATCH_MARKER = "*"


class DagNodeTag(StrEnum):
    """The tag in the square brackets after a node.

    A Trouve that Python executes shows PANDAS. Each other Trouve shows the
    Trouve type, because the type tells the reader what the SQL makes.
    """

    SOURCE = "SOURCE"
    TABLE = "TABLE"
    VIEW = "VIEW"
    PANDAS = "PANDAS"


def node_tag(trouve_type: TrouveType, execution_type: ExecutionType) -> DagNodeTag:
    """Give the tag of one Trouve."""
    if execution_type == ExecutionType.PANDAS:
        return DagNodeTag.PANDAS
    if execution_type != ExecutionType.SNOWFLAKE:
        raise ClairError(f"Clair does not know the execution type {execution_type}.")

    if trouve_type == TrouveType.SOURCE:
        return DagNodeTag.SOURCE
    elif trouve_type == TrouveType.TABLE:
        return DagNodeTag.TABLE
    elif trouve_type == TrouveType.VIEW:
        return DagNodeTag.VIEW
    else:
        raise ClairError(f"Clair does not know the Trouve type {trouve_type}.")


class DagTreeNode(BaseModel):
    """One Trouve, at one place in the tree.

    A Trouve that two parents read occurs two times or more. The first place
    holds the children, and each later place has ``is_repeat`` True and no
    child. Thus the tree stays finite and each subtree shows one time.
    """

    physical_address: str
    trouve_type: TrouveType
    execution_type: ExecutionType
    is_matched: bool
    is_repeat: bool
    children: list[DagTreeNode]

    @property
    def tag(self) -> DagNodeTag:
        """Give the tag in the square brackets."""
        return node_tag(self.trouve_type, self.execution_type)

    def walk(self, depth: int = 0) -> Iterator[tuple[int, DagTreeNode]]:
        """Give each node of this subtree, with its depth. The parent comes first."""
        yield depth, self
        for child in self.children:
            yield from child.walk(depth + 1)

    def find(self, physical_address: str) -> DagTreeNode | None:
        """Give the first node with this address, or None."""
        for _depth, node in self.walk():
            if node.physical_address == physical_address:
                return node
        return None


class DagRenderOutput(BaseModel):
    """The result after clair draws a DAG."""

    model_count: int
    source_count: int
    visible_nodes: list[str]
    matched_nodes: list[str]
    selector: str | None
    no_match: bool
    roots: list[DagTreeNode]

    def walk(self) -> Iterator[tuple[int, DagTreeNode]]:
        """Give each node of each root, with its depth."""
        for root in self.roots:
            yield from root.walk()

    def find(self, physical_address: str) -> DagTreeNode | None:
        """Give the first node with this address, or None."""
        for _depth, node in self.walk():
            if node.physical_address == physical_address:
                return node
        return None

    def depth_of(self, physical_address: str) -> int:
        """Give the depth of the first place where one address occurs.

        Raises:
            ClairError: If the tree holds no node with this address.
        """
        for depth, node in self.walk():
            if node.physical_address == physical_address:
                return depth
        raise ClairError(f"The tree holds no node for {physical_address}.")

    def render(self) -> str:
        """Give the complete tree text for stdout."""
        return format_dag_tree(self)


def render_dag(dag: ClairDag, selected: list[str] | None = None) -> DagRenderOutput:
    """Draw a ClairDag as a tree. The data flows from the top to the bottom.

    This is a pure function. It changes no state.

    Args:
        dag: The dependency graph to draw.
        selected: Optional glob patterns that limit the visible nodes. With
            these patterns, the tree shows only the nodes that agree with a
            pattern, and their parents at each level above.

    Returns:
        A DagRenderOutput that holds the counts, the tree, and a .render()
        method for the text.
    """
    selected = selected or []
    visible, matched = _compute_visible_nodes(dag, selected)
    pattern = selected[0] if selected else None

    if selected and not visible:
        return DagRenderOutput(
            model_count=0,
            source_count=0,
            visible_nodes=[],
            matched_nodes=[],
            selector=pattern,
            no_match=True,
            roots=[],
        )

    source_count = sum(
        1 for node in visible if dag.get_trouve(node).type == TrouveType.SOURCE
    )

    subgraph = dag.subgraph(visible)
    root_addresses = sorted(
        node for node in subgraph.nodes if subgraph.in_degree(node) == 0
    )

    built: set[str] = set()
    roots = [
        _build_subtree(dag, subgraph, root_address, matched, built)
        for root_address in root_addresses
    ]

    return DagRenderOutput(
        model_count=len(visible) - source_count,
        source_count=source_count,
        visible_nodes=sorted(visible),
        matched_nodes=sorted(matched),
        selector=pattern,
        no_match=False,
        roots=roots,
    )


def _build_subtree(
    dag: ClairDag,
    subgraph: nx.DiGraph,
    physical_address: str,
    matched: set[str],
    built: set[str],
) -> DagTreeNode:
    """Make the node at one place in the tree. This function calls itself."""
    trouve = dag.get_trouve(physical_address)
    if trouve.compiled is None:
        raise ClairError(f"Clair did not compile {physical_address}.")

    is_repeat = physical_address in built
    built.add(physical_address)

    children: list[DagTreeNode] = []
    if not is_repeat:
        children = [
            _build_subtree(dag, subgraph, child_address, matched, built)
            for child_address in sorted(subgraph.successors(physical_address))
        ]

    return DagTreeNode(
        physical_address=physical_address,
        trouve_type=trouve.type,
        execution_type=trouve.compiled.execution_type,
        is_matched=physical_address in matched,
        is_repeat=is_repeat,
        children=children,
    )


def format_dag_tree(output: DagRenderOutput) -> str:
    """Give the complete tree text of one DagRenderOutput."""
    if output.no_match:
        return f"Clair found no Trouves for the selector '{output.selector}'."

    lines = [
        format_header(output.model_count, output.source_count, output.selector),
        "",
    ]
    for index, root in enumerate(output.roots):
        if index > 0:
            lines.append("")
        _format_subtree(root, lines, prefix="", is_last=True, is_root=True)
    return "\n".join(lines)


def _format_subtree(
    node: DagTreeNode,
    lines: list[str],
    prefix: str,
    is_last: bool,
    is_root: bool = False,
) -> None:
    """Write the lines of one node and its children. This function calls itself."""
    if is_root:
        connector = ""
    elif is_last:
        connector = "└── "
    else:
        connector = "├── "

    node_prefix = "" if is_root else prefix
    if node.is_repeat:
        lines.append(
            f"{node_prefix}{connector}{node.physical_address}  "
            f"[{node.tag}]  {REPEAT_MARKER}"
        )
        return

    marker = f"  {MATCH_MARKER}" if node.is_matched else ""
    lines.append(f"{node_prefix}{connector}{node.physical_address}  [{node.tag}]{marker}")

    if is_root:
        child_prefix = prefix
    elif is_last:
        child_prefix = prefix + "    "
    else:
        child_prefix = prefix + "│   "

    for index, child in enumerate(node.children):
        _format_subtree(child, lines, child_prefix, index == len(node.children) - 1)


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


def format_header(model_count: int, source_count: int, pattern: str | None) -> str:
    """Give the === header line ===."""
    model_word = "model" if model_count == 1 else "models"
    source_word = "source" if source_count == 1 else "sources"

    if pattern is not None:
        return (
            f"=== Clair DAG (filtered: {pattern}): "
            f"{model_count} {model_word}, {source_count} {source_word} ==="
        )
    return f"=== Clair DAG: {model_count} {model_word}, {source_count} {source_word} ==="
