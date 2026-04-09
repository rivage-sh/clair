"""Selector matching for --select / --exclude glob patterns.

Supports plain glob patterns as well as dbt-style graph traversal operators:

    +pattern          all upstream ancestors (unlimited depth)
    pattern+          all downstream descendants (unlimited depth)
    +pattern+         both directions
    N+pattern         upstream ancestors up to N levels
    pattern+N         downstream descendants up to N levels
    N+pattern+M       N levels upstream, M levels downstream

Examples:
    mydb.analytics.*        glob only — no traversal
    +mydb.analytics.orders  orders + all its upstream dependencies
    mydb.analytics.orders+  orders + everything downstream of it
    2+mydb.analytics.orders orders + up to 2 levels of upstream parents
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from fnmatch import fnmatch

import networkx as nx


@dataclass(frozen=True)
class ParsedSelector:
    """The result of parsing a selector pattern.

    upstream_depth / downstream_depth:
        None  — no traversal in that direction
        0     — unlimited depth
        n > 0 — exactly n levels
    """

    glob: str
    upstream_depth: int | None
    downstream_depth: int | None


def match_selector(full_name: str, pattern: str) -> bool:
    """Match a Trouve full_name against a glob-style selector pattern.

    Uses fnmatch semantics on the dotted full_name.

    Examples:
        match_selector("mydb.analytics.orders", "mydb.*.orders") -> True
        match_selector("mydb.analytics.orders", "mydb.analytics.*") -> True
        match_selector("mydb.analytics.orders", "mydb.analytics.orders") -> True
        match_selector("mydb.staging.users", "mydb.analytics.*") -> False
    """
    return fnmatch(full_name, pattern)


def filter_by_selector(full_names: list[str], pattern: str | None) -> list[str]:
    """Filter a list of full_names by glob pattern.

    If pattern is None, returns all names unchanged.
    """
    if pattern is None:
        return full_names
    return [name for name in full_names if match_selector(name, pattern)]


def filter_by_selectors(full_names: list[str], patterns: tuple[str, ...] | None) -> list[str]:
    """Filter a list of full_names by multiple glob patterns (union).

    If patterns is None or an empty tuple, returns all names unchanged.
    Names that match ANY pattern are included; original order is preserved.
    """
    if not patterns:
        return full_names
    return [name for name in full_names if any(match_selector(name, pattern) for pattern in patterns)]


def parse_selector(pattern: str) -> ParsedSelector:
    """Parse a selector pattern into a ParsedSelector.

    Examples:
        "mydb.analytics.*"      -> ParsedSelector(glob="mydb.analytics.*", upstream_depth=None, downstream_depth=None)
        "+mydb.analytics.*"     -> ParsedSelector(glob="mydb.analytics.*", upstream_depth=0,    downstream_depth=None)
        "mydb.analytics.*+"     -> ParsedSelector(glob="mydb.analytics.*", upstream_depth=None, downstream_depth=0)
        "+mydb.analytics.*+"    -> ParsedSelector(glob="mydb.analytics.*", upstream_depth=0,    downstream_depth=0)
        "2+mydb.analytics.*"    -> ParsedSelector(glob="mydb.analytics.*", upstream_depth=2,    downstream_depth=None)
        "mydb.analytics.*+3"    -> ParsedSelector(glob="mydb.analytics.*", upstream_depth=None, downstream_depth=3)
        "2+mydb.analytics.*+3"  -> ParsedSelector(glob="mydb.analytics.*", upstream_depth=2,    downstream_depth=3)
    """
    # Both sides: [N+]glob[+M]
    match = re.match(r'^(\d*)\+(.+)\+(\d*)$', pattern)
    if match:
        left, glob, right = match.groups()
        return ParsedSelector(
            glob=glob,
            upstream_depth=int(left) if left else 0,
            downstream_depth=int(right) if right else 0,
        )

    # Left only: [N+]glob
    match = re.match(r'^(\d*)\+(.+)$', pattern)
    if match:
        left, glob = match.groups()
        return ParsedSelector(glob=glob, upstream_depth=int(left) if left else 0, downstream_depth=None)

    # Right only: glob[+N]
    match = re.match(r'^(.+)\+(\d*)$', pattern)
    if match:
        glob, right = match.groups()
        return ParsedSelector(glob=glob, upstream_depth=None, downstream_depth=int(right) if right else 0)

    return ParsedSelector(glob=pattern, upstream_depth=None, downstream_depth=None)


def _traverse_upstream(dag: nx.DiGraph, start_nodes: set[str], depth: int) -> set[str]:
    """Return ancestors of start_nodes up to `depth` levels (0 = unlimited).

    Does not include start_nodes themselves in the result.
    """
    if depth == 0:
        ancestors: set[str] = set()
        for node in start_nodes:
            ancestors |= nx.ancestors(dag, node)
        return ancestors

    visited = set(start_nodes)
    frontier = set(start_nodes)
    for _ in range(depth):
        next_frontier: set[str] = set()
        for node in frontier:
            for predecessor in dag.predecessors(node):
                if predecessor not in visited:
                    next_frontier.add(predecessor)
        if not next_frontier:
            break
        visited |= next_frontier
        frontier = next_frontier
    return visited - start_nodes


def _traverse_downstream(dag: nx.DiGraph, start_nodes: set[str], depth: int) -> set[str]:
    """Return descendants of start_nodes up to `depth` levels (0 = unlimited).

    Does not include start_nodes themselves in the result.
    """
    if depth == 0:
        descendants: set[str] = set()
        for node in start_nodes:
            descendants |= nx.descendants(dag, node)
        return descendants

    visited = set(start_nodes)
    frontier = set(start_nodes)
    for _ in range(depth):
        next_frontier: set[str] = set()
        for node in frontier:
            for successor in dag.successors(node):
                if successor not in visited:
                    next_frontier.add(successor)
        if not next_frontier:
            break
        visited |= next_frontier
        frontier = next_frontier
    return visited - start_nodes


def expand_selector(dag: nx.DiGraph, pattern: str) -> set[str]:
    """Expand a single selector pattern (may include + operators) against the DAG.

    Returns the set of matching node full_names.
    """
    parsed = parse_selector(pattern)

    matched = {node for node in dag.nodes if fnmatch(node, parsed.glob)}

    extra: set[str] = set()
    if parsed.upstream_depth is not None:
        extra |= _traverse_upstream(dag, matched, parsed.upstream_depth)
    if parsed.downstream_depth is not None:
        extra |= _traverse_downstream(dag, matched, parsed.downstream_depth)

    return matched | extra


def expand_selectors(dag: nx.DiGraph, patterns: tuple[str, ...] | None) -> list[str]:
    """Expand multiple selector patterns against the DAG (union semantics).

    Supports plain globs and + operators. If patterns is None or empty, returns
    all nodes in topological order.

    Returns nodes in topological order (dependencies before dependents).
    """
    if not patterns:
        return list(nx.topological_sort(dag))

    selected: set[str] = set()
    for pattern in patterns:
        selected |= expand_selector(dag, pattern)

    return [node for node in nx.topological_sort(dag) if node in selected]
