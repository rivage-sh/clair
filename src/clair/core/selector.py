"""Clair matches the glob patterns of --select and --exclude.

A pattern can be a plain glob. A pattern can also contain a graph operator, as
in dbt:

    +pattern          each upstream parent, at any distance
    pattern+          each downstream child, at any distance
    +pattern+         both directions
    N+pattern         each upstream parent, to a distance of N levels
    pattern+N         each downstream child, to a distance of N levels
    N+pattern+M       N levels upstream and M levels downstream

Examples:
    mydb.analytics.*        only a glob, with no movement in the graph
    +mydb.analytics.orders  orders and each of its upstream dependencies
    mydb.analytics.orders+  orders and each node downstream of it
    2+mydb.analytics.orders orders and its parents, to a distance of 2 levels
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from fnmatch import fnmatch

import networkx as nx


@dataclass(frozen=True)
class ParsedSelector:
    """The result after clair reads a selector pattern.

    The upstream_depth field and the downstream_depth field have these values:
        None  — clair does not move in that direction
        0     — no limit on the distance
        n > 0 — exactly n levels
    """

    glob: str
    upstream_depth: int | None
    downstream_depth: int | None


def match_selector(physical_address: str, pattern: str) -> bool:
    """Compare the physical_address of a Trouve with a glob selector pattern.

    The function applies the fnmatch rules to the dotted physical_address.

    Examples:
        match_selector("mydb.analytics.orders", "mydb.*.orders") -> True
        match_selector("mydb.analytics.orders", "mydb.analytics.*") -> True
        match_selector("mydb.analytics.orders", "mydb.analytics.orders") -> True
        match_selector("mydb.staging.users", "mydb.analytics.*") -> False
    """
    return fnmatch(physical_address, pattern)


def filter_by_selector(addresses: list[str], pattern: str | None) -> list[str]:
    """Keep each physical_address that agrees with the glob pattern.

    If the pattern is None, the function gives each name, with no change.
    """
    if pattern is None:
        return addresses
    return [name for name in addresses if match_selector(name, pattern)]


def filter_by_selectors(addresses: list[str], patterns: tuple[str, ...] | None) -> list[str]:
    """Keep each physical_address that agrees with one or more glob patterns.

    If the patterns argument is None or an empty tuple, the function gives each
    name, with no change. A name that agrees with one pattern is sufficient. The
    function keeps the initial order of the names.
    """
    if not patterns:
        return addresses
    return [name for name in addresses if any(match_selector(name, pattern) for pattern in patterns)]


def parse_selector(pattern: str) -> ParsedSelector:
    """Read a selector pattern and make a ParsedSelector.

    Examples:
        "mydb.analytics.*"      -> ParsedSelector(glob="mydb.analytics.*", upstream_depth=None, downstream_depth=None)
        "+mydb.analytics.*"     -> ParsedSelector(glob="mydb.analytics.*", upstream_depth=0,    downstream_depth=None)
        "mydb.analytics.*+"     -> ParsedSelector(glob="mydb.analytics.*", upstream_depth=None, downstream_depth=0)
        "+mydb.analytics.*+"    -> ParsedSelector(glob="mydb.analytics.*", upstream_depth=0,    downstream_depth=0)
        "2+mydb.analytics.*"    -> ParsedSelector(glob="mydb.analytics.*", upstream_depth=2,    downstream_depth=None)
        "mydb.analytics.*+3"    -> ParsedSelector(glob="mydb.analytics.*", upstream_depth=None, downstream_depth=3)
        "2+mydb.analytics.*+3"  -> ParsedSelector(glob="mydb.analytics.*", upstream_depth=2,    downstream_depth=3)
    """
    # Operators on both sides: [N+]glob[+M]
    match = re.match(r'^(\d*)\+(.+)\+(\d*)$', pattern)
    if match:
        left, glob, right = match.groups()
        return ParsedSelector(
            glob=glob,
            upstream_depth=int(left) if left else 0,
            downstream_depth=int(right) if right else 0,
        )

    # An operator on the left side only: [N+]glob
    match = re.match(r'^(\d*)\+(.+)$', pattern)
    if match:
        left, glob = match.groups()
        return ParsedSelector(glob=glob, upstream_depth=int(left) if left else 0, downstream_depth=None)

    # An operator on the right side only: glob[+N]
    match = re.match(r'^(.+)\+(\d*)$', pattern)
    if match:
        glob, right = match.groups()
        return ParsedSelector(glob=glob, upstream_depth=None, downstream_depth=int(right) if right else 0)

    return ParsedSelector(glob=pattern, upstream_depth=None, downstream_depth=None)


def _traverse_upstream(dag: nx.DiGraph, start_nodes: set[str], depth: int) -> set[str]:
    """Give the parents of start_nodes, to a distance of `depth` levels.

    A `depth` of 0 puts no limit on the distance. The result does not contain
    the start_nodes.
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
    """Give the children of start_nodes, to a distance of `depth` levels.

    A `depth` of 0 puts no limit on the distance. The result does not contain
    the start_nodes.
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
    """Apply one selector pattern to the DAG. The pattern can contain a + operator.

    Returns the set of addresses that agree with the pattern.
    """
    parsed = parse_selector(pattern)

    matched = {node for node in dag.nodes if fnmatch(node, parsed.glob)}

    extra: set[str] = set()
    if parsed.upstream_depth is not None:
        extra |= _traverse_upstream(dag, matched, parsed.upstream_depth)
    if parsed.downstream_depth is not None:
        extra |= _traverse_downstream(dag, matched, parsed.downstream_depth)

    return matched | extra


def expand_selectors(dag: nx.DiGraph, patterns: Sequence[str] | None) -> list[str]:
    """Apply many selector patterns to the DAG and join the results.

    A pattern can be a plain glob or contain a + operator. If the patterns
    argument is None or empty, the function gives each node.

    Returns the nodes in topological order. Each dependency comes first.
    """
    if not patterns:
        return list(nx.topological_sort(dag))

    selected: set[str] = set()
    for pattern in patterns:
        selected |= expand_selector(dag, pattern)

    return [node for node in nx.topological_sort(dag) if node in selected]
