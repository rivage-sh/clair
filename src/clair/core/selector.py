"""Selector matching for --select glob patterns."""

from fnmatch import fnmatch


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
