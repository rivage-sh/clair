"""Column inference from SQL for clair docs.

Attempts to extract column names from a Trouve's resolved SQL when
the user hasn't explicitly declared columns. This is a best-effort
heuristic -- it handles common SELECT patterns but won't cover every
SQL edge case. When inference fails (e.g., ``SELECT *``), it returns
a clear reason so the frontend can display helpful guidance.
"""

from __future__ import annotations

import re
from enum import StrEnum

from pydantic import BaseModel

from clair.trouves.column import Column


class ColumnStatus(StrEnum):
    """How the column list for a Trouve was determined."""

    DECLARED = "declared"
    """User explicitly defined columns on the Trouve."""

    INFERRED = "inferred"
    """Columns were extracted from the SQL by heuristic parsing."""

    SELECT_STAR = "select_star"
    """SQL uses SELECT * -- columns depend on upstream and cannot be inferred offline."""

    NO_SQL = "no_sql"
    """Trouve has no SQL (e.g., a SOURCE). Columns must be declared."""

    PARSE_FAILED = "parse_failed"
    """SQL could not be parsed to extract columns."""


class ColumnInference(BaseModel):
    """Result of attempting to determine a Trouve's columns.

    Attributes:
        status: How the columns were determined (or why they couldn't be).
        columns: The column list -- either user-declared or inferred.
        message: Human-readable explanation of the status, for display in docs.
    """

    status: ColumnStatus
    columns: list[Column]
    message: str


# ── Regex patterns ──────────────────────────────────────────────────

# Matches SELECT ... FROM, handling multiline and common whitespace.
# Captures the projection list between SELECT and FROM.
_SELECT_PROJECTION_PATTERN = re.compile(
    r"\bSELECT\s+(DISTINCT\s+)?(.*?)\s+FROM\b",
    re.IGNORECASE | re.DOTALL,
)

# Matches a trailing column alias: ... AS alias_name
_ALIAS_PATTERN = re.compile(
    r"\bAS\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*$",
    re.IGNORECASE,
)


def infer_columns(
    declared_columns: list[Column],
    resolved_sql: str | None,
) -> ColumnInference:
    """Determine column information for a Trouve.

    Priority:
    1. If the user declared columns, use those (status = DECLARED).
    2. If there's no SQL (SOURCE), return NO_SQL.
    3. Try to parse columns from the SQL.
       - If SELECT * is detected, return SELECT_STAR with guidance.
       - Otherwise, extract aliased/named columns and return INFERRED.
       - If parsing fails entirely, return PARSE_FAILED.

    Args:
        declared_columns: The columns the user explicitly set on the Trouve.
        resolved_sql: The SQL after placeholder resolution (None for SOURCEs).

    Returns:
        A ColumnInference with the status, columns, and a display message.
    """
    if declared_columns:
        return ColumnInference(
            status=ColumnStatus.DECLARED,
            columns=declared_columns,
            message="",
        )

    if not resolved_sql or not resolved_sql.strip():
        return ColumnInference(
            status=ColumnStatus.NO_SQL,
            columns=[],
            message=(
                "This is a source trouve with no SQL. "
                "Add columns=[] to document its schema."
            ),
        )

    cleaned_sql = resolved_sql.strip()

    if _uses_select_star(cleaned_sql):
        return ColumnInference(
            status=ColumnStatus.SELECT_STAR,
            columns=[],
            message=(
                "This model uses SELECT * -- columns depend on the upstream "
                "source and cannot be inferred from SQL alone. Add explicit "
                "columns=[] to document them."
            ),
        )

    extracted_column_names = _extract_column_names(cleaned_sql)

    if extracted_column_names:
        inferred_columns = [
            Column(name=column_name, type="UNKNOWN")
            for column_name in extracted_column_names
        ]
        return ColumnInference(
            status=ColumnStatus.INFERRED,
            columns=inferred_columns,
            message="Columns inferred from SQL. Add explicit columns=[] for types and docs.",
        )

    return ColumnInference(
        status=ColumnStatus.PARSE_FAILED,
        columns=[],
        message=(
            "Could not infer columns from SQL. "
            "Add explicit columns=[] to document them."
        ),
    )


def _uses_select_star(sql: str) -> bool:
    """Detect whether a SQL statement uses SELECT * (with or without table prefix).

    Handles patterns like:
    - SELECT *
    - SELECT DISTINCT *
    - SELECT t.*
    - SELECT alias.*
    - SELECT *, count(*) (star in projection, not just inside functions)
    """
    match = _SELECT_PROJECTION_PATTERN.search(sql)
    if not match:
        return False

    projection = match.group(2).strip()

    # Split projection into individual expressions (respecting parentheses)
    expressions = _split_projection(projection)

    for expression in expressions:
        stripped_expression = expression.strip()
        # Bare star: *
        if stripped_expression == "*":
            return True
        # Qualified star: alias.* or table.*
        if re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*\.\*$", stripped_expression):
            return True

    return False


def _extract_column_names(sql: str) -> list[str]:
    """Extract column names from a SELECT statement's projection list.

    For each expression in the SELECT list:
    - If it has an AS alias, use the alias.
    - If it's a bare column reference (word), use that.
    - If it's a qualified reference (table.column), use the column part.
    - Otherwise skip it (complex expression without alias).

    Returns an ordered list of column names, or empty if parsing fails.
    """
    match = _SELECT_PROJECTION_PATTERN.search(sql)
    if not match:
        return []

    projection = match.group(2).strip()
    expressions = _split_projection(projection)
    column_names: list[str] = []

    for expression in expressions:
        stripped_expression = expression.strip()
        if not stripped_expression:
            continue

        column_name = _column_name_from_expression(stripped_expression)
        if column_name:
            column_names.append(column_name)

    return column_names


def _column_name_from_expression(expression: str) -> str | None:
    """Extract a column name from a single SELECT expression.

    Returns the name if determinable, None otherwise.
    """
    # Check for AS alias first
    alias_match = _ALIAS_PATTERN.search(expression)
    if alias_match:
        return alias_match.group(1).lower()

    # Bare column reference: just a name
    if re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", expression):
        return expression.lower()

    # Qualified reference: table.column
    qualified_match = re.match(
        r"^[a-zA-Z_][a-zA-Z0-9_]*\.([a-zA-Z_][a-zA-Z0-9_]*)$", expression
    )
    if qualified_match:
        return qualified_match.group(1).lower()

    return None


def _split_projection(projection: str) -> list[str]:
    """Split a SELECT projection into individual expressions.

    Respects parentheses so that ``count(*)`` or ``coalesce(a, b)``
    are not split on their internal commas.
    """
    expressions: list[str] = []
    current_expression: list[str] = []
    parenthesis_depth = 0

    for character in projection:
        if character == "(":
            parenthesis_depth += 1
            current_expression.append(character)
        elif character == ")":
            parenthesis_depth -= 1
            current_expression.append(character)
        elif character == "," and parenthesis_depth == 0:
            expressions.append("".join(current_expression))
            current_expression = []
        else:
            current_expression.append(character)

    # Don't forget the last expression
    if current_expression:
        expressions.append("".join(current_expression))

    return expressions
