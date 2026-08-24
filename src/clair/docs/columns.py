"""Clair docs finds the columns in the SQL.

The code reads the column names from the resolved SQL of a Trouve when the user
declared no column. The code applies simple rules. It knows the usual SELECT
patterns, but it does not know every form of SQL. If the code finds no column,
for example with ``SELECT *``, it gives a clear reason. Thus the frontend can
show good advice to the user.
"""

from __future__ import annotations

import re
from enum import StrEnum

from pydantic import BaseModel

from clair.trouves.column import Column


class ColumnStatus(StrEnum):
    """The method that gave the column list of a Trouve."""

    DECLARED = "declared"
    """The user set the columns on the Trouve."""

    INFERRED = "inferred"
    """Clair read the columns from the SQL with its simple rules."""

    SELECT_STAR = "select_star"
    """The SQL contains SELECT *. The columns come from the upstream table."""

    NO_SQL = "no_sql"
    """The Trouve has no SQL, for example a SOURCE. The user must declare the columns."""

    PARSE_FAILED = "parse_failed"
    """Clair could not read the columns from the SQL."""


class ColumnInference(BaseModel):
    """The result after clair looks for the columns of a Trouve.

    Attributes:
        status: The method that gave the columns, or the reason for a failure.
        columns: The column list. The user declared it, or clair read it from
            the SQL.
        message: Text about the status for a person to read. The docs show it.
    """

    status: ColumnStatus
    columns: list[Column]
    message: str


# ── The regular expressions ──────────────────────────────────────────────────

# This pattern matches SELECT ... FROM. It accepts many lines and the usual
# space characters. It keeps the projection list between SELECT and FROM.
_SELECT_PROJECTION_PATTERN = re.compile(
    r"\bSELECT\s+(DISTINCT\s+)?(.*?)\s+FROM\b",
    re.IGNORECASE | re.DOTALL,
)

# This pattern matches a column alias at the end: ... AS alias_name
_ALIAS_PATTERN = re.compile(
    r"\bAS\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*$",
    re.IGNORECASE,
)


def infer_columns(
    declared_columns: list[Column],
    resolved_sql: str | None,
) -> ColumnInference:
    """Find the column data of a Trouve.

    The function obeys this order:
    1. If the user declared the columns, use them. The status is DECLARED.
    2. If there is no SQL, as for a SOURCE, give NO_SQL.
    3. If there is SQL, read the columns from it.
       - If the SQL contains SELECT *, give SELECT_STAR and some advice.
       - If not, read each column name or alias and give INFERRED.
       - If the code finds no column, give PARSE_FAILED.

    Args:
        declared_columns: The columns that the user set on the Trouve.
        resolved_sql: The SQL after clair replaced each placeholder. It is None
            for a SOURCE.

    Returns:
        A ColumnInference with the status, the columns, and a message.
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
                "This is a source Trouve with no SQL. "
                "Add columns=[] to the Trouve to show its columns here."
            ),
        )

    cleaned_sql = resolved_sql.strip()

    if _uses_select_star(cleaned_sql):
        return ColumnInference(
            status=ColumnStatus.SELECT_STAR,
            columns=[],
            message=(
                "This model uses SELECT *. The columns come from the upstream "
                "source, thus Clair cannot find them in the SQL. Add columns=[] "
                "to the Trouve to show them here."
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
            message="Clair found these columns in the SQL. Add columns=[] to the Trouve to give types and documentation.",
        )

    return ColumnInference(
        status=ColumnStatus.PARSE_FAILED,
        columns=[],
        message=(
            "Clair cannot find the columns in the SQL. "
            "Add columns=[] to the Trouve to show them here."
        ),
    )


def _uses_select_star(sql: str) -> bool:
    """Tell you if a SQL statement contains SELECT *, with or without a prefix.

    The function knows these patterns:
    - SELECT *
    - SELECT DISTINCT *
    - SELECT t.*
    - SELECT alias.*
    - SELECT *, count(*) — a star in the projection, not only in a function
    """
    match = _SELECT_PROJECTION_PATTERN.search(sql)
    if not match:
        return False

    projection = match.group(2).strip()

    # Cut the projection into separate expressions. Keep each parenthesis pair.
    expressions = _split_projection(projection)

    for expression in expressions:
        stripped_expression = expression.strip()
        # A star alone: *
        if stripped_expression == "*":
            return True
        # A star with a prefix: alias.* or table.*
        if re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*\.\*$", stripped_expression):
            return True

    return False


def _extract_column_names(sql: str) -> list[str]:
    """Read the column names from the projection list of a SELECT statement.

    The function examines each expression in the SELECT list:
    - If the expression has an AS alias, use the alias.
    - If the expression is one word, use that word.
    - If the expression is table.column, use the column part.
    - If not, skip the expression, because it has no alias.

    Returns the column names in order. The list is empty if the code finds no
    column.
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
    """Read the column name from one SELECT expression.

    Returns the name, or None if the code cannot find a name.
    """
    # Look for an AS alias first.
    alias_match = _ALIAS_PATTERN.search(expression)
    if alias_match:
        return alias_match.group(1).lower()

    # A column reference alone: only a name.
    if re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", expression):
        return expression.lower()

    # A column reference with a prefix: table.column
    qualified_match = re.match(
        r"^[a-zA-Z_][a-zA-Z0-9_]*\.([a-zA-Z_][a-zA-Z0-9_]*)$", expression
    )
    if qualified_match:
        return qualified_match.group(1).lower()

    return None


def _split_projection(projection: str) -> list[str]:
    """Cut a SELECT projection into separate expressions.

    The function keeps each parenthesis pair together. Thus it does not cut
    ``count(*)`` or ``coalesce(a, b)`` at an internal comma.
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

    # Keep the last expression.
    if current_expression:
        expressions.append("".join(current_expression))

    return expressions
