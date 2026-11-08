"""Comments -- write the docs of a Trouve and of its columns to the warehouse.

``Trouve.docs`` becomes the comment of the object, and ``Column.docs`` becomes
the comment of the column. A person who reads the table in Snowflake, or a
catalog tool that reads INFORMATION_SCHEMA, then sees the same text as
``clair docs``.

The comments go onto the physical object after the promotion. A promotion of a
view is a ``CREATE OR REPLACE VIEW``, and it removes each comment that the
staging view had. A run with no staging writes the comments after the build.

A new object has no comment, thus clair writes each text that is not empty. An
incremental table keeps its comments from the last run. The runner already
reads that table, for its type and for its columns, and the two answers hold
the comments. Clair compares them with the docs, and writes only a comment that
differs. An unchanged incremental Trouve thus gets no comment statement. A
column that you remove from ``columns`` keeps its old comment.

The comments of the columns of a table go into one ALTER. A view gets one
``ALTER VIEW ... ALTER COLUMN`` for each column: ``COMMENT ON COLUMN`` accepts
a table only, and ALTER VIEW accepts one column in each statement.
"""

from __future__ import annotations

import inspect

from pydantic import BaseModel

from clair.adapters.base import WarehouseColumn, WarehouseObject
from clair.environments.routing import TrouveAddress
from clair.trouves.trouve import TrouveAbc, TrouveType, table_options_of


class WarehouseComments(BaseModel):
    """The comments that an object in the warehouse holds now.

    Attributes:
        object_comment: The comment of the object.
        column_comments: The comment of each column, by the name that the
            warehouse stores.
    """

    object_comment: str
    column_comments: dict[str, str]

    @classmethod
    def of(
        cls, warehouse_object: WarehouseObject, warehouse_columns: list[WarehouseColumn]
    ) -> WarehouseComments:
        """Collect the comments from the answers that the runner already has."""
        return cls(
            object_comment=warehouse_object.comment,
            column_comments={column.name: column.comment for column in warehouse_columns},
        )

    def column_comment(self, column_name: str) -> str:
        """Give the comment of a declared column, or an empty text for a new column.

        Snowflake stores an unquoted name in upper case, thus the match ignores
        the case.
        """
        for warehouse_name, comment in self.column_comments.items():
            if warehouse_name.upper() == column_name.upper():
                return comment
        return ""


def quote_comment(text: str) -> str:
    """Give the text as a Snowflake string literal on one line.

    A backslash starts an escape sequence in a Snowflake string, thus the
    backslash and the quote get a backslash. A line break becomes ``\\n``, so
    each statement stays on one line, and an error message can quote it.
    """
    escaped_text = (
        text.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )
    return f"'{escaped_text}'"


def build_comment_statements(
    address: TrouveAddress,
    trouve: TrouveAbc,
    current_comments: WarehouseComments | None = None,
) -> list[str]:
    """Give the statements that write the docs of a Trouve to the object at an address.

    Args:
        address: The physical address of the object.
        trouve: The Trouve that holds the docs.
        current_comments: The comments that the object holds now. None means a
            new object, which holds no comment.

    Returns:
        The statement for the object, then the statements for the columns.
        Each text that the object already holds gets no statement.
    """
    assert trouve.type != TrouveType.SOURCE, "clair does not write to a SOURCE Trouve"
    table_options = table_options_of(trouve)

    # A docs text is often a triple-quoted string. Clean it as Python cleans a docstring.
    trouve_docs = inspect.cleandoc(trouve.docs)
    current_object_comment = "" if current_comments is None else current_comments.object_comment

    changed_columns: list[tuple[str, str]] = []
    for column in trouve.columns:
        column_docs = inspect.cleandoc(column.docs)
        current_column_comment = (
            "" if current_comments is None else current_comments.column_comment(column.name)
        )
        if column_docs != current_column_comment:
            changed_columns.append((column.name, column_docs))

    statements: list[str] = []
    if trouve_docs != current_object_comment:
        object_keyword = "VIEW" if table_options is None else table_options.object_keyword
        statements.append(
            f"-- comment: write the docs of the Trouve\n"
            f"ALTER {object_keyword} {address} SET COMMENT = {quote_comment(trouve_docs)}"
        )
    if not changed_columns:
        return statements
    if table_options is None:
        statements.extend(
            f"-- comment: write the docs of the column {column_name}\n"
            f"ALTER VIEW {address} ALTER COLUMN {column_name} COMMENT {quote_comment(column_docs)}"
            for column_name, column_docs in changed_columns
        )
        return statements
    column_clauses = ", ".join(
        f"COLUMN {column_name} COMMENT {quote_comment(column_docs)}"
        for column_name, column_docs in changed_columns
    )
    statements.append(
        f"-- comment: write the docs of the columns\n"
        f"ALTER {table_options.object_keyword} {address} ALTER {column_clauses}"
    )
    return statements
