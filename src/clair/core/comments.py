"""Comments -- write the docs of a Trouve and of its columns to the warehouse.

``Trouve.docs`` becomes the comment of the object, and ``Column.docs`` becomes
the comment of the column. A person who reads the table in Snowflake, or a
catalog tool that reads INFORMATION_SCHEMA, then sees the same text as
``clair docs``.

The comments go onto the physical object after the promotion. A promotion of a
view is a ``CREATE OR REPLACE VIEW``, and it removes each comment that the
staging view had. A run with no staging writes the comments after the build.

Clair writes a comment only when the text is not empty, because a new object
has no comment. An incremental table is the exception: it keeps each comment
from the last run. An incremental Trouve thus writes each comment, also an
empty one, so that a docs text that you remove does not stay on the table. A
column that you remove from ``columns`` keeps its old comment.
"""

from __future__ import annotations

import inspect

from clair.environments.routing import TrouveAddress
from clair.trouves.run_config import RunMode
from clair.trouves.trouve import Table, TrouveAbc, TrouveType, table_options_of


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


def keeps_comments_between_runs(trouve: TrouveAbc) -> bool:
    """Tell you if the object of the Trouve can hold a comment from an earlier run.

    Only an incremental table survives a run. Each other object is new.
    """
    return isinstance(trouve, Table) and trouve.run_config.run_mode == RunMode.INCREMENTAL


def build_comment_statements(address: TrouveAddress, trouve: TrouveAbc) -> list[str]:
    """Give the statements that write the docs of a Trouve to the object at an address.

    Args:
        address: The physical address of the object.
        trouve: The Trouve that holds the docs.

    Returns:
        One statement for the object, then one statement for each column, or
        no statement when there is nothing to write.
    """
    assert trouve.type != TrouveType.SOURCE, "clair does not write to a SOURCE Trouve"
    write_empty_text = keeps_comments_between_runs(trouve)
    table_options = table_options_of(trouve)
    object_keyword = "VIEW" if table_options is None else table_options.object_keyword
    # A docs text is often a triple-quoted string. Clean it as Python cleans a docstring.
    trouve_docs = inspect.cleandoc(trouve.docs)

    statements: list[str] = []
    if trouve_docs or write_empty_text:
        statements.append(
            f"-- comment: write the docs of the Trouve\n"
            f"ALTER {object_keyword} {address} SET COMMENT = {quote_comment(trouve_docs)}"
        )
    for column in trouve.columns:
        column_docs = inspect.cleandoc(column.docs)
        if not column_docs and not write_empty_text:
            continue
        statements.append(
            f"-- comment: write the docs of the column {column.name}\n"
            f"COMMENT ON COLUMN {address}.{column.name} IS {quote_comment(column_docs)}"
        )
    return statements
