"""Find each Trouve address that an author writes as text, and not as a reference.

An author points to a different Trouve with an f-string::

    from mydb.refined.events import trouve as refined_events
    trouve = Trouve(type=TrouveType.TABLE, sql=f"SELECT * FROM {refined_events}")

The f-string makes a placeholder token, and clair replaces the token with an
address. See ``clair.core.discovery``.

An author who writes the same address as text gets no token::

    trouve = Trouve(type=TrouveType.TABLE, sql="SELECT * FROM mydb.refined.events")

Two faults follow, and both are silent:

* The text makes no DAG edge. Clair can build this Trouve before the Trouve
  that it reads.
* Routing does not move the text. In a dev environment this Trouve reads the
  production table, and each Trouve beside it reads the dev table.

This module finds that text. It reads the SQL of the author, which keeps the
tokens, and it examines the syntax tree. Thus an address in a comment or in a
string literal is not a fault: only a true table name is.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlglot
import structlog
from pydantic import BaseModel
from sqlglot import exp

from clair.trouves.test import TestSql
from clair.trouves.trouve import Trouve, TrouveAbc

logger = structlog.get_logger()

# clair is a Snowflake framework, thus the parser reads Snowflake SQL.
_DIALECT = "snowflake"

# A logical address has three parts: database_name.schema_name.table_name. A
# name with fewer parts cannot be a Trouve, thus this module ignores it. A CTE
# name has one part, and a placeholder token has one part too.
_ADDRESS_PART_COUNT = 3


class TextReference(BaseModel):
    """One address that a Trouve names as text.

    Attributes:
        logical_address: The Trouve that holds the text.
        text_address: The address that the author wrote as text.
        location: Where the text is: "sql" or "test sql".
    """

    logical_address: str
    text_address: str
    location: str


def _table_names(sql: str) -> list[str]:
    """Give the name of each table in the SQL, in upper case.

    The function reads the syntax tree, thus it gives a name that the SQL uses
    as a table. A name in a comment or in a string literal is not a table name,
    and the tree does not hold it.

    Clair does not own the SQL syntax, Snowflake does. Thus SQL that the parser
    cannot read gives an empty list, and not an error.
    """
    if not sql.strip():
        return []
    try:
        statements = sqlglot.parse(sql, dialect=_DIALECT)
    except Exception as parse_error:  # noqa: BLE001 — the author SQL is unknown; Snowflake reports a syntax fault
        logger.debug("text_references.parse_error", error=str(parse_error))
        return []

    names: list[str] = []
    for statement in statements:
        if statement is None:
            continue
        for table in statement.find_all(exp.Table):
            parts = [part.name for part in table.parts]
            if len(parts) == _ADDRESS_PART_COUNT:
                names.append(".".join(parts).upper())
    return names


def find_text_references(trouves: Sequence[TrouveAbc]) -> list[TextReference]:
    """Give each address that a Trouve names as text, and not as a reference.

    The function examines the SQL of each Trouve and the SQL of each TestSql. It
    reports a name only when that name is the logical address of a Trouve in
    this project. A name of a table that clair does not hold is correct SQL, and
    the function ignores it.

    Args:
        trouves: Each Trouve from discover_project().

    Returns:
        One TextReference for each fault, in discovery order.
    """
    # Snowflake reads an unquoted name without case sensitivity. Thus the key is
    # the upper case name, and the value keeps the form that the project uses.
    logical_addresses = {
        str(trouve.compiled.logical_address).upper(): str(trouve.compiled.logical_address)
        for trouve in trouves
        if trouve.compiled
    }

    references: list[TextReference] = []
    for trouve in trouves:
        if not trouve.compiled:
            continue
        own_address = str(trouve.compiled.logical_address)

        sources: list[tuple[str, str]] = []
        if isinstance(trouve, Trouve):
            sources.append(("sql", trouve.sql))
        for test in trouve.tests:
            if isinstance(test, TestSql):
                sources.append(("test sql", test.sql))

        for location, sql in sources:
            for name in _table_names(sql):
                # A Trouve that names itself as text is the same fault. The THIS
                # marker gives the correct address for the run.
                if name in logical_addresses:
                    references.append(
                        TextReference(
                            logical_address=own_address,
                            text_address=logical_addresses[name],
                            location=location,
                        )
                    )
    return references
