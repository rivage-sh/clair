"""The SQL text of each step of the lifecycle of a Trouve.

Each function here gives SQL text, and nothing else: it opens no connection, and
it reads no Trouve. The phase methods of a Trouve call these functions with the
options of that Trouve, and an override of a phase can call them too. See
``clair.trouves.plan`` for the phases.

This module imports nothing from ``clair.trouves.trouve``, thus ``trouve.py``
can import it.

Staging. Snowflake can test a table only after the table exists, thus clair
writes each Trouve to ``<table>__clair_<run_id>`` first, tests it there, and
then promotes it. An incremental Trouve first gets a zero-copy clone of the
physical table, because the incremental statements need a base. The default
promotion is ``CREATE OR REPLACE TABLE <physical> CLONE <staging> COPY GRANTS``.
Snowflake does this in the metadata, so the time does not grow with the size
of the table. A Snowflake feature that attaches to the object, such as Time
Travel or a stream, starts again at each promotion, because the promotion makes
a new object.

A TRANSIENT table goes through the same steps, and each CREATE says TRANSIENT.
Snowflake cannot clone a transient table into a permanent one, thus the staging
object and the physical object must be the same kind.

A clone copies the clustering key, but Snowflake suspends Automatic Clustering
on the clone. A table with a ``cluster_by`` thus gets its key on the staging
object after the build, and ``RESUME RECLUSTER`` after the promotion.

Comments. ``Trouve.docs`` becomes the comment of the object, and ``Column.docs``
the comment of the column. They go onto the physical object after the
promotion, because a promotion of a view is a ``CREATE OR REPLACE VIEW``, and it
removes each comment. An incremental table keeps its comments from the last
run, thus clair writes only a comment that differs. The comments of the columns
of a table go into one ALTER. A view gets one ``ALTER VIEW ... ALTER COLUMN``
for each column: ``COMMENT ON COLUMN`` accepts a table only, and ALTER VIEW
accepts one column in each statement.
"""

from __future__ import annotations

import inspect

from pydantic import BaseModel

from clair.adapters.objects import WarehouseColumn, WarehouseObject, WarehouseObjectType
from clair.trouves.address import TrouveAddress
from clair.trouves.column import Column


def quote_column_name(column_name: str) -> str:
    """Give the column name as a quoted identifier.

    The warehouse gives each name as it stores it, for example ``ORDER_DATE``.
    A quoted identifier matches that name exactly, also when the name is a
    reserved word or holds lower case letters.
    """
    escaped_name = column_name.replace('"', '""')
    return f'"{escaped_name}"'


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


def clone_statement(
    source_address: TrouveAddress, clone_address: TrouveAddress, *, table_keyword: str
) -> str:
    """Give the zero-copy clone that gives the staging table the rows of the physical table.

    A permanent table clones into a transient one, but a transient table does
    not clone into a permanent one. Thus a Trouve that becomes transient keeps
    its rows, and a Trouve that becomes permanent fails here, before it writes
    anything.

    Args:
        source_address: The table to copy.
        clone_address: The table that the clone makes.
        table_keyword: ``TABLE``, ``TRANSIENT TABLE`` or ``ICEBERG TABLE``.
    """
    return (
        f"-- staging: clone the target, so the incremental statements have a base\n"
        f"CREATE OR REPLACE {table_keyword} {clone_address} CLONE {source_address}"
    )


def add_column_statements(
    address: TrouveAddress, added_columns: list[WarehouseColumn], *, object_keyword: str
) -> list[str]:
    """Give one ALTER for each column that the SQL adds.

    Args:
        address: The table that gets the columns.
        added_columns: The columns that the SQL gives and the table lacks.
        object_keyword: ``TABLE`` or ``ICEBERG TABLE``.
    """
    return [
        f"-- add the new column {column.name}\n"
        f"ALTER {object_keyword} {address} "
        f"ADD COLUMN {quote_column_name(column.name)} {column.data_type}"
        for column in added_columns
    ]


def cluster_by_statements(
    address: TrouveAddress, cluster_by: list[str], *, object_keyword: str, incremental: bool
) -> list[str]:
    """Give the statement that sets the clustering key of a table after the build.

    The key goes on after the build, as in dbt-snowflake: the Snowflake
    documentation asks for column definitions in a CREATE TABLE AS SELECT that
    clusters, and clair does not know the columns. The key also replaces the key
    that an incremental clone copies from the physical table.

    An incremental run writes into a table that already exists, and that table
    can hold a key that the Trouve no longer declares. ``clair compile`` cannot
    see the key of a table, thus an incremental run with no ``cluster_by``
    drops the key each time. Snowflake accepts the drop on a table with no key.

    Args:
        address: The table that gets the key.
        cluster_by: The parts of the key. An empty list gives no key.
        object_keyword: ``TABLE`` or ``ICEBERG TABLE``.
        incremental: True when the table existed before this run.

    Returns:
        One statement, or no statement for a new table with no key.
    """
    if not cluster_by:
        if not incremental:
            return []
        return [
            (
                f"-- cluster: drop a clustering key that the Trouve no longer declares\n"
                f"ALTER {object_keyword} {address} DROP CLUSTERING KEY"
            )
        ]
    cluster_key = ", ".join(cluster_by)
    return [
        (
            f"-- cluster: set the clustering key\n"
            f"ALTER {object_keyword} {address} CLUSTER BY ({cluster_key})"
        )
    ]


def clone_promote_statements(
    staging_address: TrouveAddress,
    physical_address: TrouveAddress,
    *,
    table_keyword: str,
    object_keyword: str,
    clustered: bool,
) -> list[str]:
    """Give the statements that replace the physical table with a clone of the tested table.

    ``COPY GRANTS`` makes this operation safe against a production object.
    Without it clair removes each privilege that an administrator granted on
    the target, because Snowflake attaches a privilege to the object and not to
    the name. An ``ALTER TABLE ... SWAP WITH`` moves the grants to the staging
    name and leaves the physical address bare. With ``COPY GRANTS`` Snowflake
    copies each privilege except OWNERSHIP from the object that it replaces. If
    the physical object does not exist, Snowflake copies from the clone source.

    OWNERSHIP is the exception. It goes to the role that runs clair. ``SWAP``
    has the same result, so this is not a new fault, but a target that a
    different role owns changes owner.

    Args:
        staging_address: The table that holds the tested data.
        physical_address: The address that the table must have.
        table_keyword: ``TABLE``, ``TRANSIENT TABLE`` or ``ICEBERG TABLE``.
        object_keyword: ``TABLE`` or ``ICEBERG TABLE``.
        clustered: True when the table has a clustering key.

    Returns:
        The clone first. A clustered table then gets a ``RESUME RECLUSTER``,
        because Snowflake suspends Automatic Clustering on each clone.
    """
    statements = [
        (
            f"-- staging: promote the tested table\n"
            f"CREATE OR REPLACE {table_keyword} {physical_address} "
            f"CLONE {staging_address} COPY GRANTS"
        )
    ]
    if clustered:
        statements.append(
            f"-- cluster: the clone starts with Automatic Clustering suspended\n"
            f"ALTER {object_keyword} {physical_address} RESUME RECLUSTER"
        )
    return statements


def view_promote_statement(physical_address: TrouveAddress, resolved_sql: str) -> str:
    """Give the statement that makes the tested view at the physical address.

    Snowflake cannot clone a view into position. But CREATE OR REPLACE VIEW is
    atomic and it operates in the metadata. The staging view showed that the
    SQL is correct and that the results pass the tests.
    """
    return (
        f"-- staging: promote the tested view\n"
        f"CREATE OR REPLACE VIEW {physical_address} COPY GRANTS AS (\n"
        f"{resolved_sql.strip()}\n)"
    )


def drop_statement(address: TrouveAddress, *, object_keyword: str) -> str:
    """Give the statement that drops the staging object after clair promotes it.

    The default plan drops a staging object only after a promotion. A staging
    object that a failed build or a failed test leaves stays: it is the only
    copy of the rejected candidate, and a new copy needs a new run of each
    upstream Trouve.

    Args:
        address: The staging object.
        object_keyword: ``TABLE`` or ``VIEW``.
    """
    return (
        f"-- staging: drop the staging object that clair promoted\n"
        f"DROP {object_keyword} IF EXISTS {address}"
    )


def comment_statements(
    address: TrouveAddress,
    docs: str,
    columns: list[Column],
    current_comments: WarehouseComments | None,
    *,
    object_keyword: str,
) -> list[str]:
    """Give the statements that write the docs of a Trouve to the object at an address.

    Args:
        address: The physical address of the object.
        docs: The docs of the Trouve.
        columns: The columns of the Trouve, with their docs.
        current_comments: The comments that the object holds now. None means a
            new object, which holds no comment.
        object_keyword: ``TABLE``, ``ICEBERG TABLE`` or ``VIEW``.

    Returns:
        The statement for the object, then the statements for the columns.
        Each text that the object already holds gets no statement.
    """
    # A docs text is often a triple-quoted string. Clean it as Python cleans a docstring.
    trouve_docs = inspect.cleandoc(docs)
    current_object_comment = "" if current_comments is None else current_comments.object_comment

    changed_columns: list[tuple[str, str]] = []
    for column in columns:
        column_docs = inspect.cleandoc(column.docs)
        current_column_comment = (
            "" if current_comments is None else current_comments.column_comment(column.name)
        )
        if column_docs != current_column_comment:
            changed_columns.append((column.name, column_docs))

    statements: list[str] = []
    if trouve_docs != current_object_comment:
        statements.append(
            f"-- comment: write the docs of the Trouve\n"
            f"ALTER {object_keyword} {address} SET COMMENT = {quote_comment(trouve_docs)}"
        )
    if not changed_columns:
        return statements
    if object_keyword == "VIEW":
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
        f"ALTER {object_keyword} {address} ALTER {column_clauses}"
    )
    return statements


def type_change_comment(object_type: WarehouseObjectType, physical_address: TrouveAddress) -> str:
    """Give the comment that tells a reader of the plan about the type test.

    ``clair compile`` opens no warehouse connection, thus it cannot know the type
    that the physical address holds now. The comment names the condition, so a
    reader of the compiled SQL knows that the run can stop here.
    """
    return (
        f"-- object type: if {physical_address} holds an object of a type other than "
        f"{object_type.value.upper()}, clair stops here and writes nothing to it"
    )


def type_change_error(
    current_object: WarehouseObject | None,
    wanted_type: WarehouseObjectType,
    physical_address: TrouveAddress,
) -> str | None:
    """Give the error that stops a Trouve whose type differs from the object at its address.

    A warehouse replaces an object with an object of the same type only. A
    Trouve that changes between a TABLE, an ICEBERG TABLE and a VIEW thus needs
    a drop of the old object. Clair does not send that drop. A drop removes each
    privilege on the object, and the role of the run often cannot grant them
    again, for example in a managed access schema. The person who changed the
    type must drop the object, and decide about the grants.

    Args:
        current_object: The object at the physical address, as the runner read
            it at the start of the node. None means that the address holds nothing.
        wanted_type: The type that the Trouve makes.
        physical_address: The address that clair writes to.

    Returns:
        The error message, or None when the address holds nothing or an object
        of the correct type.
    """
    current_type = None if current_object is None else current_object.object_type
    if current_type is None or current_type == wanted_type:
        return None

    old_type_name = current_type.value.upper()
    new_type_name = wanted_type.value.upper()
    return (
        f"{physical_address} holds the type {old_type_name}, and the Trouve now "
        f"makes the type {new_type_name}. Snowflake cannot replace one type with the other. "
        f"Clair does not drop the {old_type_name}, because a drop removes each "
        f"grant on it. Run DROP {old_type_name} {physical_address}, then run "
        f"clair again"
    )
