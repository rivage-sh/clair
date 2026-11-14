"""The data that a warehouse adapter gives: statements, objects and columns.

This module imports nothing from ``clair.trouves``, thus a Trouve can read
these types, and a plan can hold them.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel


class StatementStatus(StrEnum):
    """What happened to one SQL statement."""

    SUCCESS = "success"
    FAILURE = "failure"
    # Clair made the statement, but it did not send it. A statement after the
    # one that failed has this status.
    NOT_RUN = "not_run"


class WarehouseObjectType(StrEnum):
    """The type of an object that exists in the warehouse.

    Clair materializes a TABLE, an ICEBERG TABLE or a VIEW. Snowflake cannot
    replace one of the three with another, thus each is a member. An adapter
    maps the type names of its warehouse onto these members.
    """

    TABLE = "table"
    ICEBERG_TABLE = "iceberg table"
    VIEW = "view"


class WarehouseColumn(BaseModel):
    """One column of an object in the warehouse.

    Attributes:
        name: The name that the warehouse stores. Snowflake stores an unquoted
            name in upper case.
        data_type: The type as DDL text, for example ``NUMBER(38,0)``.
        comment: The comment of the column, or an empty text.
    """

    name: str
    data_type: str
    comment: str = ""


class WarehouseObject(BaseModel):
    """One object that exists in the warehouse.

    Attributes:
        object_type: The type of the object.
        comment: The comment of the object, or an empty text.
    """

    object_type: WarehouseObjectType
    comment: str = ""


class Statement(BaseModel):
    """One SQL statement, and what the warehouse answered.

    An adapter gives a Statement for each statement that it executes. A result
    object of clair holds the list of them, thus the text, the identifier and
    the answer of one statement stay together.

    Attributes:
        sql: The text that clair sent, or made and did not send.
        status: SUCCESS, FAILURE, or NOT_RUN.
        query_id: The identifier that the warehouse gave to the query. It is
            None for a statement that the warehouse did not identify.
        query_url: The URL of the query detail page in the warehouse console.
        error: The error message of a statement that failed.
        row_count: The number of rows that the statement returned or changed.
    """

    sql: str = ""
    status: StatementStatus = StatementStatus.NOT_RUN
    query_id: str | None = None
    query_url: str | None = None
    error: str = ""
    row_count: int = 0

    @property
    def success(self) -> bool:
        """Tell you if the warehouse executed the statement with no error."""
        return self.status == StatementStatus.SUCCESS
