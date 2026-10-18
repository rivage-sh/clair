"""The abstract warehouse adapter interface.

This interface hides the database connection. Thus you can add a new adapter,
for example BigQuery or Databricks, and keep the runner as it is.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import StrEnum

import pandas as pd
from pydantic import BaseModel

from clair.environments.environments import Environment
from clair.trouves.address import TrouveAddress


class StatementStatus(StrEnum):
    """What happened to one SQL statement."""

    SUCCESS = "success"
    FAILURE = "failure"
    # Clair made the statement, but it did not send it. A statement after the
    # one that failed has this status.
    NOT_RUN = "not_run"


class WarehouseObjectType(StrEnum):
    """The type of an object that exists in the warehouse.

    Clair materializes a TABLE or a VIEW. It never makes a SOURCE, thus this
    enum holds two members and ``TrouveType`` holds three. An adapter maps the
    type names of its warehouse onto these two members.
    """

    TABLE = "table"
    VIEW = "view"


class WarehouseColumn(BaseModel):
    """One column of an object in the warehouse.

    Attributes:
        name: The name that the warehouse stores. Snowflake stores an unquoted
            name in upper case.
        data_type: The type as DDL text, for example ``NUMBER(38,0)``.
    """

    name: str
    data_type: str


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
        retryable: True if the warehouse can accept the same statement on a
            second attempt: a network fault, or an internal error of the
            warehouse. A fault of the SQL is never retryable.
    """

    sql: str = ""
    status: StatementStatus = StatementStatus.NOT_RUN
    query_id: str | None = None
    query_url: str | None = None
    error: str = ""
    row_count: int = 0
    retryable: bool = False

    @property
    def success(self) -> bool:
        """Tell you if the warehouse executed the statement with no error."""
        return self.status == StatementStatus.SUCCESS


class WarehouseAdapter(ABC):
    """The abstract interface for a warehouse connection."""

    @abstractmethod
    def connect(self, environment: Environment) -> None:
        """Open a connection with the connection and the authentication of the environment."""
        ...

    @abstractmethod
    def new_connection(self) -> WarehouseAdapter:
        """Make a second adapter with the same environment, and open its connection.

        A parallel run gives one connection to each thread. Two threads that
        share one connection change the session context of each other, because
        `USE WAREHOUSE` and `USE ROLE` apply to the full session.

        Raises:
            RuntimeError: If this adapter has no open connection.
        """
        ...

    @abstractmethod
    def execute(self, sql: str) -> Statement:
        """Execute one SQL statement and give the result."""
        ...

    @abstractmethod
    def set_context(
        self,
        warehouse: str | None = None,
        role: str | None = None,
        database_name: str | None = None,
    ) -> None:
        """Set the session context: the warehouse, the role and the database."""
        ...

    @abstractmethod
    def set_session_parameters(
        self, query_tag: str, statement_timeout_seconds: int | None
    ) -> None:
        """Set the query tag and the statement timeout of the session.

        One thread runs many Trouves on one session, thus each call sets both
        values. A timeout of None removes the timeout of the Trouve before it.
        """
        ...

    @abstractmethod
    def object_type(self, address: TrouveAddress) -> WarehouseObjectType | None:
        """Give the type of the object at the address, or None if it holds none.

        Clair asks this question before it writes to a physical address. A
        warehouse replaces an object with an object of the same type only, so a
        Trouve that changes from a TABLE to a VIEW needs a drop first. The
        answer also tells the runner if an incremental Trouve has a base table.
        """
        ...

    @abstractmethod
    def describe_columns(self, address: TrouveAddress) -> list[WarehouseColumn] | None:
        """Give the columns of the object at the address in their order, or None if it holds none.

        An incremental run inserts into a table that already exists. The
        runner asks for its columns, thus the insert names each column, and a
        new column order in the SQL cannot move a value into the wrong column.
        """
        ...

    @abstractmethod
    def describe_query(self, sql: str) -> list[WarehouseColumn]:
        """Give the columns that a SELECT gives, in their order. The warehouse reads no row.

        An incremental run compares these columns with the columns of the
        target table before it writes.

        Raises:
            RunError: If the warehouse cannot compile the query. The message
                holds the error of the warehouse.
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """Close the connection."""
        ...

    @abstractmethod
    def fetch_dataframe(self, address: TrouveAddress) -> pd.DataFrame:
        """Read a table into a pandas DataFrame."""
        ...

    @abstractmethod
    def write_dataframe(
        self, dataframe: pd.DataFrame, address: TrouveAddress
    ) -> Statement:
        """Write a DataFrame to the warehouse. This makes or replaces the table."""
        ...
