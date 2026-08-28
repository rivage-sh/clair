"""The abstract warehouse adapter interface.

This interface hides the database connection. Thus you can add a new adapter,
for example BigQuery or Databricks, and keep the runner as it is.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import StrEnum
from typing import Any

import pandas as pd
from pydantic import BaseModel

from clair.trouves.address import TrouveAddress


class StatementStatus(StrEnum):
    """What happened to one SQL statement."""

    SUCCESS = "success"
    FAILURE = "failure"
    # Clair made the statement, but it did not send it. A statement after the
    # one that failed has this status.
    NOT_RUN = "not_run"


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


class WarehouseAdapter(ABC):
    """The abstract interface for a warehouse connection."""

    @abstractmethod
    def connect(self, profile: dict[str, Any]) -> None:
        """Open a connection with the credentials from the profile."""
        ...

    @abstractmethod
    def new_connection(self) -> WarehouseAdapter:
        """Make a second adapter with the same profile, and open its connection.

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
    def table_exists(self, database_name: str, schema_name: str, table_name: str) -> bool:
        """Tell you if the table exists in the warehouse."""
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
