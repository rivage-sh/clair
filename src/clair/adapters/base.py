"""The abstract warehouse adapter interface.

This interface hides the database connection. Thus you can add a new adapter,
for example BigQuery or Databricks, and keep the runner as it is.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd
from pydantic import BaseModel

from clair.trouves.address import TrouveAddress


class QueryResult(BaseModel):
    """The result of one SQL query against the warehouse.

    Attributes:
        query_id: The identifier that the warehouse gave to the query.
        query_url: The URL of the query detail page in the warehouse console.
        success: True if the query completed with no error.
        error: The error message if the query failed.
        row_count: The number of rows that the query returned or changed.
    """

    query_id: str
    query_url: str
    success: bool
    error: str | None = None
    row_count: int = 0


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
    def execute(self, sql: str) -> QueryResult:
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
    ) -> QueryResult:
        """Write a DataFrame to the warehouse. This makes or replaces the table."""
        ...
