"""Abstract warehouse adapter interface.

Abstracts the database connection so that future adapters (e.g. BigQuery,
Databricks) can be added without rewriting the runner.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd
from pydantic import BaseModel


class QueryResult(BaseModel):
    """Result of executing a single SQL query against the warehouse.

    Attributes:
        query_id: Warehouse-assigned identifier for the executed query.
        query_url: URL to the query detail page in the warehouse console.
        success: Whether the query completed without error.
        error: Error message if the query failed.
        row_count: Number of rows returned (or affected) by the query.
    """

    query_id: str
    query_url: str
    success: bool
    error: str | None = None
    row_count: int = 0


class WarehouseAdapter(ABC):
    """Abstract interface for warehouse connections."""

    @abstractmethod
    def connect(self, profile: dict[str, Any]) -> None:
        """Establish a connection using profile credentials."""
        ...

    @abstractmethod
    def execute(self, sql: str) -> QueryResult:
        """Execute a SQL statement and return the result."""
        ...

    @abstractmethod
    def set_context(
        self,
        warehouse: str | None = None,
        role: str | None = None,
        database_name: str | None = None,
    ) -> None:
        """Set the session context (warehouse, role, database)."""
        ...

    @abstractmethod
    def table_exists(self, database_name: str, schema_name: str, table_name: str) -> bool:
        """Check whether a table exists in the warehouse."""
        ...

    @abstractmethod
    def close(self) -> None:
        """Close the connection."""
        ...

    @abstractmethod
    def fetch_dataframe(self, full_name: str) -> pd.DataFrame:
        """Fetch a table as a pandas DataFrame."""
        ...

    @abstractmethod
    def write_dataframe(
        self,
        dataframe: pd.DataFrame,
        full_name: str,
        database_name: str,
        schema_name: str,
        table_name: str,
    ) -> QueryResult:
        """Write a DataFrame to the warehouse, creating or replacing the table."""
        ...
