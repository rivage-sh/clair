"""Thin Snowflake helpers for the integration tests."""

from __future__ import annotations

from typing import Any

from clair.adapters.snowflake import SnowflakeAdapter
from tests.integration.config import IntegrationConfig


def connect(config: IntegrationConfig) -> SnowflakeAdapter:
    """Open one connection with the settings of the run."""
    adapter = SnowflakeAdapter()
    adapter.connect(config.to_connection_dict())
    return adapter


def execute(adapter: SnowflakeAdapter, sql: str) -> None:
    """Run one statement, and raise when the warehouse rejects it.

    ``SnowflakeAdapter.execute`` gives a QueryResult and raises nothing. A setup
    step must stop at the first failure.
    """
    result = adapter.execute(sql)
    if not result.success:
        raise RuntimeError(f"The statement failed: {result.error}\nSQL: {sql}")


def fetch_scalar(adapter: SnowflakeAdapter, sql: str) -> Any:
    """Give the first column of the first row."""
    frame = adapter.fetch_dataframe(f"({sql})")
    if frame.empty:
        raise RuntimeError(f"The query gave no row.\nSQL: {sql}")
    return frame.iloc[0, 0]


def row_count(adapter: SnowflakeAdapter, physical_name: str) -> int:
    """Give the number of rows in one table or view."""
    return int(fetch_scalar(adapter, f"select count(*) from {physical_name}"))


def table_names_in_schema(
    adapter: SnowflakeAdapter, database_name: str, schema_name: str
) -> set[str]:
    """Give the lower case name of each table and view in one schema."""
    # fetch_dataframe gives lower case column names.
    frame = adapter.fetch_dataframe(
        f"""(
            select table_name
            from {database_name}.information_schema.tables
            where upper(table_schema) = upper('{schema_name}')
        )"""
    )
    if frame.empty:
        return set()
    # Snowflake stores an unquoted identifier in upper case. The names of clair
    # are lower case, thus this function gives lower case names.
    return {str(name).lower() for name in frame["table_name"]}
