"""Thin Snowflake helpers for the integration tests."""

from __future__ import annotations

from clair import TrouveAddress
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


def table_exists(adapter: SnowflakeAdapter, address: TrouveAddress) -> bool:
    """Tell you if the table or the view at one address exists."""
    return adapter.table_exists(
        address.database_name, address.schema_name, address.table_name
    )


def row_count(adapter: SnowflakeAdapter, address: TrouveAddress) -> int:
    """Give the number of rows at one address.

    Each table of the tests holds a few rows, thus a complete read costs almost
    nothing.
    """
    return len(adapter.fetch_dataframe(address))
