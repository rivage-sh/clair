"""Thin Snowflake helpers for the integration tests."""

from __future__ import annotations

from clair import TrouveAddress
from clair.adapters.snowflake import SnowflakeAdapter
from tests.integration.config import DATABASE_NAME, IntegrationConfig

# Each object that clair makes beside a physical object holds this text. The
# names in INFORMATION_SCHEMA are upper case.
STAGING_MARKER = "__CLAIR_"


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


def query_rows(adapter: SnowflakeAdapter, sql: str) -> list[tuple[object, ...]]:
    """Run one query and give each row back.

    ``SnowflakeAdapter.execute`` gives a QueryResult, and it holds no row. A
    ``SHOW GRANTS`` and an INFORMATION_SCHEMA query need the rows, so this
    helper opens a cursor on the connection of the adapter.
    """
    connection = adapter._conn
    if connection is None:
        raise RuntimeError("The adapter holds no open connection.")
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        return [tuple(row) for row in cursor.fetchall()]
    finally:
        cursor.close()


def staging_objects(
    adapter: SnowflakeAdapter, schema_name: str, name_prefix: str = ""
) -> list[str]:
    """Give the name of each object of clair that stays in one schema.

    A staging address holds ``__clair_``, and the source table of a MERGE holds
    ``__clair_merge_``. After a run that passed, this list is empty. The view of
    INFORMATION_SCHEMA holds a view too, thus a staging VIEW also shows here.

    Args:
        adapter: An open connection.
        schema_name: The schema of the run.
        name_prefix: Keep the objects of one test only. The tests share one
            schema, and a different test can hold a candidate on purpose.
    """
    rows = query_rows(
        adapter,
        f"SELECT TABLE_NAME FROM {DATABASE_NAME}.INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA = '{schema_name.upper()}' "
        f"AND CONTAINS(TABLE_NAME, '{STAGING_MARKER}')",
    )
    names = sorted(str(row[0]) for row in rows)
    return [name for name in names if name.upper().startswith(name_prefix.upper())]
