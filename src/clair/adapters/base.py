"""The abstract warehouse adapter interface.

This interface hides the database connection. Thus you can add a new adapter,
for example BigQuery or Databricks, and keep the runner as it is.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from clair.adapters.objects import Statement, WarehouseColumn, WarehouseObject, WarehouseObjectType
from clair.environments.environments import Environment
from clair.trouves.address import TrouveAddress
from clair.trouves.config import ResolvedConfig
from clair.trouves.trouve import TrouveAbc


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
        """Set the session context: the warehouse, the role and the database.

        A role or a warehouse of None gives back the value that the session had
        when it opened. The runner calls this method for each Trouve, thus the
        role of one Trouve never stays for the next Trouve on the same thread.
        """
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

    def use_session_of(self, config: ResolvedConfig, query_tag: str) -> None:
        """Give the session the settings of one Trouve, before its first statement.

        ``clair run`` and ``clair test`` call this method for each Trouve. Thus
        each statement of a Trouve runs with its role, its warehouse, its query
        tag and its timeout, and no value of the Trouve before it stays.
        """
        self.set_session_parameters(
            query_tag=query_tag, statement_timeout_seconds=config.statement_timeout_seconds
        )
        self.set_context(warehouse=config.warehouse, role=config.role)

    @abstractmethod
    def describe_object(self, address: TrouveAddress) -> WarehouseObject | None:
        """Give the type and the comment of the object at the address, or None if it holds none.

        Clair asks this question before it writes to a physical address. A
        warehouse replaces an object with an object of the same type only, so a
        Trouve that changes from a TABLE to a VIEW needs a drop first. The
        answer also tells the runner if an incremental Trouve has a base table,
        and if the comment of that table differs from the docs of the Trouve.

        Raises:
            RunError: If the warehouse cannot answer. An adapter must not give
                None for a query that failed, because None makes an incremental
                Trouve build its table again.
        """
        ...

    def object_type(self, address: TrouveAddress) -> WarehouseObjectType | None:
        """Give the type of the object at the address, or None if it holds none."""
        warehouse_object = self.describe_object(address)
        return None if warehouse_object is None else warehouse_object.object_type

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

    def cancel(self) -> None:
        """Cancel the statement that this connection runs now. Another thread calls it.

        A run that is cancelled sends no new statement and promotes nothing,
        thus an adapter that cannot cancel lets one statement complete, and the
        result is still correct. The default does nothing.
        """

    @abstractmethod
    def close(self) -> None:
        """Close the connection."""
        ...

    def fetch_dataframe(
        self, source: TrouveAbc | TrouveAddress, sample_query: str | None = None
    ) -> pd.DataFrame:
        """Read a Trouve or an address into a pandas DataFrame.

        A Trouve reads its logical address, which is the production address.
        To read the object of a different environment, give
        ``trouve.get_physical_address(env_name)``. clair applies no row limit,
        in the same way as the pandas API of the Snowflake connector.

        Args:
            source: A Trouve, for example one that a notebook imports, or an
                address.
            sample_query: The subquery of a sampled run, which the adapter
                reads in place of the complete table. None reads each row.

        Raises:
            TrouveFileNotFoundError: If *source* is a Trouve that no file defines.
        """
        address = source.logical_address if isinstance(source, TrouveAbc) else source
        return self.read_table(address, sample_query)

    @abstractmethod
    def read_table(
        self, address: TrouveAddress, sample_query: str | None = None
    ) -> pd.DataFrame:
        """Read the object at *address* into a pandas DataFrame.

        Each adapter implements this method, and ``fetch_dataframe`` calls it.
        *sample_query* is the subquery of a sampled run, which the adapter
        reads in place of the complete table at *address*. None reads each row.
        """
        ...

    @abstractmethod
    def write_dataframe(
        self, dataframe: pd.DataFrame, address: TrouveAddress, transient: bool
    ) -> Statement:
        """Write a DataFrame to the warehouse. This makes or replaces the table.

        *transient* True makes a TRANSIENT table.
        """
        ...
