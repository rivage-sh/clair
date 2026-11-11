"""The Snowflake adapter."""

from __future__ import annotations

import threading
import uuid
from pathlib import Path
from typing import Any

import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from snowflake.connector.pandas_tools import write_pandas

from clair.adapters.base import (
    Statement,
    StatementStatus,
    WarehouseAdapter,
    WarehouseColumn,
    WarehouseObject,
    WarehouseObjectType,
)
from clair.environments.environments import Authenticator, Environment, Password, PrivateKey
from clair.exceptions import RunError
from clair.trouves.address import TrouveAddress


class SnowflakeAdapter(WarehouseAdapter):
    """The Snowflake warehouse adapter. It uses snowflake-connector-python."""

    def __init__(self) -> None:
        self._conn: snowflake.connector.SnowflakeConnection | None = None
        self._environment: Environment | None = None
        # The role and the warehouse that the session had before clair sent a
        # USE statement. set_context() reads them one time.
        self._opening_context: tuple[str | None, str | None] | None = None

        # The text and the request id of the statement that runs now. cancel()
        # reads them from another thread.
        self._running_statement: tuple[str, uuid.UUID] | None = None
        self._running_lock = threading.Lock()

    @classmethod
    def connected(cls, environment: Environment) -> SnowflakeAdapter:
        """Make an adapter and connect it to *environment*: one line in a notebook.

        ``SnowflakeAdapter.connected(load_environment("dev"))`` reads
        ``~/.clair/environments.yml`` and connects. Call ``close()`` at the end.
        """
        adapter = cls()
        adapter.connect(environment)
        return adapter

    def connect(self, environment: Environment) -> None:
        """Connect to Snowflake with the connection and the authentication of the environment."""
        connection = environment.connection
        authentication = environment.authentication
        self._environment = environment

        connect_args: dict[str, Any] = {
            "account": connection.account,
            "user": connection.user,
            "warehouse": connection.warehouse,
            # Snowflake ends a session after 4 hours with no query. The pool
            # opens each connection when the run starts, and a thread can wait
            # longer than that for a slow Trouve upstream. The connector then
            # sends a heartbeat, thus a run can last for any time.
            "client_session_keep_alive": True,
        }
        if connection.role:
            connect_args["role"] = connection.role

        if isinstance(authentication, Authenticator):
            connect_args["authenticator"] = authentication.authenticator
            if authentication.authenticator == "externalbrowser":
                # A parallel run opens one connection for each thread. Without
                # this cache, SSO opens one browser window for each connection.
                # The connector keeps the token in the credential store of the
                # operating system, and the second connection reads it.
                connect_args["client_store_temporary_credential"] = True
        elif isinstance(authentication, PrivateKey):
            connect_args["private_key"] = _read_private_key(authentication)
        elif isinstance(authentication, Password):
            connect_args["password"] = authentication.password
        else:
            raise TypeError(f"No connection for the authentication {authentication!r}.")

        self._conn = snowflake.connector.connect(**connect_args)

    def new_connection(self) -> SnowflakeAdapter:
        """Make a second SnowflakeAdapter with the same environment, and connect it."""
        if self._environment is None:
            raise RuntimeError("Not connected. Call connect() first.")
        return SnowflakeAdapter.connected(self._environment)

    def execute(self, sql: str) -> Statement:
        """Execute the SQL and give a Statement with the query ID and the URL."""
        if self._conn is None:
            raise RuntimeError("Not connected. Call connect() first.")

        cursor = self._conn.cursor()
        # clair gives the request id, thus cancel() can name this statement
        # while execute() waits for it.
        request_id = uuid.uuid4()
        with self._running_lock:
            self._running_statement = (sql.strip(" \t\n\r"), request_id)
        try:
            cursor.execute(sql, _statement_params={"requestId": str(request_id)})
            query_id = cursor.sfqid or "unknown"
            return Statement(
                sql=sql,
                status=StatementStatus.SUCCESS,
                query_id=query_id,
                query_url=self._build_query_url(query_id),
                row_count=cursor.rowcount or 0,
            )
        except Exception as e:  # noqa: BLE001 — each driver error becomes a Statement that failed
            query_id = getattr(cursor, "sfqid", None) or "unknown"
            return Statement(
                sql=sql,
                status=StatementStatus.FAILURE,
                query_id=query_id,
                query_url=self._build_query_url(query_id),
                error=str(e),
            )
        finally:
            with self._running_lock:
                self._running_statement = None
            cursor.close()

    def cancel(self) -> None:
        """Cancel the statement that execute() waits for now. Another thread calls it.

        The connector cancels a statement by its text and its request id, in
        the same way as for its own timeout. execute() then gives a Statement
        that failed. With no statement, the method does nothing.
        """
        with self._running_lock:
            running_statement = self._running_statement
        if running_statement is None or self._conn is None:
            return
        sql, request_id = running_statement
        self._conn._cancel_query(sql, request_id)

    def describe_object(self, address: TrouveAddress) -> WarehouseObject | None:
        """Give the type and the comment of the object at the address. Reads INFORMATION_SCHEMA.

        ``INFORMATION_SCHEMA.TABLES`` holds a row for a view too, and the
        ``TABLE_TYPE`` column separates the two. Snowflake also writes
        ``MATERIALIZED VIEW`` and ``EXTERNAL TABLE`` in that column. Clair makes
        neither type, but a user can point a Trouve at an address that holds
        one, so this method reads each name that holds the word VIEW as a view.
        An Iceberg table is a ``BASE TABLE`` with ``IS_ICEBERG = 'YES'``.

        The method gives None when the address holds no object.

        Raises:
            RunError: If the query fails. "No answer" is not "no object": an
                incremental run that sees no object makes the table again.
        """
        if self._conn is None:
            raise RuntimeError("Not connected. Call connect() first.")

        cursor = self._conn.cursor()
        try:
            cursor.execute(
                f"SELECT TABLE_TYPE, IS_ICEBERG, COMMENT "
                f"FROM {address.database_name}.INFORMATION_SCHEMA.TABLES "
                f"WHERE TABLE_CATALOG = '{address.database_name.upper()}' "
                f"AND TABLE_SCHEMA = '{address.schema_name.upper()}' "
                f"AND TABLE_NAME = '{address.table_name.upper()}'"
            )
            row = cursor.fetchone()
        except Exception as error:
            raise RunError(f"Clair cannot read the object at {address}: {error}") from error
        finally:
            cursor.close()

        if row is None or row[0] is None:
            return None
        table_type, is_iceberg, comment = row
        if "VIEW" in str(table_type).upper():
            object_type = WarehouseObjectType.VIEW
        elif str(is_iceberg).upper() == "YES":
            object_type = WarehouseObjectType.ICEBERG_TABLE
        else:
            object_type = WarehouseObjectType.TABLE
        return WarehouseObject(object_type=object_type, comment=str(comment or ""))

    def describe_columns(self, address: TrouveAddress) -> list[WarehouseColumn] | None:
        """Give the columns of the object at the address. Reads DESCRIBE TABLE.

        DESCRIBE TABLE gives the type as DDL text, for example
        ``VARCHAR(16777216)``, thus an ALTER can use it as it is. The method
        gives None when the query fails, for example because the address holds
        no object.
        """
        if self._conn is None:
            raise RuntimeError("Not connected. Call connect() first.")

        cursor = self._conn.cursor()
        try:
            cursor.execute(f"DESCRIBE TABLE {address}")
            rows = cursor.fetchall()
        except Exception:  # noqa: BLE001 — a query that fails means "clair sees no object"
            return None
        finally:
            cursor.close()

        return _columns_of(rows)

    def describe_query(self, sql: str) -> list[WarehouseColumn]:
        """Give the columns of a SELECT. Reads DESCRIBE RESULT of the query with LIMIT 0.

        ``LIMIT 0`` makes Snowflake compile the query and read no row.
        DESCRIBE RESULT then gives each column with its type as DDL text, in the
        same form as DESCRIBE TABLE.

        Raises:
            RunError: If Snowflake cannot compile the query.
        """
        if self._conn is None:
            raise RuntimeError("Not connected. Call connect() first.")

        cursor = self._conn.cursor()
        try:
            cursor.execute(f"SELECT * FROM (\n{sql}\n) LIMIT 0")
            cursor.execute(f"DESCRIBE RESULT '{cursor.sfqid}'")
            rows = cursor.fetchall()
        except Exception as error:
            raise RunError(f"Snowflake cannot read the columns of the SQL: {error}") from error
        finally:
            cursor.close()

        return _columns_of(rows)

    def set_context(
        self,
        warehouse: str | None = None,
        role: str | None = None,
        database_name: str | None = None,
    ) -> None:
        """Set the session context with USE commands.

        A thread runs many Trouves on one session, and a USE statement stays
        until the next one. Thus a role or a warehouse of None gives back the
        value that the session had when it opened, and does not keep the value
        of the Trouve before. The method sends ROLE first, because the role
        controls the permissions. Then it sends WAREHOUSE, then DATABASE. A
        database_name of None sends no USE DATABASE.
        """
        if self._conn is None:
            raise RuntimeError("Not connected. Call connect() first.")

        cursor = self._conn.cursor()
        try:
            if self._opening_context is None:
                cursor.execute("SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE()")
                opening_row = cursor.fetchone()
                self._opening_context = (
                    (opening_row[0], opening_row[1]) if opening_row else (None, None)
                )
            opening_role, opening_warehouse = self._opening_context
            wanted_role = role if role and role.strip() else opening_role
            wanted_warehouse = warehouse if warehouse and warehouse.strip() else opening_warehouse
            if wanted_role:
                cursor.execute(f"USE ROLE {wanted_role}")
            if wanted_warehouse:
                cursor.execute(f"USE WAREHOUSE {wanted_warehouse}")
            if database_name and database_name.strip():
                cursor.execute(f"USE DATABASE {database_name}")
        finally:
            cursor.close()

    def set_session_parameters(
        self, query_tag: str, statement_timeout_seconds: int | None
    ) -> None:
        """Set QUERY_TAG and STATEMENT_TIMEOUT_IN_SECONDS with ALTER SESSION.

        UNSET gives the timeout of the account or the user back.
        """
        if self._conn is None:
            raise RuntimeError("Not connected. Call connect() first.")

        escaped_tag = query_tag.replace("\\", "\\\\").replace("'", "\\'")
        cursor = self._conn.cursor()
        try:
            cursor.execute(f"ALTER SESSION SET QUERY_TAG = '{escaped_tag}'")
            if statement_timeout_seconds is None:
                cursor.execute("ALTER SESSION UNSET STATEMENT_TIMEOUT_IN_SECONDS")
            else:
                cursor.execute(
                    f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {statement_timeout_seconds}"
                )
        finally:
            cursor.close()

    def read_table(
        self, address: TrouveAddress, sample_query: str | None = None
    ) -> pd.DataFrame:
        """Read a Snowflake table, or the sample of a sampled run, into a pandas DataFrame."""
        if self._conn is None:
            raise RuntimeError("Not connected. Call connect() first.")

        relation = sample_query if sample_query is not None else str(address)
        query = f"SELECT * FROM {relation}"
        cursor = self._conn.cursor()
        try:
            cursor.execute(query)
            dataframe = cursor.fetch_pandas_all()
            dataframe.columns = dataframe.columns.str.lower()
            return dataframe
        finally:
            cursor.close()

    def write_dataframe(
        self, dataframe: pd.DataFrame, address: TrouveAddress, transient: bool
    ) -> Statement:
        """Write a DataFrame to Snowflake. This makes or replaces the table."""
        if self._conn is None:
            raise RuntimeError("Not connected. Call connect() first.")

        success, _num_chunks, num_rows, _output = write_pandas(
            conn=self._conn,
            df=dataframe,
            table_name=address.table_name.upper(),
            database=address.database_name.upper(),
            schema=address.schema_name.upper(),
            auto_create_table=True,
            overwrite=True,
            # An empty table type makes a permanent table.
            table_type="transient" if transient else "",
            quote_identifiers=False,
            # write_pandas sends the rows as a Parquet file, and Snowflake reads
            # the type of each column from that file. Parquet holds a timestamp
            # as an integer plus a logical type. Without this option Snowflake
            # reads the integer only, and a datetime column becomes NUMBER.
            use_logical_type=True,
        )
        # The Statement holds no SQL, no query ID and no URL. Internally
        # write_dataframe sends CREATE TEMP STAGE and PUT, not one SQL
        # statement that you can look up.
        return Statement(
            status=StatementStatus.SUCCESS if success else StatementStatus.FAILURE,
            error="" if success else f"Clair cannot write the DataFrame to {address}",
            row_count=num_rows,
        )

    def close(self) -> None:
        """Close the Snowflake connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def _build_query_url(self, query_id: str) -> str:
        """Make the Snowflake console URL for a query ID."""
        assert self._environment is not None
        connection = self._environment.connection
        return (
            f"https://app.snowflake.com/{connection.region or ''}/{connection.account_locator or ''}"
            f"/#/compute/history/queries/{query_id}/detail"
        )


def _read_private_key(authentication: PrivateKey) -> bytes:
    """Read the PEM key file, and give the key in the DER format that the connector reads."""
    key_path = Path(authentication.private_key_path).expanduser()
    passphrase = authentication.private_key_passphrase
    private_key = serialization.load_pem_private_key(
        key_path.read_bytes(), password=passphrase.encode() if passphrase else None
    )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


# The position of the comment in a row of DESCRIBE TABLE and of DESCRIBE RESULT.
_DESCRIBE_COMMENT_INDEX = 9


def _columns_of(rows: list[tuple]) -> list[WarehouseColumn]:
    """Give the columns of the rows of DESCRIBE TABLE or DESCRIBE RESULT.

    Each row is (name, type, kind, null?, default, primary key, unique key,
    check, expression, comment, ...). A kind other than COLUMN, for example a
    virtual column, takes no value from an insert.
    """
    return [
        WarehouseColumn(
            name=str(row[0]),
            data_type=str(row[1]),
            comment=str(row[_DESCRIBE_COMMENT_INDEX] or "") if len(row) > _DESCRIBE_COMMENT_INDEX else "",
        )
        for row in rows
        if str(row[2]).upper() == "COLUMN"
    ]
