"""The Snowflake adapter."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from snowflake.connector.pandas_tools import write_pandas

from clair.adapters.base import ObjectType, Statement, StatementStatus, WarehouseAdapter
from clair.trouves.address import TrouveAddress


class SnowflakeAdapter(WarehouseAdapter):
    """The Snowflake warehouse adapter. It uses snowflake-connector-python."""

    def __init__(self) -> None:
        self._conn: snowflake.connector.SnowflakeConnection | None = None
        self._region: str = ""
        self._account_locator: str = ""
        self._profile: dict[str, Any] | None = None

    def connect(self, profile: dict[str, Any]) -> None:
        """Connect to Snowflake with the credentials from the profile.

        The method accepts these authentication methods:
        - SSO, with authenticator=externalbrowser
        - Key pair, with private_key_path
        - The usual user name and password
        """
        self._region = profile.get("region", "")
        self._account_locator = profile.get("account_locator", "")
        self._profile = dict(profile)

        connect_args: dict[str, Any] = {
            "account": profile["account"],
            "user": profile["user"],
        }

        # The authentication method.
        if "authenticator" in profile:
            connect_args["authenticator"] = profile["authenticator"]
            if profile["authenticator"] == "externalbrowser":
                # A parallel run opens one connection for each thread. Without
                # this cache, SSO opens one browser window for each connection.
                # The connector keeps the token in the credential store of the
                # operating system, and the second connection reads it.
                connect_args["client_store_temporary_credential"] = True
        elif "private_key_pem" in profile:
            pem_content = profile["private_key_pem"]
            passphrase = profile.get("private_key_passphrase")
            password = passphrase.encode() if isinstance(passphrase, str) else passphrase
            pem_bytes = pem_content.encode() if isinstance(pem_content, str) else pem_content
            p_key = serialization.load_pem_private_key(pem_bytes, password=password)
            connect_args["private_key"] = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        elif "private_key_path" in profile:
            key_path = Path(profile["private_key_path"]).expanduser()
            passphrase = profile.get("private_key_passphrase")
            password = passphrase.encode() if isinstance(passphrase, str) else passphrase
            with open(key_path, "rb") as f:
                p_key = serialization.load_pem_private_key(f.read(), password=password)
                connect_args["private_key"] = p_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
        elif "password" in profile:
            connect_args["password"] = profile["password"]

        # The optional session context.
        for key in ("warehouse", "role", "database"):
            if key in profile:
                connect_args[key] = profile[key]

        self._conn = snowflake.connector.connect(**connect_args)

    def new_connection(self) -> SnowflakeAdapter:
        """Make a second SnowflakeAdapter with the same profile, and connect it."""
        if self._profile is None:
            raise RuntimeError("Not connected. Call connect() first.")
        adapter = SnowflakeAdapter()
        adapter.connect(self._profile)
        return adapter

    def execute(self, sql: str) -> Statement:
        """Execute the SQL and give a Statement with the query ID and the URL."""
        if self._conn is None:
            raise RuntimeError("Not connected. Call connect() first.")

        cursor = self._conn.cursor()
        try:
            cursor.execute(sql)
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
            cursor.close()

    def object_type(self, address: TrouveAddress) -> ObjectType | None:
        """Give the type of the object at the address. Reads INFORMATION_SCHEMA.

        ``INFORMATION_SCHEMA.TABLES`` holds a row for a view too, and the
        ``TABLE_TYPE`` column separates the two. Snowflake also writes
        ``MATERIALIZED VIEW`` and ``EXTERNAL TABLE`` in that column. Clair makes
        neither type, but a user can point a Trouve at an address that holds
        one, so this method reads each name that holds the word VIEW as a view.

        The method gives None when the address holds no object, and also when
        the query fails, for example because the database does not exist. A
        caller must therefore make no decision that needs the object to exist.
        """
        if self._conn is None:
            raise RuntimeError("Not connected. Call connect() first.")

        cursor = self._conn.cursor()
        try:
            cursor.execute(
                f"SELECT TABLE_TYPE FROM {address.database_name}.INFORMATION_SCHEMA.TABLES "
                f"WHERE TABLE_CATALOG = '{address.database_name.upper()}' "
                f"AND TABLE_SCHEMA = '{address.schema_name.upper()}' "
                f"AND TABLE_NAME = '{address.table_name.upper()}'"
            )
            row = cursor.fetchone()
        except Exception:  # noqa: BLE001 — a query that fails means "clair sees no object"
            return None
        finally:
            cursor.close()

        if row is None or row[0] is None:
            return None
        return ObjectType.VIEW if "VIEW" in str(row[0]).upper() else ObjectType.TABLE

    def set_context(
        self,
        warehouse: str | None = None,
        role: str | None = None,
        database_name: str | None = None,
    ) -> None:
        """Set the session context with USE commands.

        The method sends a USE statement only for a value that is not None and
        not empty. It sends ROLE first, because the role controls the
        permissions. Then it sends WAREHOUSE, then DATABASE.
        """
        if self._conn is None:
            raise RuntimeError("Not connected. Call connect() first.")

        cursor = self._conn.cursor()
        try:
            if role and role.strip():
                cursor.execute(f"USE ROLE {role}")
            if warehouse and warehouse.strip():
                cursor.execute(f"USE WAREHOUSE {warehouse}")
            if database_name and database_name.strip():
                cursor.execute(f"USE DATABASE {database_name}")
        finally:
            cursor.close()

    def fetch_dataframe(self, address: TrouveAddress) -> pd.DataFrame:
        """Read a complete Snowflake table into a pandas DataFrame."""
        if self._conn is None:
            raise RuntimeError("Not connected. Call connect() first.")

        cursor = self._conn.cursor()
        try:
            cursor.execute(f"SELECT * FROM {address}")
            dataframe = cursor.fetch_pandas_all()
            dataframe.columns = dataframe.columns.str.lower()
            return dataframe
        finally:
            cursor.close()

    def write_dataframe(
        self, dataframe: pd.DataFrame, address: TrouveAddress
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
        return (
            f"https://app.snowflake.com/{self._region}/{self._account_locator}"
            f"/#/compute/history/queries/{query_id}/detail"
        )
