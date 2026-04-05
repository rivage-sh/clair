"""Snowflake adapter implementation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization

from clair.adapters.base import QueryResult, WarehouseAdapter


class SnowflakeAdapter(WarehouseAdapter):
    """Snowflake warehouse adapter using snowflake-connector-python."""

    def __init__(self) -> None:
        self._conn: snowflake.connector.SnowflakeConnection | None = None
        self._region: str = ""
        self._account_locator: str = ""

    def connect(self, profile: dict[str, Any]) -> None:
        """Connect to Snowflake using profile credentials.

        Supports:
        - SSO via authenticator=externalbrowser
        - Key pair auth via private_key_path
        - Standard username/password
        """
        self._region = profile.get("region", "")
        self._account_locator = profile.get("account_locator", "")

        connect_args: dict[str, Any] = {
            "account": profile["account"],
            "user": profile["user"],
        }

        # Auth method
        if "authenticator" in profile:
            connect_args["authenticator"] = profile["authenticator"]
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

        # Optional context
        for key in ("warehouse", "role", "database"):
            if key in profile:
                connect_args[key] = profile[key]

        self._conn = snowflake.connector.connect(**connect_args)

    def execute(self, sql: str) -> QueryResult:
        """Execute SQL and return a QueryResult with query ID and URL."""
        if self._conn is None:
            raise RuntimeError("Not connected. Call connect() first.")

        cursor = self._conn.cursor()
        try:
            cursor.execute(sql)
            query_id = cursor.sfqid or "unknown"
            return QueryResult(
                query_id=query_id,
                query_url=self._build_query_url(query_id),
                success=True,
                row_count=cursor.rowcount or 0,
            )
        except Exception as e:
            query_id = getattr(cursor, "sfqid", None) or "unknown"
            return QueryResult(
                query_id=query_id,
                query_url=self._build_query_url(query_id),
                success=False,
                error=str(e),
            )
        finally:
            cursor.close()

    def table_exists(self, database_name: str, schema_name: str, table_name: str) -> bool:
        """Check whether a table exists in Snowflake via INFORMATION_SCHEMA."""
        result = self.execute(
            f"SELECT 1 FROM {database_name}.INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_CATALOG = '{database_name.upper()}' "
            f"AND TABLE_SCHEMA = '{schema_name.upper()}' "
            f"AND TABLE_NAME = '{table_name.upper()}'"
        )
        return result.row_count > 0

    def set_context(
        self,
        warehouse: str | None = None,
        role: str | None = None,
        database_name: str | None = None,
    ) -> None:
        """Set session context by executing USE commands.

        Only issues USE statements for values that are non-None and non-empty.
        Order: ROLE first (affects permissions), then WAREHOUSE, then DATABASE.
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

    def fetch_dataframe(self, full_name: str) -> pd.DataFrame:
        """Fetch an entire Snowflake table as a pandas DataFrame."""
        if self._conn is None:
            raise RuntimeError("Not connected. Call connect() first.")

        cursor = self._conn.cursor()
        try:
            cursor.execute(f"SELECT * FROM {full_name}")
            dataframe = cursor.fetch_pandas_all()
            dataframe.columns = dataframe.columns.str.lower()
            return dataframe
        finally:
            cursor.close()

    def write_dataframe(
        self,
        dataframe: pd.DataFrame,
        full_name: str,
        database_name: str,
        schema_name: str,
        table_name: str,
    ) -> QueryResult:
        """Write a DataFrame to Snowflake, creating or replacing the table."""
        if self._conn is None:
            raise RuntimeError("Not connected. Call connect() first.")

        success, num_chunks, num_rows, output = write_pandas(
            conn=self._conn,
            df=dataframe,
            table_name=table_name.upper(),
            database=database_name.upper(),
            schema=schema_name.upper(),
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False,
        )
        # query_id and query_url are empty: write_dataframe uses CREATE TEMP STAGE
        # and PUT under the hood, not a single queryable SQL statement.
        return QueryResult(
            query_id="",
            query_url="",
            success=success,
            row_count=num_rows,
        )

    def close(self) -> None:
        """Close the Snowflake connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def _build_query_url(self, query_id: str) -> str:
        """Build a Snowflake console URL for a query ID."""
        return (
            f"https://app.snowflake.com/{self._region}/{self._account_locator}"
            f"/#/compute/history/queries/{query_id}/detail"
        )
