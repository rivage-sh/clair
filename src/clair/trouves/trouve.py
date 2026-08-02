"""Trouve -- the basic unit in Clair.

One Trouve maps to one object in Snowflake that you can query: a source table,
a transformed table, or a view. Each Trouve stays in its own .py file. The
framework finds each Trouve automatically.

This module holds two classes:

* ``TrouveAbc`` -- the abstract base. It holds the attributes that every backend
  shares: the columns, the tests, the docs, and the compiled attributes.
* ``Trouve`` -- the SQL backend. Snowflake materializes it from SQL.

The pandas backend, ``PandasTrouve``, stays in ``pandas_trouve.py``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from clair.trouves._refs import register
from clair.trouves.column import Column
from clair.trouves.run_config import SOURCE, TARGET, IncrementalMode, RunConfig, RunMode
from clair.trouves.test import AnyTest


class TrouveType(StrEnum):
    SOURCE = "source"
    TABLE = "table"
    VIEW = "view"


class ExecutionType(StrEnum):
    SNOWFLAKE = "snowflake"
    PANDAS = "pandas"


class CompiledAttributes(BaseModel):
    """The attributes that discovery sets after it loads a Trouve.

    These attributes exist only when ``TrouveAbc.is_compiled`` is True.
    """

    full_name: str       # The routed name. Clair puts it in the SQL and the DDL.
    logical_name: str    # The name from the file path. DAG edges and selectors use it.
    resolved_sql: str
    resolved_transform: str = ""
    file_path: Path
    module_name: str
    imports: list[str]
    config: Any  # A ResolvedConfig. The type is Any, to prevent a circular import.
    execution_type: ExecutionType


class TrouveAbc(BaseModel, ABC):
    """The base class of every Trouve backend.

    A subclass supplies the backend. ``Trouve`` runs SQL in Snowflake.
    ``PandasTrouve`` runs a Python function on the clair machine. Each subclass
    gives its own ``execution_type`` and its own ``upstream_trouves``.

    Attributes:
        type: SOURCE, TABLE, or VIEW.
        columns: The column definitions, for the docs and for future checks.
        tests: The data quality tests.
        docs: The documentation text for this Trouve.
        run_config: The materialization strategy.
        compiled: Discovery sets this. It stays None until discovery reads the
             project.
    """

    type: TrouveType = Field(default=TrouveType.TABLE)
    columns: list[Column] = []
    tests: list[AnyTest] = []
    docs: str = ""
    run_config: RunConfig = Field(default_factory=RunConfig)
    compiled: CompiledAttributes | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @property
    @abstractmethod
    def execution_type(self) -> ExecutionType:
        """The backend that materializes this Trouve."""

    @abstractmethod
    def upstream_trouves(self) -> list[TrouveAbc]:
        """Give the Trouves that this Trouve reads, in a stable order.

        A SQL Trouve gives an empty list. Its dependencies come from the
        placeholder tokens in its SQL, and discovery reads those tokens.
        """

    def __format__(self, _spec: str) -> str:
        """Give a placeholder token for f-string SQL.

        Python calls this method when you write ``f"SELECT * FROM
        {other_trouve}"``. The method adds the Trouve to the global refs
        registry. Then it gives a token, for example
        ``__CLAIR_TROUVE_140234567890__``. Discovery replaces the token with the
        true full_name.
        """
        return register(self)

    @property
    def is_compiled(self) -> bool:
        """True if discovery processed this Trouve."""
        return self.compiled is not None

    @property
    def full_name(self) -> str:
        """The full Snowflake object name: database.schema.table.

        This property raises a RuntimeError if you read it before discovery runs.
        """
        if self.compiled is None:
            raise RuntimeError(
                "Trouve.full_name is not set. "
                "The discovery layer of clair did not load this Trouve."
            )
        return self.compiled.full_name

    def sample(self) -> str:
        """Give a subquery that reads a sample of this Trouve, for test SQL.

        The default result is ``(SELECT TOP 1000 * FROM {full_name})``. To change
        how clair takes the sample, override this method in a subclass.
        """
        assert self.compiled is not None, "sample() needs a compiled Trouve"
        return f"(SELECT TOP 1000 * FROM {self.compiled.full_name})"

    def get_full_table_name(self) -> str:
        """An alias for .full_name. Use it in f-string SQL."""
        return self.full_name


class Trouve(TrouveAbc):
    """A Trouve that Snowflake materializes from SQL.

    Attributes:
        sql: The SQL query. A TABLE or a VIEW needs it. A SOURCE must leave it
             empty. To point to a different Trouve, write
             ``f"SELECT * FROM {other_trouve}"``. Discovery replaces the
             f-string placeholder with the true full_name.

    ``TrouveAbc`` holds the attributes that every backend shares.
    """

    sql: str = Field(default="", exclude=True)

    @property
    def execution_type(self) -> ExecutionType:
        return ExecutionType.SNOWFLAKE

    def upstream_trouves(self) -> list[TrouveAbc]:
        """Give an empty list. Discovery reads the SQL placeholder tokens."""
        return []

    @model_validator(mode="after")
    def _validate_sql(self) -> Trouve:
        if self.type in (TrouveType.TABLE, TrouveType.VIEW) and not self.sql.strip():
            raise ValueError(
                f"a Trouve with the type '{self.type.value}' must have sql"
            )
        if self.type == TrouveType.SOURCE and self.sql.strip():
            raise ValueError("a SOURCE Trouve must not have sql")
        if self.run_config.run_mode == RunMode.INCREMENTAL and self.type != TrouveType.TABLE:
            raise ValueError("only a TABLE Trouve can use the incremental mode")
        return self

    def build_sql(self, effective_mode: RunMode, run_id: str) -> list[str]:
        """Make the SQL statements that materialize this Trouve.

        Args:
            effective_mode: The final run mode. The caller selects it.
            run_id: The unique identifier of this clair run.

        Returns:
            The SQL statements to execute, in order. The list is empty for a
            SOURCE Trouve.

        Raises:
            RuntimeError: If clair did not compile the Trouve.
            ValueError: If the config asks for UPSERT but has no columns.
        """
        if not self.is_compiled:
            raise RuntimeError("build_sql() needs a compiled Trouve")
        assert self.compiled is not None

        if self.type == TrouveType.SOURCE:
            return []

        resolved_sql = self.compiled.resolved_sql.strip()

        if effective_mode == RunMode.FULL_REFRESH:
            object_type = "TABLE" if self.type == TrouveType.TABLE else "VIEW"
            return [
                f"CREATE OR REPLACE {object_type} {self.full_name} AS (\n{resolved_sql}\n)"
            ]

        if self.run_config.incremental_mode == IncrementalMode.APPEND:
            return [
                f"INSERT INTO {self.full_name}\nSELECT * FROM (\n{resolved_sql}\n)"
            ]

        # The UPSERT mode.
        if not self.columns:
            raise ValueError(
                "the upsert mode needs columns on the Trouve"
            )

        staging_name = f"{self.full_name}__clair_staging_{run_id}"
        all_col_names = [c.name for c in self.columns]
        unique_keys = set(self.run_config.primary_key_columns or [])

        upsert_config = self.run_config.upsert_config

        if self.run_config.join_sql:
            join_condition = self.run_config.join_sql
            update_cols = upsert_config.update_columns if upsert_config and upsert_config.update_columns is not None else all_col_names
        else:
            join_condition = " AND ".join(
                f"{TARGET}.{col} = {SOURCE}.{col}" for col in (self.run_config.primary_key_columns or [])
            )
            update_cols = upsert_config.update_columns if upsert_config and upsert_config.update_columns is not None else [c for c in all_col_names if c not in unique_keys]

        insert_col_names = upsert_config.insert_columns if upsert_config and upsert_config.insert_columns is not None else all_col_names

        update_clause = ", ".join(f"{c} = {SOURCE}.{c}" for c in update_cols)
        all_columns = ", ".join(insert_col_names)
        all_source_columns = ", ".join(f"{SOURCE}.{c}" for c in insert_col_names)

        stmt_1 = (
            f"-- [1/3] create the staging table\n"
            f"CREATE OR REPLACE TABLE {staging_name} AS (\n{resolved_sql}\n)"
        )
        stmt_2 = (
            f"-- [2/3] merge the staging table into the target table\n"
            f"MERGE INTO {self.full_name} AS {TARGET}\n"
            f"USING {staging_name} AS {SOURCE}\n"
            f"ON {join_condition}\n"
            f"WHEN MATCHED THEN UPDATE SET {update_clause}\n"
            f"WHEN NOT MATCHED THEN INSERT ({all_columns}) VALUES ({all_source_columns})"
        )
        stmt_3 = (
            f"-- [3/3] drop the staging table\n"
            f"DROP TABLE IF EXISTS {staging_name}"
        )

        return [stmt_1, stmt_2, stmt_3]
