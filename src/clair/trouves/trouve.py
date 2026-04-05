"""Trouve -- the fundamental unit in Clair.

A Trouve maps 1:1 to a queryable object in Snowflake (a source table,
a transformed table, or a view). Every Trouve lives in its own .py file
and is discovered automatically by the framework.
"""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from clair.trouves._refs import register
from clair.trouves.column import Column
from clair.trouves.run_config import IncrementalMode, RunConfig, RunMode, SOURCE, TARGET
from clair.trouves.test import AnyTest


class TrouveType(StrEnum):
    SOURCE = "source"
    TABLE = "table"
    VIEW = "view"


class ExecutionType(StrEnum):
    SNOWFLAKE = "snowflake"
    PANDAS = "pandas"


class CompiledAttributes(BaseModel):
    """Attributes set by discovery after a Trouve is loaded.

    Only present when ``Trouve.is_compiled`` is True.
    """

    full_name: str       # Routed name -- used in SQL and DDL
    logical_name: str    # Filesystem-derived name -- used in DAG edges and selectors
    resolved_sql: str
    resolved_df_fn: str = ""
    file_path: Path
    module_name: str
    imports: list[str]
    config: Any  # ResolvedConfig -- typed as Any to avoid circular import
    execution_type: ExecutionType


class Trouve(BaseModel):
    """The core class in Clair.

    Attributes:
        type: Whether this is a SOURCE, TABLE, or VIEW.
        sql: The SQL query (required for TABLE/VIEW, must be empty for SOURCE).
             Use ``f"SELECT * FROM {other_trouve}"`` to reference other Trouves --
             the f-string placeholder is resolved to the real full_name by discovery.
        columns: Column definitions for documentation and future validation.
        tests: Data quality tests.
        docs: Documentation string for this Trouve.
        compiled: Set by discovery. None until the project has been discovered.
    """

    type: TrouveType = Field(default=TrouveType.TABLE)
    sql: str = Field(default="", exclude=True)
    df_fn: Any = Field(default=None, exclude=True)
    columns: list[Column] = []
    tests: list[AnyTest] = []
    docs: str = ""
    run_config: RunConfig = Field(default_factory=RunConfig)
    compiled: CompiledAttributes | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def _validate_sql(self) -> Trouve:
        if self.df_fn is not None:
            if not callable(self.df_fn):
                raise ValueError("df_fn must be callable")
            if self.sql.strip():
                raise ValueError("Trouve cannot have both sql and df_fn")
            if self.type != TrouveType.TABLE:
                raise ValueError(f"df_fn Trouves must be TABLE type, got '{self.type.value}'")
            if self.run_config.run_mode == RunMode.INCREMENTAL:
                raise ValueError("df_fn Trouves do not support incremental mode")
            return self

        if self.type in (TrouveType.TABLE, TrouveType.VIEW) and not self.sql.strip():
            raise ValueError(
                f"Trouve of type '{self.type.value}' requires non-empty sql"
            )
        if self.type == TrouveType.SOURCE and self.sql.strip():
            raise ValueError("SOURCE Trouve must not have sql")
        if self.run_config.run_mode == RunMode.INCREMENTAL and self.type != TrouveType.TABLE:
            raise ValueError("only TABLE Trouves support incremental mode")
        return self

    def __format__(self, _spec: str) -> str:
        """Return a placeholder token for use in f-string SQL.

        When you write ``f"SELECT * FROM {other_trouve}"``, this method is called.
        It registers the Trouve in the global refs registry and returns a token
        like ``__CLAIR_TROUVE_140234567890__`` that discovery replaces with the real full_name.
        """
        return register(self)

    @property
    def is_compiled(self) -> bool:
        """True if this Trouve has been processed by discovery."""
        return self.compiled is not None

    @property
    def full_name(self) -> str:
        """The fully-qualified Snowflake object name: database.schema.table.

        Raises RuntimeError if accessed before discovery has run.
        """
        if self.compiled is None:
            raise RuntimeError(
                "Trouve.full_name is not set. "
                "This Trouve was not loaded by clair's discovery layer."
            )
        return self.compiled.full_name

    def sample(self) -> str:
        """Return a sampled subquery of this Trouve for use in test SQL.

        Returns ``(SELECT TOP 1000 * FROM {full_name})`` by default.
        Override in a subclass to customise the sampling strategy.
        """
        assert self.compiled is not None, "sample() requires a compiled Trouve"
        return f"(SELECT TOP 1000 * FROM {self.compiled.full_name})"

    def build_sql(self, effective_mode: RunMode, run_id: str) -> list[str]:
        """Generate the SQL statements to materialize this Trouve.

        Args:
            effective_mode: The resolved run mode (caller determines this).
            run_id: Unique identifier for this clair run invocation.

        Returns:
            Ordered list of SQL statements to execute. Empty for SOURCE Trouves.

        Raises:
            RuntimeError: If the Trouve has not been compiled.
            ValueError: If UPSERT is requested but columns are not defined.
        """
        if not self.is_compiled:
            raise RuntimeError("build_sql() requires a compiled Trouve")
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

        # UPSERT
        if not self.columns:
            raise ValueError(
                "upsert mode requires columns to be defined on the Trouve"
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
            f"-- [1/3] create staging table\n"
            f"CREATE OR REPLACE TABLE {staging_name} AS (\n{resolved_sql}\n)"
        )
        stmt_2 = (
            f"-- [2/3] merge into target\n"
            f"MERGE INTO {self.full_name} AS {TARGET}\n"
            f"USING {staging_name} AS {SOURCE}\n"
            f"ON {join_condition}\n"
            f"WHEN MATCHED THEN UPDATE SET {update_clause}\n"
            f"WHEN NOT MATCHED THEN INSERT ({all_columns}) VALUES ({all_source_columns})"
        )
        stmt_3 = (
            f"-- [3/3] drop staging table\n"
            f"DROP TABLE IF EXISTS {staging_name}"
        )

        return [stmt_1, stmt_2, stmt_3]

    def get_full_table_name(self) -> str:
        """Convenience alias for .full_name. Use inside f-string SQL."""
        return self.full_name
