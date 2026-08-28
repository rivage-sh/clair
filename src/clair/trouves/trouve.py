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
from clair.trouves.address import TrouveAddress
from clair.trouves.column import Column
from clair.trouves.run_config import SOURCE, TARGET, IncrementalMode, RunConfig, RunMode
from clair.trouves.test import AnyTest


class UpsertPlan(BaseModel):
    """The decisions that the MERGE of an UPSERT run needs.

    The plan holds columns and names, and it holds no SQL text. The properties
    make the clauses, thus the semantics and the format stay apart.

    Attributes:
        merge_source_address: The table that holds the new rows of this run.
        join_sql: The join condition that the author wrote, or None.
        join_key_columns: The key columns that make the join, when the author
            wrote no join_sql.
        update_column_names: The columns that a row that matches receives.
        insert_column_names: The columns that a row that does not match gets.
    """

    merge_source_address: str
    join_sql: str | None
    join_key_columns: list[str]
    update_column_names: list[str]
    insert_column_names: list[str]

    @property
    def join_condition(self) -> str:
        """Give the ON condition of the MERGE."""
        if self.join_sql:
            return self.join_sql
        return " AND ".join(
            f"{TARGET}.{column_name} = {SOURCE}.{column_name}"
            for column_name in self.join_key_columns
        )

    @property
    def update_clause(self) -> str:
        """Give the SET list of the UPDATE."""
        return ", ".join(
            f"{column_name} = {SOURCE}.{column_name}"
            for column_name in self.update_column_names
        )

    @property
    def insert_clause(self) -> str:
        """Give the column list of the INSERT."""
        return ", ".join(self.insert_column_names)

    @property
    def insert_values_clause(self) -> str:
        """Give the VALUES list of the INSERT."""
        return ", ".join(
            f"{SOURCE}.{column_name}" for column_name in self.insert_column_names
        )


class TrouveType(StrEnum):
    SOURCE = "source"
    TABLE = "table"
    VIEW = "view"


class ExecutionType(StrEnum):
    SNOWFLAKE = "snowflake"
    PANDAS = "pandas"


# The MERGE of an UPSERT reads a table that holds the new rows. This suffix and
# the run_id make the name of that table. It is not the staging suffix of
# clair.core.staging: the two objects hold different data, and an incremental
# run makes both.
MERGE_SUFFIX = "__clair_merge_"


class CompiledAttributes(BaseModel):
    """The attributes that discovery sets after it loads a Trouve.

    These attributes exist only when ``TrouveAbc.is_compiled`` is True.
    """

    # The address that clair writes to. Clair puts it in the SQL and the DDL.
    physical_address: TrouveAddress
    # The address that the file path gives. DAG edges and selectors use it.
    logical_address: TrouveAddress
    resolved_sql: str
    resolved_transform: str = ""
    # The address that each input of a PANDAS Trouve reads, in the parameter
    # order of the transform. A SQL Trouve keeps its addresses in resolved_sql,
    # thus this list stays empty for it.
    input_addresses: list[str] = []
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
        true physical_address.
        """
        return register(self)

    @property
    def is_compiled(self) -> bool:
        """True if discovery processed this Trouve."""
        return self.compiled is not None

    @property
    def physical_address(self) -> TrouveAddress:
        """The address of this Trouve in the warehouse: database.schema.table.

        This property raises a RuntimeError if you read it before discovery runs.
        """
        if self.compiled is None:
            raise RuntimeError(
                "Trouve.physical_address is not set. "
                "The discovery layer of clair did not load this Trouve."
            )
        return self.compiled.physical_address

    def sample(self) -> str:
        """Give a subquery that reads a sample of this Trouve, for test SQL.

        The default result is ``(SELECT TOP 1000 * FROM {physical_address})``. To change
        how clair takes the sample, override this method in a subclass.
        """
        assert self.compiled is not None, "sample() needs a compiled Trouve"
        return f"(SELECT TOP 1000 * FROM {self.compiled.physical_address})"

    def get_full_table_name(self) -> str:
        """The physical address as a string. Use it in f-string SQL."""
        return str(self.physical_address)


class Trouve(TrouveAbc):
    """A Trouve that Snowflake materializes from SQL.

    Attributes:
        sql: The SQL query. A TABLE or a VIEW needs it. A SOURCE must leave it
             empty. To point to a different Trouve, write
             ``f"SELECT * FROM {other_trouve}"``. Discovery replaces the
             f-string placeholder with the true physical_address.

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

    def build_sql(
        self,
        effective_mode: RunMode,
        run_id: str,
        staging_address: TrouveAddress | None = None,
    ) -> list[str]:
        """Make the SQL statements that materialize this Trouve.

        Args:
            effective_mode: The final run mode. The caller selects it.
            run_id: The unique identifier of this clair run.
            staging_address: The staging address, if the run has a staging step.
                The statements write there, and not to the physical address. A
                reference to an upstream Trouve keeps its physical address.

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
        address = str(staging_address) if staging_address else self.physical_address

        if effective_mode == RunMode.FULL_REFRESH:
            object_type = "TABLE" if self.type == TrouveType.TABLE else "VIEW"
            return [
                f"CREATE OR REPLACE {object_type} {address} AS (\n{resolved_sql}\n)"
            ]

        if self.run_config.incremental_mode == IncrementalMode.APPEND:
            return [
                f"INSERT INTO {address}\nSELECT * FROM (\n{resolved_sql}\n)"
            ]

        plan = self.upsert_plan(run_id)
        create_merge_source = (
            f"-- [1/3] create the merge source table\n"
            f"CREATE OR REPLACE TABLE {plan.merge_source_address} AS "
            f"(\n{resolved_sql}\n)"
        )
        merge = (
            f"-- [2/3] merge the source table into the target table\n"
            f"MERGE INTO {address} AS {TARGET}\n"
            f"USING {plan.merge_source_address} AS {SOURCE}\n"
            f"ON {plan.join_condition}\n"
            f"WHEN MATCHED THEN UPDATE SET {plan.update_clause}\n"
            f"WHEN NOT MATCHED THEN INSERT ({plan.insert_clause}) "
            f"VALUES ({plan.insert_values_clause})"
        )
        drop_merge_source = (
            f"-- [3/3] drop the merge source table\n"
            f"DROP TABLE IF EXISTS {plan.merge_source_address}"
        )
        return [create_merge_source, merge, drop_merge_source]

    def upsert_plan(self, run_id: str) -> UpsertPlan:
        """Give the decisions that the MERGE of an UPSERT run needs.

        The plan holds the columns and the join, and it holds no SQL text. Thus
        a caller reads which column clair updates, and it parses no statement.

        Args:
            run_id: The unique identifier of this clair run. It makes the name
                of the merge source table.

        Returns:
            The plan of the MERGE.

        Raises:
            ValueError: If the Trouve has no column.
        """
        if not self.columns:
            raise ValueError("the upsert mode needs columns on the Trouve")

        all_column_names = [column.name for column in self.columns]
        primary_key_columns = list(self.run_config.primary_key_columns or [])
        upsert_config = self.run_config.upsert_config

        if upsert_config and upsert_config.update_columns is not None:
            update_column_names = list(upsert_config.update_columns)
        elif self.run_config.join_sql:
            # A join of the author names no key column, thus clair cannot
            # remove one. Each column stays in the UPDATE.
            update_column_names = all_column_names
        else:
            update_column_names = [
                column_name
                for column_name in all_column_names
                if column_name not in set(primary_key_columns)
            ]

        if upsert_config and upsert_config.insert_columns is not None:
            insert_column_names = list(upsert_config.insert_columns)
        else:
            insert_column_names = all_column_names

        # The MERGE needs a source table. It is not the staging address: it
        # holds the new rows only, and the MERGE reads it. The name comes from
        # the physical address, thus the two suffixes do not go on top of each
        # other.
        return UpsertPlan(
            merge_source_address=f"{self.physical_address}{MERGE_SUFFIX}{run_id}",
            join_sql=self.run_config.join_sql,
            join_key_columns=primary_key_columns,
            update_column_names=update_column_names,
            insert_column_names=insert_column_names,
        )
