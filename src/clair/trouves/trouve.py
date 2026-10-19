"""Trouve -- the basic unit in Clair.

One Trouve maps to one object in Snowflake that you can query: a source table,
a transformed table, or a view. Each Trouve stays in its own .py file. The
framework finds each Trouve automatically.

This module holds the base class and the classes of the SQL backend:

* ``TrouveAbc`` -- the abstract base. It holds the attributes that every Trouve
  shares: the columns, the tests, the docs, and the compiled attributes.
* ``Source`` -- a table that clair reads, and never writes.
* ``SqlTrouve`` -- the base of ``Table`` and ``View``. Snowflake materializes
  each one from SQL.

The DataFrame backends stay in ``pandas_trouve.py`` and ``seed_trouve.py``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field, computed_field, model_validator

from clair.exceptions import CompileError
from clair.trouves._refs import register
from clair.trouves.address import TrouveAddress
from clair.trouves.column import Column
from clair.trouves.run_config import (
    SOURCE,
    TARGET,
    Append,
    DeleteInsert,
    FullRefresh,
    Incremental,
    RunConfig,
    RunMode,
    Upsert,
)
from clair.trouves.test import AnyTest


class BuildPlan(BaseModel):
    """The SQL statements that materialize one Trouve.

    Attributes:
        statements: The statements that write the object, in order. The runner
            stops at the first statement that fails.
        cleanup_statements: The statements that remove each object that the
            statements make for one run only, for example a temporary table.
            The runner executes them after the statements, when the statements
            pass and when one of them fails. Thus a cleanup statement must be
            correct after any statement, for example ``DROP TABLE IF EXISTS``.
    """

    statements: list[str] = []
    cleanup_statements: list[str] = []

    @property
    def all_statements(self) -> list[str]:
        """Give each statement in the order of a run that passes."""
        return [*self.statements, *self.cleanup_statements]


class UpsertPlan(BaseModel):
    """The decisions that the MERGE of an UPSERT run needs.

    The plan holds columns and names, and it holds no SQL text. The properties
    make the clauses, thus the semantics and the format stay apart.

    Attributes:
        merge_source_address: The table that holds the new rows of this run.
        join_condition: The ON condition of the MERGE.
        update_column_names: The columns that a row that matches receives.
        insert_column_names: The columns that a row that does not match gets.
    """

    merge_source_address: str
    join_condition: str
    update_column_names: list[str]
    insert_column_names: list[str]

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


# An UPSERT and a DELETE_INSERT first write the new rows to a table, and the
# MERGE or the DELETE reads it. This suffix and the run_id make the name of that
# table. It is not the staging suffix of clair.core.staging: the two objects
# hold different data, and an incremental run makes both.
NEW_ROWS_SUFFIX = "__clair_new_rows_"


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
    """The base class of every Trouve.

    Each concrete class is one kind of object. ``Source`` is a table that clair
    reads and never writes. ``Table`` and ``View`` are objects that Snowflake
    makes from SQL. ``PandasTrouve`` and ``SeedTrouve`` are tables that clair
    writes from a DataFrame. Each class gives its own ``trouve_type``,
    ``execution_type`` and ``upstream_trouves``.

    Attributes:
        columns: The column definitions, for the docs and for future checks.
        tests: The data quality tests.
        docs: The documentation text for this Trouve.
        compiled: Discovery sets this. It stays None until discovery reads the
             project.
    """

    # The kind of object. Each concrete class sets it, and no author passes it.
    trouve_type: ClassVar[TrouveType]

    columns: list[Column] = []
    tests: list[AnyTest] = []
    docs: str = ""
    compiled: CompiledAttributes | None = None

    # "forbid" makes a keyword that the class does not read an error, for
    # example a run_config on a View.
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def type(self) -> TrouveType:
        """Give SOURCE, TABLE, or VIEW. The class decides it."""
        return self.trouve_type

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


class Source(TrouveAbc):
    """A table that clair reads, and never writes.

    Another tool loads it. A Trouve reads it with ``f"SELECT * FROM {source}"``.
    """

    trouve_type: ClassVar[TrouveType] = TrouveType.SOURCE

    @property
    def execution_type(self) -> ExecutionType:
        return ExecutionType.SNOWFLAKE

    def upstream_trouves(self) -> list[TrouveAbc]:
        """Give an empty list. A Source reads nothing."""
        return []


class SqlTrouve(TrouveAbc, ABC):
    """The base of each Trouve that Snowflake materializes from SQL: a Table or a View.

    Attributes:
        sql: The SQL query. To point to a different Trouve, write
             ``f"SELECT * FROM {other_trouve}"``. Discovery replaces the
             f-string placeholder with the true physical_address.
    """

    sql: str = Field(exclude=True)

    @property
    def execution_type(self) -> ExecutionType:
        return ExecutionType.SNOWFLAKE

    def upstream_trouves(self) -> list[TrouveAbc]:
        """Give an empty list. Discovery reads the SQL placeholder tokens."""
        return []

    @model_validator(mode="after")
    def _require_sql(self) -> SqlTrouve:
        if not self.sql.strip():
            raise ValueError(f"a {type(self).__name__} must have sql")
        return self

    @abstractmethod
    def build_sql(
        self,
        effective_mode: RunMode,
        run_id: str,
        staging_address: TrouveAddress | None = None,
    ) -> BuildPlan:
        """Make the SQL statements that materialize this Trouve.

        Args:
            effective_mode: The final run mode. The caller selects it.
            run_id: The unique identifier of this clair run.
            staging_address: The staging address, if the run has a staging step.
                The statements write there, and not to the physical address. A
                reference to an upstream Trouve keeps its physical address.

        Returns:
            The statements to execute, and the statements that clean up after
            them.
        """

    def resolved_sql(self) -> str:
        """Give the SQL with each reference resolved. It needs a compiled Trouve."""
        if self.compiled is None:
            raise RuntimeError("build_sql() needs a compiled Trouve")
        return self.compiled.resolved_sql.strip()

    def _full_refresh_plan(self, object_type: str, staging_address: TrouveAddress | None) -> BuildPlan:
        """Give the plan that makes the object again from the SQL."""
        resolved_sql = self.resolved_sql()
        address = staging_address or self.physical_address
        return BuildPlan(statements=[
            f"CREATE OR REPLACE {object_type} {address} AS (\n{resolved_sql}\n)"
        ])


class View(SqlTrouve):
    """A view that Snowflake makes from the SQL. Each run makes it again."""

    trouve_type: ClassVar[TrouveType] = TrouveType.VIEW

    def build_sql(
        self,
        effective_mode: RunMode,
        run_id: str,
        staging_address: TrouveAddress | None = None,
    ) -> BuildPlan:
        """Give CREATE OR REPLACE VIEW. A view has no incremental mode."""
        return self._full_refresh_plan("VIEW", staging_address)


class Table(SqlTrouve):
    """A table that Snowflake makes from the SQL.

    Attributes:
        run_config: FullRefresh, or Incremental with its strategy.
    """

    trouve_type: ClassVar[TrouveType] = TrouveType.TABLE

    run_config: RunConfig = Field(default_factory=FullRefresh)

    def build_sql(
        self,
        effective_mode: RunMode,
        run_id: str,
        staging_address: TrouveAddress | None = None,
    ) -> BuildPlan:
        """Make the SQL statements that materialize this Table.

        See ``SqlTrouve.build_sql`` for the arguments.

        Raises:
            RuntimeError: If clair did not compile the Trouve.
            CompileError: If an incremental run has no built-in strategy.
            ValueError: If the config asks for UPSERT but has no columns.
        """
        if effective_mode == RunMode.FULL_REFRESH:
            return self._full_refresh_plan("TABLE", staging_address)

        resolved_sql = self.resolved_sql()
        address = str(staging_address or self.physical_address)

        strategy = self.incremental_strategy()
        if strategy is None:
            assert self.compiled is not None
            raise CompileError(
                f"{self.compiled.logical_address} runs in the incremental mode, but its "
                "run_config names no strategy. Give Incremental(strategy=...), or "
                "override build_sql() to write the incremental SQL."
            )
        if isinstance(strategy, Append):
            return BuildPlan(statements=[
                f"INSERT INTO {address}\nSELECT * FROM (\n{resolved_sql}\n)"
            ])
        if isinstance(strategy, Upsert):
            return self._upsert_build_plan(address, resolved_sql, run_id)
        if isinstance(strategy, DeleteInsert):
            return self._delete_insert_build_plan(strategy, address, resolved_sql, run_id)
        raise CompileError(f"No incremental plan for the strategy {strategy}.")

    def incremental_strategy(self) -> Append | Upsert | DeleteInsert | None:
        """Give the built-in strategy of an incremental run_config, or None."""
        if isinstance(self.run_config, Incremental):
            return self.run_config.strategy
        return None

    def _upsert_build_plan(self, address: str, resolved_sql: str, run_id: str) -> BuildPlan:
        """Make the plan that merges the new rows into the target table."""
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
        return BuildPlan(
            statements=[create_merge_source, merge],
            cleanup_statements=[drop_merge_source],
        )

    def _delete_insert_build_plan(
        self, strategy: DeleteInsert, address: str, resolved_sql: str, run_id: str
    ) -> BuildPlan:
        """Make the plan that deletes the target rows of the new rows, then inserts the new rows.

        A DELETE_INSERT keeps a duplicate key in the new rows, where a MERGE
        fails. A key on a date column replaces each day that the new rows hold.
        A range column replaces each day from the first to the last day of the
        new rows. Its bounds are scalar subqueries, and Snowflake prunes the
        micro-partitions of the target on them when it compiles the DELETE.
        """
        new_rows_address = self.new_rows_address(run_id)
        create_new_rows = (
            f"-- [1/4] create the new rows table\n"
            f"CREATE OR REPLACE TABLE {new_rows_address} AS (\n{resolved_sql}\n)"
        )
        if strategy.range_column is not None:
            range_column = strategy.range_column
            delete_replaced_rows = (
                f"-- [2/4] delete each target row in the range of the new rows\n"
                f"DELETE FROM {address} AS {TARGET}\n"
                f"WHERE {TARGET}.{range_column} BETWEEN "
                f"(SELECT MIN({range_column}) FROM {new_rows_address}) "
                f"AND (SELECT MAX({range_column}) FROM {new_rows_address})"
            )
        else:
            delete_replaced_rows = (
                f"-- [2/4] delete each target row that a new row matches\n"
                f"DELETE FROM {address} AS {TARGET}\n"
                f"USING {new_rows_address} AS {SOURCE}\n"
                f"WHERE {strategy.match_condition}"
            )
        insert_new_rows = (
            f"-- [3/4] insert the new rows\n"
            f"INSERT INTO {address}\nSELECT * FROM {new_rows_address}"
        )
        drop_new_rows = (
            f"-- [4/4] drop the new rows table\n"
            f"DROP TABLE IF EXISTS {new_rows_address}"
        )
        return BuildPlan(
            statements=[create_new_rows, delete_replaced_rows, insert_new_rows],
            cleanup_statements=[drop_new_rows],
        )

    def new_rows_address(self, run_id: str) -> str:
        """Give the table that holds the new rows of an UPSERT or a DELETE_INSERT.

        It is not the staging address: it holds the new rows only. The name
        comes from the physical address, thus the two suffixes do not go on top
        of each other.

        Args:
            run_id: The unique identifier of this clair run.
        """
        return f"{self.physical_address}{NEW_ROWS_SUFFIX}{run_id}"

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
            TypeError: If the strategy is not Upsert.
            ValueError: If the Trouve has no column.
        """
        strategy = self.incremental_strategy()
        if not isinstance(strategy, Upsert):
            raise TypeError("upsert_plan() needs Incremental(strategy=Upsert(...))")
        if not self.columns:
            raise ValueError("the upsert strategy needs columns on the Trouve")

        all_column_names = [column.name for column in self.columns]
        primary_key_columns = list(strategy.primary_key_columns or [])

        if strategy.update_columns is not None:
            update_column_names = list(strategy.update_columns)
        elif strategy.join_sql:
            # A join of the author names no key column, thus clair cannot
            # remove one. Each column stays in the UPDATE.
            update_column_names = all_column_names
        else:
            update_column_names = [
                column_name
                for column_name in all_column_names
                if column_name not in set(primary_key_columns)
            ]

        if strategy.insert_columns is not None:
            insert_column_names = list(strategy.insert_columns)
        else:
            insert_column_names = all_column_names

        return UpsertPlan(
            merge_source_address=self.new_rows_address(run_id),
            join_condition=strategy.match_condition,
            update_column_names=update_column_names,
            insert_column_names=insert_column_names,
        )
