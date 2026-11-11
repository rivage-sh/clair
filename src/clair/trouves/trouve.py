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

The DataFrame backends stay in ``pandas_table.py`` and ``seed_table.py``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import StrEnum
from pathlib import Path
from typing import ClassVar, Generic, Self

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, computed_field, field_validator, model_validator

# typing.TypeVar takes a default from Python 3.13 only.
from typing_extensions import TypeVar

from clair.core import project_imports
from clair.environments.project_routing import find_project_root, routing_entry_of
from clair.environments.routing import route
from clair.exceptions import CompileError, TrouveFileNotFoundError, TrouveNotCompiledError
from clair.trouves._refs import THIS_PLACEHOLDER, register
from clair.trouves.address import TrouveAddress
from clair.trouves.column import Column
from clair.trouves.config import ResolvedConfig, SessionConfig, TableDefaults
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
from clair.trouves.trouve_type import TrouveType


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


class ExecutionType(StrEnum):
    SNOWFLAKE = "snowflake"
    PANDAS = "pandas"


# An UPSERT and a DELETE_INSERT first write the new rows to a table, and the
# MERGE or the DELETE reads it. This suffix and the run_id make the name of that
# table. It is not the staging suffix of clair.core.staging: the two objects
# hold different data, and an incremental run makes both.
NEW_ROWS_SUFFIX = "__clair_new_rows_"


def quote_column_name(column_name: str) -> str:
    """Give the column name as a quoted identifier.

    The warehouse gives each name as it stores it, for example ``ORDER_DATE``.
    A quoted identifier matches that name exactly, also when the name is a
    reserved word or holds lower case letters.
    """
    escaped_name = column_name.replace('"', '""')
    return f'"{escaped_name}"'


class Iceberg(BaseModel):
    """The settings of a Snowflake-managed Apache Iceberg table.

    Snowflake is the catalog. Snowflake writes the data and the metadata as
    Parquet and Iceberg files to the external volume, and an external engine
    can read them.

    Attributes:
        external_volume: The external volume that holds the files. None takes
            the default of the schema, the database or the account. Without
            one of those, Snowflake uses ``SNOWFLAKE_MANAGED``, its own storage.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    external_volume: str | None = None

    @field_validator("external_volume")
    @classmethod
    def _reject_an_empty_name(cls, external_volume: str | None) -> str | None:
        if external_volume is not None and not external_volume.strip():
            raise ValueError("external_volume is empty. Give a name, or None for the default.")
        return external_volume


class TableOptions(TableDefaults):
    """The options of each Trouve that makes a TABLE: ``Table``, ``PandasTable``, ``SeedTable``.

    ``clair.core.staging`` tells how a run keeps each option on the physical table.

    Attributes:
        transient: True makes a TRANSIENT table, and False makes a permanent
            table. Snowflake keeps no Fail-safe copy of a transient table, and
            its Time Travel lasts one day maximum. The table costs less
            storage, and it can lose data that a permanent table keeps. None
            takes the value of ``__schema_config__.py``, then of
            ``__database_config__.py``, and then False. ``is_transient`` gives
            the result.
        cluster_by: The clustering key, one column name or SQL expression for
            each part, for example ``["order_date", "customer_id"]``. Snowflake
            then reclusters the table in the background. An empty list gives
            no key.
    """

    cluster_by: list[str] = Field(default_factory=list)

    @field_validator("cluster_by")
    @classmethod
    def _reject_an_empty_expression(cls, cluster_by: list[str]) -> list[str]:
        if any(not expression.strip() for expression in cluster_by):
            raise ValueError(
                "cluster_by holds an empty expression. Give a column name or an SQL expression."
            )
        return cluster_by

    def iceberg_settings(self) -> Iceberg | None:
        """Give the Iceberg settings. Only a ``Table`` can make an Iceberg table."""
        return None

    @abstractmethod
    def require_compiled(self) -> CompiledAttributes:
        """``TrouveAbc`` gives this method. Each class with table options is a Trouve."""

    @property
    def is_transient(self) -> bool:
        """True if a run makes a TRANSIENT table.

        Discovery resolves ``transient`` against ``__schema_config__.py`` and
        ``__database_config__.py``. Thus the value exists only after discovery.

        Raises:
            TrouveNotCompiledError: If discovery did not load this Trouve.
        """
        return self.require_compiled().config.transient

    @property
    def table_keyword(self) -> str:
        """Give the object keyword of a CREATE statement.

        ``TABLE``, ``TRANSIENT TABLE`` or ``ICEBERG TABLE``.
        """
        if self.iceberg_settings() is not None:
            return "ICEBERG TABLE"
        return "TRANSIENT TABLE" if self.is_transient else "TABLE"

    @property
    def object_keyword(self) -> str:
        """Give the object keyword of an ALTER statement: ``TABLE`` or ``ICEBERG TABLE``."""
        return "TABLE" if self.iceberg_settings() is None else "ICEBERG TABLE"

    @property
    def create_parameters(self) -> str:
        """Give the parameters of a CREATE TABLE AS SELECT, after the table name.

        A standard table needs none. An Iceberg table names its catalog, and
        its external volume when the Trouve gives one.
        """
        iceberg = self.iceberg_settings()
        if iceberg is None:
            return ""
        parameters = ""
        if iceberg.external_volume is not None:
            escaped_volume = iceberg.external_volume.replace("'", "''")
            parameters += f"\nEXTERNAL_VOLUME = '{escaped_volume}'"
        return parameters + "\nCATALOG = 'SNOWFLAKE'"


def table_options_of(trouve: TrouveAbc) -> TableOptions | None:
    """Give the table options of the Trouve. A View and a Source have none."""
    return trouve if isinstance(trouve, TableOptions) else None


# The rows that a sampled run reads from an input that has no sample_sql.
DEFAULT_SAMPLE_ROWS = 1_000


class DataframeInput(BaseModel):
    """One input of a DataFrame Trouve, as the runner reads it.

    Attributes:
        address: The address of the input.
        sample_query: The subquery that a sampled run reads in place of the
            address. None reads each row of the address.
    """

    address: str
    sample_query: str | None = None


class CompiledAttributes(BaseModel):
    """The record of one discovery: what clair decided for this Trouve in one run.

    These attributes exist only when ``TrouveAbc.is_compiled`` is True. The
    environment, the selection and the run mode of the run decide them, thus a
    different run gives different values. What the file alone decides, such as
    ``TrouveAbc.logical_address``, is an attribute of the Trouve itself.

    A ``Source`` holds these fields only. ``CompiledSql`` and
    ``CompiledDataframe`` add the fields of one backend.
    """

    # The address that the routing entry of this run gives. Clair writes here.
    physical_address: TrouveAddress
    file_path: Path
    module_name: str
    imports: list[str]
    config: ResolvedConfig


class CompiledSql(CompiledAttributes):
    """The compiled attributes of a ``SqlTrouve``: a Table or a View.

    Attributes:
        resolved_sql: The SQL of the author, with an address in place of each
            reference token.
        full_refresh_sql: The resolved SQL that the module gives when
            ``clair.run_mode`` is ``FULL_REFRESH``. An incremental run sets it,
            because a node of that run can make its object again: the table
            does not exist yet, the Trouve is a VIEW, or its run_config is
            ``FullRefresh``. None means that ``resolved_sql`` is that SQL.
    """

    resolved_sql: str
    full_refresh_sql: str | None = None


class CompiledDataframe(CompiledAttributes):
    """The compiled attributes of a ``DataframeTrouve``.

    Attributes:
        resolved_transform: The text that shows how the Trouve makes its rows.
            ``clair compile`` writes it to the artifact file.
        inputs: The read of each input, in the parameter order of the
            transform.
    """

    resolved_transform: str
    inputs: list[DataframeInput]


# The compiled attributes of one kind of Trouve. The default gives the fields that
# each Trouve holds, thus code that reads any Trouve writes TrouveAbc alone.
CompiledT_co = TypeVar(
    "CompiledT_co", bound=CompiledAttributes, default=CompiledAttributes, covariant=True
)


class TrouveAbc(SessionConfig, ABC, Generic[CompiledT_co]):
    """The base class of every Trouve.

    Each concrete class is one kind of object. ``Source`` is a table that clair
    reads and never writes. ``Table`` and ``View`` are objects that Snowflake
    makes from SQL. ``PandasTable`` and ``SeedTable`` are tables that clair
    writes from a DataFrame. Each class gives its own ``trouve_type``,
    ``execution_type`` and ``upstream_trouves``.

    ``SessionConfig`` gives the session fields: ``warehouse``, ``role``,
    ``query_tags`` and ``statement_timeout_seconds``. A value here replaces the
    value of ``__schema_config__.py`` and ``__database_config__.py``.

    Attributes:
        columns: The column definitions, for the docs and for future checks.
        tests: The data quality tests.
        docs: The documentation text for this Trouve.
        sample_sql: The query that a sampled run reads in place of this
            Trouve. Write ``{THIS}`` for the address of this Trouve, for example
            ``f"SELECT * FROM {THIS} WHERE user_id IN (SELECT user_id FROM
            {test_users})"``. None reads a number of rows that Snowflake
            selects. See ``sample()``.
        compiled: Discovery sets this. It stays None until discovery reads the
             project.
    """

    # The kind of object. Each concrete class sets it, and no author passes it.
    trouve_type: ClassVar[TrouveType]

    columns: list[Column] = Field(default_factory=list)
    tests: list[AnyTest] = Field(default_factory=list)
    docs: str = ""
    sample_sql: str | None = Field(default=None, exclude=True)
    compiled: CompiledT_co | None = None

    # The file of an object never changes. A discovery runs each file again, and
    # the module then holds a new object. The cache lets a Trouve that a
    # notebook imported before that discovery keep its address.
    _defining_file_cache: Path | None = PrivateAttr(default=None)

    # "forbid" makes a keyword that the class does not read an error, for
    # example a run_config on a View.
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def type(self) -> TrouveType:
        """Give SOURCE, TABLE, or VIEW. The class decides it."""
        return self.trouve_type

    @model_validator(mode="after")
    def _reject_an_empty_sample_sql(self) -> Self:
        if self.sample_sql is not None and not self.sample_sql.strip():
            raise ValueError("sample_sql is empty. Give a query, or None for the row limit.")
        return self

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
        address that the run reads. See ``recompile_for_selection``.
        """
        return register(self)

    @property
    def is_compiled(self) -> bool:
        """True if discovery processed this Trouve."""
        return self.compiled is not None

    def require_compiled(self) -> CompiledT_co:
        """Give the compiled attributes, the record of the run that loaded this Trouve.

        Raises:
            TrouveNotCompiledError: If discovery did not load this Trouve.
        """
        if self.compiled is None:
            raise TrouveNotCompiledError(type(self).__name__)
        return self.compiled

    @property
    def logical_address(self) -> TrouveAddress:
        """The address that the file path gives: database.schema.table.

        The logical address is the production address. The file alone decides
        it, thus it needs no environment, no routing entry and no run. A Trouve
        that you import in a notebook has it too.

        Raises:
            TrouveFileNotFoundError: If no file defines this Trouve, or two
                files do.
        """
        # Discovery recorded the file of each Trouve that it loaded, thus it
        # needs no search of the module table.
        if self.compiled is not None:
            return TrouveAddress.from_file_path(self.compiled.file_path)
        return TrouveAddress.from_file_path(self._defining_file())

    def get_physical_address(self, env_name: str) -> TrouveAddress:
        """Give the address that the routing entry of *env_name* gives.

        ``clair run --env <env_name>`` writes this Trouve there. The method does
        the steps of a run with no discovery and no connection: it finds the
        project root above the file, loads the entry of ``__routing__.py``, and
        routes the logical address. A routing file that does not name
        *env_name* gives passthrough, and clair warns as a run does.

        Raises:
            TrouveFileNotFoundError: If no file, or two files, define this Trouve.
            ProjectRootNotFoundError: If no directory above the file holds
                ``__routing__.py``.
            InvalidRoutingConfigError: If the routing entry fails.
        """
        file_path = self._defining_file()
        routing = routing_entry_of(find_project_root(file_path.parent), env_name)
        return route(TrouveAddress.from_file_path(file_path), self.type, routing)

    def _defining_file(self) -> Path:
        """Give the absolute path of the one file that defines this Trouve."""
        if self._defining_file_cache is not None:
            return self._defining_file_cache
        file_paths = project_imports.files_that_define(self)
        if len(file_paths) != 1:
            raise TrouveFileNotFoundError(
                type(self).__name__, [str(file_path) for file_path in file_paths]
            )
        self._defining_file_cache = file_paths[0]
        return self._defining_file_cache

    def sample(self, sample_rows: int) -> str:
        """Give the subquery that a sampled run reads in place of this Trouve.

        The result holds the ``{THIS}`` token and the reference tokens of the
        author. Clair renders them with the address that the reader reads.
        The default result is ``sample_sql`` in parentheses. Without it, the
        result reads *sample_rows* rows, and Snowflake selects them: a LIMIT
        without an ORDER BY gives no specific rows. To change the rule for many
        Trouves, override this method in a subclass.
        """
        if self.sample_sql is not None:
            return f"({self.sample_sql})"
        return f"(SELECT * FROM {THIS_PLACEHOLDER} LIMIT {sample_rows})"


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


class SqlTrouve(TrouveAbc[CompiledSql], ABC):
    """The base of each Trouve that Snowflake materializes from SQL: a Table or a View.

    Attributes:
        sql: The SQL query. To point to a different Trouve, write
             ``f"SELECT * FROM {other_trouve}"``. Discovery replaces the
             f-string placeholder with the address that the run reads.
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
        target_address: TrouveAddress,
        target_column_names: list[str] | None = None,
    ) -> BuildPlan:
        """Make the SQL statements that materialize this Trouve.

        Args:
            effective_mode: The final run mode. The caller selects it.
            run_id: The unique identifier of this clair run.
            target_address: The address that the statements write to. The
                runner gives the staging address of a run with a staging step,
                and the physical address of a run without one. A reference to
                an upstream Trouve keeps its own address.
            target_column_names: The columns of the table that an incremental
                run writes to, in the order of the warehouse. The runner reads
                them before it calls this method. It is None in a full refresh,
                and in ``clair compile``, which has no connection.

        Returns:
            The statements to execute, and the statements that clean up after
            them.
        """

    def resolved_sql(self) -> str:
        """Give the SQL with each reference resolved.

        Raises:
            TrouveNotCompiledError: If discovery did not load this Trouve.
        """
        return self.require_compiled().resolved_sql.strip()

    def full_refresh_sql(self) -> str:
        """Give the resolved SQL that makes the object again from nothing.

        A module can read ``clair.run_mode`` and add an incremental filter. A
        node that makes its object again must not keep that filter, because
        the object then holds the filtered rows only.

        Raises:
            TrouveNotCompiledError: If discovery did not load this Trouve.
        """
        compiled = self.require_compiled()
        if compiled.full_refresh_sql is not None:
            return compiled.full_refresh_sql.strip()
        return compiled.resolved_sql.strip()

    def _full_refresh_plan(
        self,
        object_keyword: str,
        target_address: TrouveAddress,
        create_parameters: str = "",
    ) -> BuildPlan:
        """Give the plan that makes the object again from the SQL."""
        resolved_sql = self.full_refresh_sql()
        return BuildPlan(statements=[
            f"CREATE OR REPLACE {object_keyword} {target_address}{create_parameters} AS (\n{resolved_sql}\n)"
        ])


class View(SqlTrouve):
    """A view that Snowflake makes from the SQL. Each run makes it again."""

    trouve_type: ClassVar[TrouveType] = TrouveType.VIEW

    def build_sql(
        self,
        effective_mode: RunMode,
        run_id: str,
        target_address: TrouveAddress,
        target_column_names: list[str] | None = None,
    ) -> BuildPlan:
        """Give CREATE OR REPLACE VIEW. A view has no incremental mode."""
        return self._full_refresh_plan("VIEW", target_address)


class Table(SqlTrouve, TableOptions):
    """A table that Snowflake makes from the SQL.

    Attributes:
        run_config: FullRefresh, or Incremental with its strategy.
        iceberg: Iceberg settings make a Snowflake-managed Iceberg table. None
            makes a standard table.

    ``TableOptions`` gives ``transient`` and ``cluster_by``.
    """

    trouve_type: ClassVar[TrouveType] = TrouveType.TABLE

    run_config: RunConfig = Field(default_factory=FullRefresh)
    iceberg: Iceberg | None = None

    @model_validator(mode="after")
    def _reject_a_transient_iceberg_table(self) -> Table:
        # Snowflake gives no TRANSIENT in the CLONE of an Iceberg table, and
        # each run clones. A transient Iceberg table also needs the
        # SNOWFLAKE_MANAGED volume.
        if self.iceberg is not None and self.transient:
            raise ValueError(
                "A Table cannot be both transient and Iceberg. Give transient=True "
                "or iceberg=Iceberg(...), not both."
            )
        return self

    def iceberg_settings(self) -> Iceberg | None:
        """Give the Iceberg settings of this Table, or None for a standard table."""
        return self.iceberg

    def build_sql(
        self,
        effective_mode: RunMode,
        run_id: str,
        target_address: TrouveAddress,
        target_column_names: list[str] | None = None,
    ) -> BuildPlan:
        """Make the SQL statements that materialize this Table.

        See ``SqlTrouve.build_sql`` for the arguments.

        Raises:
            TrouveNotCompiledError: If discovery did not load this Trouve.
            CompileError: If an incremental run has no built-in strategy.
            ValueError: If the config asks for UPSERT but has no columns.
        """
        if effective_mode == RunMode.FULL_REFRESH:
            return self._full_refresh_plan(
                self.table_keyword, target_address, self.create_parameters
            )

        resolved_sql = self.resolved_sql()
        address = str(target_address)

        strategy = self.incremental_strategy()
        if strategy is None:
            raise CompileError(
                f"{self.logical_address} runs in the incremental mode, but its "
                "run_config names no strategy. Give Incremental(strategy=...), or "
                "override build_sql() to write the incremental SQL."
            )
        if isinstance(strategy, Append):
            return BuildPlan(statements=[
                self.insert_statement(address, f"(\n{resolved_sql}\n)", target_column_names)
            ])
        if isinstance(strategy, Upsert):
            return self._upsert_build_plan(address, resolved_sql, run_id)
        if isinstance(strategy, DeleteInsert):
            return self._delete_insert_build_plan(
                strategy, address, resolved_sql, run_id, target_column_names
            )
        raise CompileError(f"No incremental plan for the strategy {strategy}.")

    def insert_statement(
        self, address: str, new_rows: str, target_column_names: list[str] | None
    ) -> str:
        """Give the INSERT that adds the new rows to the target, column by column name.

        An INSERT without a column list matches the columns by position. Then
        two columns that change places in the SQL swap their values with no
        error. The column list matches each value to its column by name.

        Args:
            address: The table that receives the rows.
            new_rows: The relation that holds the new rows: a table name, or a
                subquery in parentheses.
            target_column_names: The columns of the target, from the warehouse.
                None uses the declared columns. With none of the two, the
                statement matches by position, and a comment says that the run
                names the columns.
        """
        if target_column_names is not None:
            column_list = ", ".join(
                quote_column_name(column_name) for column_name in target_column_names
            )
        elif self.columns:
            column_list = ", ".join(column.name for column in self.columns)
        else:
            return (
                f"INSERT INTO {address}\nSELECT * FROM {new_rows}\n"
                "-- clair run names each column of the target table"
            )
        return f"INSERT INTO {address} ({column_list})\nSELECT {column_list} FROM {new_rows}"

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
        self,
        strategy: DeleteInsert,
        address: str,
        resolved_sql: str,
        run_id: str,
        target_column_names: list[str] | None,
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
            f"{self.insert_statement(address, new_rows_address, target_column_names)}"
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
        return f"{self.require_compiled().physical_address}{NEW_ROWS_SUFFIX}{run_id}"

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
