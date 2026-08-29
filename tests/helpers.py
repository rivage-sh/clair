"""The objects that the tests share: routing entries, DAG builders, an adapter.

Clair ships no concrete ``RoutingEntry``. A user writes one. The entries here
give the tests the shapes that a user writes most often.

``RecordingAdapter`` is a complete ``WarehouseAdapter``, not a mock. It holds
the tables in memory, it answers each method, and it records each statement.
A test that needs the real warehouse belongs in ``tests/integration/``.
"""

from __future__ import annotations

import threading
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from clair.adapters.base import Statement, StatementStatus, WarehouseAdapter
from clair.core.dag import ClairDag
from clair.core.runner import RunResult
from clair.core.test_runner import TestResult
from clair.environments.routing import RoutingEntry, TrouveAddress
from clair.trouves.address import NodeAddresses
from clair.trouves.config import ResolvedConfig
from clair.trouves.seed_trouve import SeedTrouve
from clair.trouves.trouve import (
    CompiledAttributes,
    ExecutionType,
    Trouve,
    TrouveType,
)


class DatabaseOverrideRouting(RoutingEntry):
    """Send every Trouve, a SOURCE too, to one database."""

    environment_name: str = "dev"
    database_name: str

    def route(
        self, trouve_address: TrouveAddress, trouve_type: TrouveType
    ) -> TrouveAddress:
        return trouve_address.model_copy(update={"database_name": self.database_name})


class SchemaIsolationRouting(RoutingEntry):
    """Collapse the three names into one table name under a fixed schema."""

    environment_name: str = "dev"
    database_name: str
    schema_name: str

    def route(
        self, trouve_address: TrouveAddress, trouve_type: TrouveType
    ) -> TrouveAddress:
        collapsed_table_name = (
            f"{trouve_address.database_name}_{trouve_address.schema_name}_"
            f"{trouve_address.table_name}"
        ).upper()
        return TrouveAddress(
            database_name=self.database_name,
            schema_name=self.schema_name,
            table_name=collapsed_table_name,
        )


class SourceAwareRouting(RoutingEntry):
    """Send each TABLE and VIEW to one database, and keep each SOURCE."""

    environment_name: str = "dev"
    database_name: str

    def route(
        self, trouve_address: TrouveAddress, trouve_type: TrouveType
    ) -> TrouveAddress:
        if trouve_type == TrouveType.SOURCE:
            return trouve_address
        return trouve_address.model_copy(update={"database_name": self.database_name})


# ---------------------------------------------------------------------------
# The DAG builders.
#
# A test that examines the DAG, the render tree, or the selectors needs a
# compiled Trouve, and it needs no file on the disk. These two functions make
# one, thus no test file writes a fourth builder.
# ---------------------------------------------------------------------------


def make_compiled_trouve(
    physical_address: str,
    trouve_type: TrouveType = TrouveType.TABLE,
    execution_type: ExecutionType = ExecutionType.SNOWFLAKE,
    imports: list[str] | None = None,
) -> Trouve:
    """Make one compiled Trouve, with no file on the disk."""
    sql = "" if trouve_type == TrouveType.SOURCE else "select 1"
    trouve = Trouve(type=trouve_type, sql=sql)
    address = TrouveAddress.parse(physical_address)
    trouve.compiled = CompiledAttributes(
        physical_address=address,
        logical_address=address,
        resolved_sql=sql,
        file_path=Path(f"/fake/{physical_address.replace('.', '/')}.py"),
        module_name=physical_address,
        imports=imports or [],
        config=ResolvedConfig(),
        execution_type=execution_type,
    )
    return trouve


def make_compiled_seed_trouve(physical_address: str) -> SeedTrouve:
    """Make one compiled SeedTrouve, with no file on the disk."""
    trouve = SeedTrouve(dataframe=pd.DataFrame({"code": ["US", "FR"]}))
    address = TrouveAddress.parse(physical_address)
    trouve.compiled = CompiledAttributes(
        physical_address=address,
        logical_address=address,
        resolved_sql="",
        file_path=Path(f"/fake/{physical_address.replace('.', '/')}.py"),
        module_name=physical_address,
        imports=[],
        config=ResolvedConfig(),
        execution_type=ExecutionType.PANDAS,
    )
    return trouve


def build_dag_of(
    nodes: Sequence[tuple[str, TrouveType]],
    edges: Sequence[tuple[str, str]] = (),
) -> ClairDag:
    """Make a ClairDag from (physical_address, type) pairs and (parent, child) edges."""
    dag = ClairDag()
    for physical_address, trouve_type in nodes:
        dag.add_trouve(make_compiled_trouve(physical_address, trouve_type))
    for parent_address, child_address in edges:
        dag.add_dependency(parent_address, child_address)
    return dag


# ---------------------------------------------------------------------------
# The result builders.
#
# A test that examines a summary needs a result, and it needs no run. These
# builders make one from an address text.
# ---------------------------------------------------------------------------


def addresses_of_text(physical_address: str, staging_address: str | None = None) -> NodeAddresses:
    """Make a NodeAddresses where the logical name and the physical name agree."""
    address = TrouveAddress.parse(physical_address)
    return NodeAddresses(
        logical=address,
        physical=address,
        staging=None if staging_address is None else TrouveAddress.parse(staging_address),
    )


def make_run_result(physical_address: str, **fields: Any) -> RunResult:
    """Make a RunResult at one address. Each other attribute takes its default."""
    return RunResult(addresses=addresses_of_text(physical_address), **fields)


def make_statement(sql: str = "select 1", **fields: Any) -> Statement:
    """Make a Statement that succeeded. Each keyword replaces one attribute."""
    fields.setdefault("status", StatementStatus.SUCCESS)
    return Statement(sql=sql, **fields)


def make_test_result(
    physical_address: str = "db.s.t",
    test_type: str = "unique",
    column_name: str | None = None,
    test_index: int = 0,
    **statement_fields: Any,
) -> TestResult:
    """Make a TestResult. The keywords make the Statement of the test query."""
    return TestResult(
        address=TrouveAddress.parse(physical_address),
        test_index=test_index,
        test_type=test_type,
        column_name=column_name,
        statement=make_statement(**statement_fields),
    )


# ---------------------------------------------------------------------------
# The adapter that the tests share.
# ---------------------------------------------------------------------------


class StatementRecord:
    """The shared record of each adapter of one run.

    A parallel run makes one adapter for each thread. Each adapter writes to
    one record, thus a test reads the statements of the complete run and the
    concurrency that the run reached.

    Attributes:
        statements: Each SQL statement, in execution order.
        adapter_count: The number of adapters that the run made.
        max_concurrent_statements: The largest number of statements that ran at
            one time.
    """

    def __init__(self) -> None:
        self.statements: list[str] = []
        self.adapter_count: int = 1
        self.max_concurrent_statements: int = 0
        self._concurrent = 0
        self._lock = threading.Lock()

    def enter(self, sql: str) -> None:
        with self._lock:
            self.statements.append(sql)
            self._concurrent += 1
            self.max_concurrent_statements = max(
                self.max_concurrent_statements, self._concurrent
            )

    def leave(self) -> None:
        with self._lock:
            self._concurrent -= 1

    def new_adapter(self) -> None:
        with self._lock:
            self.adapter_count += 1

    def statements_that_hold(self, text: str) -> list[str]:
        """Give each statement that holds *text*."""
        return [statement for statement in self.statements if text in statement]


class RecordingAdapter(WarehouseAdapter):
    """A complete WarehouseAdapter that holds its tables in memory.

    The adapter answers each method of the interface. It makes no connection,
    and it parses no SQL: a test tells it which statements fail, and which
    tables exist.

    Args:
        fail_on: A statement that holds one of these texts gives a failed
            Statement. Use the name of a Trouve to fail that Trouve.
        existing_tables: The address of each table that the warehouse holds.
            None means that every table exists, which is the common case.
        dataframes: The DataFrame at each address, by the address text.
        default_dataframe: The DataFrame that a read of an absent address gives.
            None makes an absent address raise a KeyError.
        select_row_count: The number of rows that a SELECT gives. A data
            quality test reads this number: zero rows is a test that passes.
        write_row_count: The number of rows that a statement that writes gives.
        fetch_error: The exception that a read of a DataFrame raises. This
            covers the error path of a warehouse that loses the connection.
        write_error: The exception that a write of a DataFrame raises.
        record: The shared record. None makes a new one.
    """

    def __init__(
        self,
        *,
        fail_on: Sequence[str] = (),
        existing_tables: Sequence[str] | None = None,
        dataframes: dict[str, pd.DataFrame] | None = None,
        default_dataframe: pd.DataFrame | None = None,
        select_row_count: int = 0,
        write_row_count: int = 42,
        fetch_error: Exception | None = None,
        write_error: Exception | None = None,
        record: StatementRecord | None = None,
    ) -> None:
        self.fail_on = list(fail_on)
        self.existing_tables = None if existing_tables is None else set(existing_tables)
        self.dataframes: dict[str, pd.DataFrame] = dict(dataframes or {})
        self.default_dataframe = default_dataframe
        self.select_row_count = select_row_count
        self.write_row_count = write_row_count
        self.fetch_error = fetch_error
        self.write_error = write_error
        self.record = record or StatementRecord()

        self.profile: dict[str, Any] | None = None
        self.contexts: list[dict[str, str | None]] = []
        self.is_open = True
        self.written_addresses: list[str] = []
        self.fetched_addresses: list[str] = []
        self._query_counter = 0

    # The connection.

    def connect(self, profile: dict[str, Any]) -> None:
        self.profile = dict(profile)
        self.is_open = True

    def new_connection(self) -> RecordingAdapter:
        if not self.is_open:
            raise RuntimeError("This adapter holds no open connection.")
        self.record.new_adapter()
        return RecordingAdapter(
            fail_on=self.fail_on,
            existing_tables=None
            if self.existing_tables is None
            else sorted(self.existing_tables),
            dataframes=self.dataframes,
            default_dataframe=self.default_dataframe,
            select_row_count=self.select_row_count,
            write_row_count=self.write_row_count,
            fetch_error=self.fetch_error,
            write_error=self.write_error,
            record=self.record,
        )

    def close(self) -> None:
        self.is_open = False

    def set_context(
        self,
        warehouse: str | None = None,
        role: str | None = None,
        database_name: str | None = None,
    ) -> None:
        self.contexts.append(
            {"warehouse": warehouse, "role": role, "database_name": database_name}
        )

    # The statements.

    def _next_statement(self, sql: str) -> Statement:
        self._query_counter += 1
        query_id = f"qid-{self._query_counter:04d}"
        query_url = f"https://test.snowflake.com/#/query/{query_id}"

        for text in self.fail_on:
            if text in sql:
                return Statement(
                    sql=sql,
                    status=StatementStatus.FAILURE,
                    query_id=query_id,
                    query_url=query_url,
                    error=f"Simulated failure for {text}",
                )

        is_select = sql.lstrip().upper().startswith("SELECT")
        return Statement(
            sql=sql,
            status=StatementStatus.SUCCESS,
            query_id=query_id,
            query_url=query_url,
            row_count=self.select_row_count if is_select else self.write_row_count,
        )

    def execute(self, sql: str) -> Statement:
        self.record.enter(sql)
        try:
            return self._next_statement(sql)
        finally:
            self.record.leave()

    def table_exists(
        self, database_name: str, schema_name: str, table_name: str
    ) -> bool:
        if self.existing_tables is None:
            return True
        return f"{database_name}.{schema_name}.{table_name}" in self.existing_tables

    # The DataFrames.

    def fetch_dataframe(self, address: TrouveAddress) -> pd.DataFrame:
        self.fetched_addresses.append(str(address))
        if self.fetch_error is not None:
            raise self.fetch_error
        if str(address) not in self.dataframes and self.default_dataframe is not None:
            return self.default_dataframe
        return self.dataframes[str(address)]

    def write_dataframe(
        self, dataframe: pd.DataFrame, address: TrouveAddress
    ) -> Statement:
        if self.write_error is not None:
            raise self.write_error
        self.dataframes[str(address)] = dataframe
        self.written_addresses.append(str(address))
        return self.execute(f"-- write_dataframe {address}")
