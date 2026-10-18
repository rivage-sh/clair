"""An incremental insert names each column of the target, thus a new column order cannot swap values.

An INSERT without a column list matches the columns by position. Two columns of
one type that change places in the SQL then swap their values, and Snowflake
gives no error. Each test here states one part of the repair: the runner reads
the columns of the target, and the plan names them.
"""

from __future__ import annotations

import pytest

from clair.adapters.base import WarehouseColumn
from clair.core.runner import RunStatus
from clair.trouves.address import TrouveAddress
from clair.trouves.column import Column, ColumnType
from clair.trouves.run_config import Append, DeleteInsert, Incremental, RunMode
from clair.trouves.trouve import Table, quote_column_name
from tests.helpers import (
    RecordingAdapter,
    compile_trouve,
    executed_sql,
    run_one_trouve,
    snowflake_adapter_that_answers,
)

STRATEGIES = [
    pytest.param(Append(), id="append"),
    pytest.param(DeleteInsert(primary_key_columns=["id"]), id="delete_insert"),
]
# The target holds the columns in the reverse order of the SQL.
SQL = "SELECT 1 AS id, 'a' AS label"
TARGET_COLUMNS = [
    WarehouseColumn(name="LABEL", data_type="VARCHAR(16777216)"),
    WarehouseColumn(name="ID", data_type="NUMBER(38,0)"),
]


def incremental_table(strategy: Append | DeleteInsert, columns: list[Column] | None = None) -> Table:
    return compile_trouve(
        Table(sql=SQL, run_config=Incremental(strategy=strategy), columns=columns or [])
    )


def insert_of(statements: list[str]) -> str:
    (insert,) = [statement for statement in statements if "INSERT INTO" in statement]
    return insert


class TestThePlan:
    @pytest.mark.parametrize("strategy", STRATEGIES)
    def test_the_insert_names_the_columns_of_the_target_in_their_order(self, strategy):
        plan = incremental_table(strategy).build_sql(
            RunMode.INCREMENTAL, run_id="abc", target_column_names=["LABEL", "ID"]
        )
        insert = insert_of(plan.statements)
        assert 'analytics.revenue.orders ("LABEL", "ID")\nSELECT "LABEL", "ID" FROM' in insert

    @pytest.mark.parametrize("strategy", STRATEGIES)
    def test_compile_names_the_declared_columns(self, strategy):
        """clair compile has no connection, thus the declared columns are the best answer."""
        declared = [Column(name="id", type=ColumnType.NUMBER), Column(name="label", type=ColumnType.STRING)]
        plan = incremental_table(strategy, declared).build_sql(RunMode.INCREMENTAL, run_id="abc")
        assert "analytics.revenue.orders (id, label)\nSELECT id, label FROM" in insert_of(plan.statements)

    @pytest.mark.parametrize("strategy", STRATEGIES)
    def test_compile_with_no_declared_column_says_that_the_run_names_them(self, strategy):
        plan = incremental_table(strategy).build_sql(RunMode.INCREMENTAL, run_id="abc")
        assert insert_of(plan.statements).endswith("-- clair run names each column of the target table")

    def test_the_insert_writes_to_the_staging_address(self):
        staging_address = TrouveAddress.parse("analytics.revenue.orders__clair_stg")
        plan = incremental_table(Append()).build_sql(
            RunMode.INCREMENTAL, run_id="abc", staging_address=staging_address, target_column_names=["ID"]
        )
        assert insert_of(plan.statements).startswith(f'INSERT INTO {staging_address} ("ID")')

    @pytest.mark.parametrize(
        ("column_name", "expected_identifier"),
        [
            ("ORDER_DATE", '"ORDER_DATE"'),
            ("order", '"order"'),
            ('say "hi"', '"say ""hi"""'),
        ],
    )
    def test_a_quoted_name_matches_the_stored_name_exactly(self, column_name, expected_identifier):
        assert quote_column_name(column_name) == expected_identifier


class TestTheRun:
    @pytest.mark.parametrize("strategy", STRATEGIES)
    def test_the_run_names_the_columns_that_the_warehouse_holds(self, strategy):
        adapter = RecordingAdapter(table_columns=TARGET_COLUMNS)
        result = run_one_trouve(
            incremental_table(strategy), adapter, run_mode=RunMode.INCREMENTAL
        )

        assert result.status == RunStatus.SUCCESS
        assert '("LABEL", "ID")\nSELECT "LABEL", "ID" FROM' in insert_of(executed_sql(result))

    def test_the_warehouse_columns_win_over_the_declared_columns(self):
        """A declared column is documentation, and it can be stale."""
        stale = [Column(name="old_name", type=ColumnType.STRING)]
        adapter = RecordingAdapter(table_columns=TARGET_COLUMNS)
        result = run_one_trouve(
            incremental_table(Append(), stale), adapter, run_mode=RunMode.INCREMENTAL
        )

        assert "old_name" not in insert_of(executed_sql(result))

    def test_a_target_whose_columns_clair_cannot_read_fails_before_a_write(self):
        adapter = RecordingAdapter(table_columns=None)
        result = run_one_trouve(
            incremental_table(Append()), adapter, run_mode=RunMode.INCREMENTAL
        )

        assert result.status == RunStatus.FAILURE
        assert "cannot read the columns of analytics.revenue.orders" in result.error
        assert executed_sql(result) == []


class TestSnowflakeDescribeColumns:
    def test_it_gives_each_column_in_order_with_its_type(self):
        adapter, cursor = snowflake_adapter_that_answers([
            ("LABEL", "VARCHAR(16777216)", "COLUMN"),
            ("ID", "NUMBER(38,0)", "COLUMN"),
        ])
        columns = adapter.describe_columns(TrouveAddress.parse("analytics.revenue.orders"))

        assert columns == TARGET_COLUMNS
        assert cursor.sql == "DESCRIBE TABLE analytics.revenue.orders"

    def test_a_virtual_column_takes_no_insert_value(self):
        adapter, _ = snowflake_adapter_that_answers([
            ("ID", "NUMBER(38,0)", "COLUMN"),
            ("DOUBLED", "NUMBER(38,0)", "VIRTUAL"),
        ])
        columns = adapter.describe_columns(TrouveAddress.parse("analytics.revenue.orders"))

        assert columns is not None
        assert [column.name for column in columns] == ["ID"]

    def test_a_query_that_fails_gives_none(self):
        adapter, cursor = snowflake_adapter_that_answers(
            [], error=RuntimeError("Table 'ORDERS' does not exist")
        )
        assert adapter.describe_columns(TrouveAddress.parse("analytics.revenue.orders")) is None
        assert cursor.closed
