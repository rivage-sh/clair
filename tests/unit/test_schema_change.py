"""An incremental run compares the columns of the SQL with the columns of the table before it writes.

A new column stops the run, or clair adds it with ADD_NEW_COLUMNS. A removed
column always stops the run. A staged run adds each column to the clone, thus
the physical table keeps its columns until the promotion.
"""

from __future__ import annotations

import pytest

from clair.adapters.base import WarehouseColumn
from clair.core.runner import RunStatus
from clair.core.schema_change import SchemaChange, compare_columns, schema_change_error
from clair.exceptions import RunError
from clair.trouves.address import TrouveAddress
from clair.trouves.run_config import Append, Incremental, OnSchemaChange, RunMode
from clair.trouves.trouve import Table
from tests.helpers import (
    TEST_ADDRESS,
    TEST_RUN_ID,
    RecordingAdapter,
    executed_sql,
    run_one_trouve,
    snowflake_adapter_that_answers,
)

ID = WarehouseColumn(name="ID", data_type="NUMBER(38,0)")
LABEL = WarehouseColumn(name="LABEL", data_type="VARCHAR(16777216)")
DISCOUNT = WarehouseColumn(name="DISCOUNT", data_type="NUMBER(10,2)")
PHYSICAL_ADDRESS = TrouveAddress.parse(TEST_ADDRESS)


def incremental_table(on_schema_change: OnSchemaChange = OnSchemaChange.FAIL) -> Table:
    return Table(
        sql="SELECT 1 AS id, 'a' AS label",
        run_config=Incremental(strategy=Append(), on_schema_change=on_schema_change),
    )


def run_with_columns(
    table_columns: list[WarehouseColumn],
    query_columns: list[WarehouseColumn],
    on_schema_change: OnSchemaChange = OnSchemaChange.FAIL,
    use_staging: bool = True,
):
    adapter = RecordingAdapter(table_columns=table_columns, query_columns=query_columns)
    return run_one_trouve(
        incremental_table(on_schema_change), adapter, run_mode=RunMode.INCREMENTAL, use_staging=use_staging
    )


def alter_statements(sql_texts: list[str]) -> list[str]:
    return [sql for sql in sql_texts if "ALTER TABLE" in sql]


class TestCompareColumns:
    @pytest.mark.parametrize(
        ("table_columns", "query_columns", "expected"),
        [
            pytest.param([ID, LABEL], [ID, LABEL], SchemaChange(), id="the_same"),
            pytest.param([ID, LABEL], [LABEL, ID], SchemaChange(), id="a_new_order_is_no_change"),
            pytest.param([ID], [ID, DISCOUNT], SchemaChange(added_columns=[DISCOUNT]), id="added"),
            pytest.param([ID, LABEL], [ID], SchemaChange(removed_column_names=["LABEL"]), id="removed"),
            pytest.param(
                [ID, LABEL],
                [ID, DISCOUNT],
                SchemaChange(added_columns=[DISCOUNT], removed_column_names=["LABEL"]),
                id="a_rename_is_a_removal_and_an_addition",
            ),
        ],
    )
    def test_it_compares_by_name(self, table_columns, query_columns, expected):
        assert compare_columns(table_columns, query_columns) == expected


class TestThePolicy:
    @pytest.mark.parametrize(
        ("change", "on_schema_change", "stops"),
        [
            (SchemaChange(), OnSchemaChange.FAIL, False),
            (SchemaChange(added_columns=[DISCOUNT]), OnSchemaChange.FAIL, True),
            (SchemaChange(added_columns=[DISCOUNT]), OnSchemaChange.ADD_NEW_COLUMNS, False),
            (SchemaChange(removed_column_names=["LABEL"]), OnSchemaChange.FAIL, True),
            (SchemaChange(removed_column_names=["LABEL"]), OnSchemaChange.ADD_NEW_COLUMNS, True),
        ],
    )
    def test_only_a_permitted_change_continues(self, change, on_schema_change, stops):
        error = schema_change_error(change, on_schema_change, PHYSICAL_ADDRESS)
        assert (error is not None) == stops

    def test_the_default_is_fail(self):
        assert Incremental().on_schema_change == OnSchemaChange.FAIL


class TestTheRun:
    def test_a_new_column_with_fail_writes_nothing_and_names_the_column(self):
        result = run_with_columns([ID, LABEL], [ID, LABEL, DISCOUNT])

        assert result.status == RunStatus.FAILURE
        assert "DISCOUNT" in result.error
        assert executed_sql(result) == []

    def test_a_removed_column_writes_nothing_also_with_add_new_columns(self):
        result = run_with_columns([ID, LABEL], [ID], OnSchemaChange.ADD_NEW_COLUMNS)

        assert result.status == RunStatus.FAILURE
        assert "LABEL" in result.error
        assert executed_sql(result) == []

    def test_a_new_column_goes_onto_the_clone_and_into_the_insert(self):
        result = run_with_columns([ID, LABEL], [ID, LABEL, DISCOUNT], OnSchemaChange.ADD_NEW_COLUMNS)
        statements = executed_sql(result)
        staging_address = f"{TEST_ADDRESS}__clair_{TEST_RUN_ID}"

        assert result.status == RunStatus.SUCCESS
        (alter,) = alter_statements(statements)
        assert alter.endswith(f'ALTER TABLE {staging_address} ADD COLUMN "DISCOUNT" NUMBER(10,2)')
        # The clone first, then the ALTER, then the INSERT.
        clone_index = next(index for index, sql in enumerate(statements) if "CLONE" in sql)
        insert_index = next(index for index, sql in enumerate(statements) if "INSERT INTO" in sql)
        assert clone_index < statements.index(alter) < insert_index
        assert '("ID", "LABEL", "DISCOUNT")' in statements[insert_index]

    def test_a_run_with_no_staging_adds_the_column_to_the_physical_table(self):
        result = run_with_columns(
            [ID], [ID, DISCOUNT], OnSchemaChange.ADD_NEW_COLUMNS, use_staging=False
        )

        (alter,) = alter_statements(executed_sql(result))
        assert f"ALTER TABLE {TEST_ADDRESS} ADD COLUMN" in alter

    def test_a_sql_that_the_warehouse_cannot_compile_writes_nothing(self):
        adapter = RecordingAdapter(fail_on=["SELECT 1 AS id"])
        result = run_one_trouve(incremental_table(), adapter, run_mode=RunMode.INCREMENTAL)

        assert result.status == RunStatus.FAILURE
        assert "fails a query" in result.error
        assert executed_sql(result) == []

    def test_a_full_refresh_reads_no_column(self):
        """A full refresh makes the table again from the SQL, thus no column can drift."""
        full_refresh = run_one_trouve(
            incremental_table(), RecordingAdapter(table_columns=[ID], query_columns=[ID, DISCOUNT])
        )
        assert full_refresh.status == RunStatus.SUCCESS


class TestSnowflakeDescribeQuery:
    def test_it_reads_the_result_of_the_query_with_limit_0(self):
        adapter, cursor = snowflake_adapter_that_answers([
            ("ID", "NUMBER(1,0)", "COLUMN"),
            ("LABEL", "VARCHAR(1)", "COLUMN"),
        ])
        columns = adapter.describe_query("SELECT 1 AS id, 'a' AS label")

        assert [column.name for column in columns] == ["ID", "LABEL"]
        assert cursor.sql == "DESCRIBE RESULT 'fake-query-id'"

    def test_a_query_that_fails_raises_with_the_snowflake_error(self):
        adapter, cursor = snowflake_adapter_that_answers(
            [], error=RuntimeError("invalid identifier 'NOPE'")
        )
        with pytest.raises(RunError, match="invalid identifier 'NOPE'"):
            adapter.describe_query("SELECT nope")
        assert cursor.closed
