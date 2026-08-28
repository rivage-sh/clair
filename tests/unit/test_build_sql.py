"""The tests of Trouve.build_sql() and Trouve.upsert_plan().

``upsert_plan`` holds the decisions of a MERGE as columns, thus a test names the
column that clair updates and it splits no statement. The classes at the end
cover the SQL text, because Snowflake reads that text.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from clair.trouves.address import TrouveAddress
from clair.trouves.column import Column, ColumnType
from clair.trouves.config import ResolvedConfig
from clair.trouves.run_config import IncrementalMode, RunConfig, RunMode, UpsertConfig
from clair.trouves.trouve import CompiledAttributes, ExecutionType, Trouve, TrouveType

# The three columns that the upsert tests share.
UPSERT_SQL = "SELECT 1 AS id, 2 AS name, 3 AS value"
UPSERT_COLUMNS = [
    Column(name="id", type=ColumnType.NUMBER),
    Column(name="name", type=ColumnType.STRING),
    Column(name="value", type=ColumnType.NUMBER),
]


def make_compiled_trouve(
    trouve_type: TrouveType = TrouveType.TABLE,
    sql: str = "SELECT 1 AS id",
    physical_address: str = "db.schema.my_table",
    columns: list[Column] | None = None,
    run_config: RunConfig | None = None,
) -> Trouve:
    """Make a compiled Trouve for a test."""
    arguments: dict[str, Any] = {"type": trouve_type, "sql": sql}
    if columns is not None:
        arguments["columns"] = columns
    if run_config is not None:
        arguments["run_config"] = run_config
    if trouve_type == TrouveType.SOURCE:
        arguments.pop("sql", None)

    trouve = Trouve(**arguments)
    address = TrouveAddress.parse(physical_address)
    trouve.compiled = CompiledAttributes(
        physical_address=address,
        logical_address=address,
        resolved_sql="" if trouve_type == TrouveType.SOURCE else sql,
        file_path=Path(f"/fake/{physical_address.replace('.', '/')}.py"),
        module_name=physical_address,
        imports=[],
        config=ResolvedConfig(),
        execution_type=ExecutionType.SNOWFLAKE,
    )
    return trouve


def make_upsert_trouve(**run_config_arguments) -> Trouve:
    """Make a compiled Trouve that runs in the UPSERT mode."""
    return make_compiled_trouve(
        sql=UPSERT_SQL,
        columns=list(UPSERT_COLUMNS),
        run_config=RunConfig(
            run_mode=RunMode.INCREMENTAL,
            incremental_mode=IncrementalMode.UPSERT,
            **run_config_arguments,
        ),
    )


class TestFullRefresh:
    """A full refresh replaces the object each time."""

    @pytest.mark.parametrize(
        ("trouve_type", "expected_object_type"),
        [(TrouveType.TABLE, "TABLE"), (TrouveType.VIEW, "VIEW")],
    )
    def test_each_type_replaces_its_object(self, trouve_type, expected_object_type):
        trouve = make_compiled_trouve(trouve_type=trouve_type)
        statements = trouve.build_sql(RunMode.FULL_REFRESH, run_id="abc")
        assert len(statements) == 1
        assert statements[0].startswith(
            f"CREATE OR REPLACE {expected_object_type} db.schema.my_table AS ("
        )

    def test_the_statement_holds_the_sql_of_the_trouve(self):
        statements = make_compiled_trouve().build_sql(RunMode.FULL_REFRESH, run_id="abc")
        assert "SELECT 1 AS id" in statements[0]

    def test_a_source_makes_no_statement(self):
        """Clair reads a SOURCE, thus it never writes to one."""
        trouve = make_compiled_trouve(trouve_type=TrouveType.SOURCE, sql="")
        assert trouve.build_sql(RunMode.FULL_REFRESH, run_id="abc") == []

    def test_a_trouve_that_clair_did_not_compile_raises(self):
        trouve = Trouve(type=TrouveType.TABLE, sql="SELECT 1 AS id")
        with pytest.raises(RuntimeError, match=r"build_sql\(\) needs a compiled Trouve"):
            trouve.build_sql(RunMode.FULL_REFRESH, run_id="abc")


class TestAppend:
    """An append adds the new rows, and it keeps the rows that the table holds."""

    def test_an_append_makes_one_insert(self):
        trouve = make_compiled_trouve(
            run_config=RunConfig(
                run_mode=RunMode.INCREMENTAL,
                incremental_mode=IncrementalMode.APPEND,
            ),
        )
        statements = trouve.build_sql(RunMode.INCREMENTAL, run_id="abc")
        assert len(statements) == 1
        assert statements[0].startswith("INSERT INTO db.schema.my_table")


class TestUpsertPlanColumns:
    """The plan says which column the MERGE updates, and which it inserts."""

    def test_the_update_leaves_out_each_key_column(self):
        """A key column makes the join, thus the UPDATE must not write to it."""
        plan = make_upsert_trouve(primary_key_columns=["id"]).upsert_plan("abc")
        assert plan.update_column_names == ["name", "value"]

    def test_the_update_leaves_out_each_of_two_key_columns(self):
        plan = make_upsert_trouve(
            primary_key_columns=["id", "name"]
        ).upsert_plan("abc")
        assert plan.update_column_names == ["value"]

    def test_the_insert_holds_each_column(self):
        plan = make_upsert_trouve(primary_key_columns=["id"]).upsert_plan("abc")
        assert plan.insert_column_names == ["id", "name", "value"]

    def test_a_join_of_the_author_updates_each_column(self):
        """The join names no key column, thus clair removes none."""
        plan = make_upsert_trouve(
            join_sql="target.id = source.id"
        ).upsert_plan("abc")
        assert plan.update_column_names == ["id", "name", "value"]

    @pytest.mark.parametrize(
        "run_config_arguments",
        [
            pytest.param({"primary_key_columns": ["id"]}, id="a_key_column"),
            pytest.param(
                {"join_sql": "target.id = source.id"}, id="a_join_of_the_author"
            ),
        ],
    )
    def test_update_columns_wins_against_each_default(self, run_config_arguments):
        plan = make_upsert_trouve(
            **run_config_arguments,
            upsert_config=UpsertConfig(update_columns=["name"]),
        ).upsert_plan("abc")
        assert plan.update_column_names == ["name"]

    def test_insert_columns_limits_the_insert(self):
        plan = make_upsert_trouve(
            primary_key_columns=["id"],
            upsert_config=UpsertConfig(insert_columns=["id", "name"]),
        ).upsert_plan("abc")
        assert plan.insert_column_names == ["id", "name"]

    def test_the_two_settings_work_together(self):
        plan = make_upsert_trouve(
            join_sql="target.id = source.id",
            upsert_config=UpsertConfig(
                update_columns=["name"], insert_columns=["id", "name"]
            ),
        ).upsert_plan("abc")
        assert plan.update_column_names == ["name"]
        assert plan.insert_column_names == ["id", "name"]

    def test_an_empty_update_column_list_updates_nothing(self):
        plan = make_upsert_trouve(
            primary_key_columns=["id"],
            upsert_config=UpsertConfig(update_columns=[]),
        ).upsert_plan("abc")
        assert plan.update_column_names == []

    def test_a_trouve_with_no_column_raises(self):
        trouve = make_compiled_trouve(
            columns=[],
            run_config=RunConfig(
                run_mode=RunMode.INCREMENTAL,
                incremental_mode=IncrementalMode.UPSERT,
                primary_key_columns=["id"],
            ),
        )
        with pytest.raises(ValueError, match="the upsert mode needs columns"):
            trouve.upsert_plan("abc")


class TestUpsertPlanJoin:
    """The join comes from the key columns, or from the author."""

    def test_one_key_column_makes_one_condition(self):
        plan = make_upsert_trouve(primary_key_columns=["id"]).upsert_plan("abc")
        assert plan.join_key_columns == ["id"]
        assert plan.join_condition == "target.id = source.id"

    def test_two_key_columns_join_with_and(self):
        plan = make_upsert_trouve(
            primary_key_columns=["id", "region"]
        ).upsert_plan("abc")
        assert plan.join_condition == (
            "target.id = source.id AND target.region = source.region"
        )

    def test_a_join_of_the_author_is_the_condition(self):
        plan = make_upsert_trouve(join_sql="target.a = source.b").upsert_plan("abc")
        assert plan.join_sql == "target.a = source.b"
        assert plan.join_condition == "target.a = source.b"

    def test_a_key_column_and_a_join_together_raise(self):
        """RunConfig makes the illegal state impossible, thus the plan never sees it."""
        with pytest.raises(ValueError, match="but not both"):
            make_upsert_trouve(
                primary_key_columns=["id"], join_sql="target.a = source.b"
            )

    def test_the_merge_source_holds_the_run_id(self):
        """Two runs at one time must not share the merge source table."""
        plan = make_upsert_trouve(primary_key_columns=["id"]).upsert_plan("abc123")
        assert plan.merge_source_address == (
            "db.schema.my_table__clair_merge_abc123"
        )


class TestUpsertClauses:
    """The plan makes the clauses of the MERGE."""

    def test_the_update_clause_names_the_source_column(self):
        plan = make_upsert_trouve(primary_key_columns=["id"]).upsert_plan("abc")
        assert plan.update_clause == "name = source.name, value = source.value"

    def test_the_insert_clause_names_each_column(self):
        plan = make_upsert_trouve(primary_key_columns=["id"]).upsert_plan("abc")
        assert plan.insert_clause == "id, name, value"

    def test_the_values_clause_reads_the_source(self):
        plan = make_upsert_trouve(primary_key_columns=["id"]).upsert_plan("abc")
        assert plan.insert_values_clause == "source.id, source.name, source.value"


class TestUpsertStatements:
    """The three statements that Snowflake runs for an UPSERT."""

    def test_an_upsert_makes_three_statements(self):
        trouve = make_upsert_trouve(primary_key_columns=["id"])
        assert len(trouve.build_sql(RunMode.INCREMENTAL, run_id="abc")) == 3

    def test_the_first_statement_makes_the_merge_source(self):
        trouve = make_upsert_trouve(primary_key_columns=["id"])
        statements = trouve.build_sql(RunMode.INCREMENTAL, run_id="abc123")
        plan = trouve.upsert_plan("abc123")
        assert f"CREATE OR REPLACE TABLE {plan.merge_source_address}" in statements[0]

    def test_the_second_statement_holds_each_clause_of_the_plan(self):
        trouve = make_upsert_trouve(primary_key_columns=["id"])
        merge = trouve.build_sql(RunMode.INCREMENTAL, run_id="abc123")[1]
        plan = trouve.upsert_plan("abc123")
        assert f"ON {plan.join_condition}" in merge
        assert f"WHEN MATCHED THEN UPDATE SET {plan.update_clause}" in merge
        assert (
            f"WHEN NOT MATCHED THEN INSERT ({plan.insert_clause}) "
            f"VALUES ({plan.insert_values_clause})" in merge
        )

    def test_the_third_statement_drops_the_merge_source(self):
        trouve = make_upsert_trouve(primary_key_columns=["id"])
        statements = trouve.build_sql(RunMode.INCREMENTAL, run_id="abc123")
        plan = trouve.upsert_plan("abc123")
        assert statements[2].endswith(
            f"DROP TABLE IF EXISTS {plan.merge_source_address}"
        )

    def test_the_merge_writes_to_the_staging_address(self):
        """A staging run merges into the candidate, and not into the table."""
        trouve = make_upsert_trouve(primary_key_columns=["id"])
        staging_address = TrouveAddress.parse("db.schema.my_table__clair_stg")
        merge = trouve.build_sql(
            RunMode.INCREMENTAL, run_id="abc", staging_address=staging_address
        )[1]
        assert "MERGE INTO db.schema.my_table__clair_stg AS target" in merge
