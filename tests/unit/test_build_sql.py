"""Tests for Trouve.build_sql() method."""

from pathlib import Path
from typing import Any

import pytest

from clair.trouves.column import Column, ColumnType
from clair.trouves.config import ResolvedConfig
from clair.trouves.run_config import IncrementalMode, UpsertConfig, RunConfig, RunMode
from clair.trouves.trouve import CompiledAttributes, ExecutionType, Trouve, TrouveType


def _make_compiled_trouve(
    type: TrouveType = TrouveType.TABLE,
    sql: str = "SELECT 1 AS id",
    full_name: str = "db.schema.my_table",
    columns: list[Column] | None = None,
    run_config: RunConfig | None = None,
) -> Trouve:
    """Create a compiled Trouve for testing."""
    kwargs: dict[str, Any] = {"type": type, "sql": sql}
    if columns is not None:
        kwargs["columns"] = columns
    if run_config is not None:
        kwargs["run_config"] = run_config
    if type == TrouveType.SOURCE:
        kwargs.pop("sql", None)

    t = Trouve(**kwargs)
    t.compiled = CompiledAttributes(
        full_name=full_name,
        logical_name=full_name,
        resolved_sql=sql if type != TrouveType.SOURCE else "",
        file_path=Path(f"/fake/{full_name.replace('.', '/')}.py"),
        module_name=full_name,
        imports=[],
        config=ResolvedConfig(),
        execution_type=ExecutionType.SNOWFLAKE,
    )
    return t


class TestBuildSqlFullRefresh:
    def test_table_creates_create_or_replace_table(self):
        t = _make_compiled_trouve()
        stmts = t.build_sql(RunMode.FULL_REFRESH, run_id="abc")
        assert len(stmts) == 1
        assert "CREATE OR REPLACE TABLE db.schema.my_table" in stmts[0]
        assert "SELECT 1 AS id" in stmts[0]

    def test_view_creates_create_or_replace_view(self):
        t = _make_compiled_trouve(type=TrouveType.VIEW)
        stmts = t.build_sql(RunMode.FULL_REFRESH, run_id="abc")
        assert len(stmts) == 1
        assert "CREATE OR REPLACE VIEW db.schema.my_table" in stmts[0]

    def test_source_returns_empty_list(self):
        t = _make_compiled_trouve(type=TrouveType.SOURCE, sql="")
        stmts = t.build_sql(RunMode.FULL_REFRESH, run_id="abc")
        assert stmts == []


class TestBuildSqlNotCompiled:
    def test_raises_runtime_error(self):
        t = Trouve(type=TrouveType.TABLE, sql="SELECT 1 AS id")
        with pytest.raises(RuntimeError, match="build_sql\\(\\) requires a compiled Trouve"):
            t.build_sql(RunMode.FULL_REFRESH, run_id="abc")


class TestBuildSqlAppend:
    def test_append_produces_insert_into(self):
        t = _make_compiled_trouve(
            run_config=RunConfig(
                run_mode=RunMode.INCREMENTAL,
                incremental_mode=IncrementalMode.APPEND,
            ),
        )
        stmts = t.build_sql(RunMode.INCREMENTAL, run_id="abc")
        assert len(stmts) == 1
        assert "INSERT INTO db.schema.my_table" in stmts[0]
        assert "SELECT * FROM" in stmts[0]


class TestBuildSqlUpsert:
    def _upsert_trouve(self, **run_config_kwargs) -> Trouve:
        rc = RunConfig(
            run_mode=RunMode.INCREMENTAL,
            incremental_mode=IncrementalMode.UPSERT,
            **run_config_kwargs,
        )
        return _make_compiled_trouve(
            sql="SELECT 1 AS id, 2 AS name, 3 AS value",
            columns=[
                Column(name="id", type=ColumnType.NUMBER),
                Column(name="name", type=ColumnType.STRING),
                Column(name="value", type=ColumnType.NUMBER),
            ],
            run_config=rc,
        )

    def test_produces_three_statements(self):
        t = self._upsert_trouve(primary_key_columns=["id"])
        stmts = t.build_sql(RunMode.INCREMENTAL, run_id="abc123")
        assert len(stmts) == 3

    def test_staging_name_contains_run_id(self):
        t = self._upsert_trouve(primary_key_columns=["id"])
        stmts = t.build_sql(RunMode.INCREMENTAL, run_id="abc123")
        assert "__clair_staging_abc123" in stmts[0]

    def test_merge_join_condition_from_primary_key_columns(self):
        t = self._upsert_trouve(primary_key_columns=["id"])
        stmts = t.build_sql(RunMode.INCREMENTAL, run_id="abc123")
        assert "ON target.id = source.id" in stmts[1]

    def test_update_set_excludes_primary_key_columns(self):
        t = self._upsert_trouve(primary_key_columns=["id"])
        stmts = t.build_sql(RunMode.INCREMENTAL, run_id="abc123")
        # UPDATE SET should have name and value, not id
        assert "UPDATE SET name = source.name, value = source.value" in stmts[1]
        assert "id = source.id" not in stmts[1].split("UPDATE SET")[1].split("WHEN")[0]

    def test_insert_includes_all_columns(self):
        t = self._upsert_trouve(primary_key_columns=["id"])
        stmts = t.build_sql(RunMode.INCREMENTAL, run_id="abc123")
        assert "INSERT (id, name, value)" in stmts[1]
        assert "VALUES (source.id, source.name, source.value)" in stmts[1]

    def test_drop_table_in_statement_three(self):
        t = self._upsert_trouve(primary_key_columns=["id"])
        stmts = t.build_sql(RunMode.INCREMENTAL, run_id="abc123")
        assert "DROP TABLE IF EXISTS" in stmts[2]
        assert "__clair_staging_abc123" in stmts[2]

    def test_join_sql_uses_custom_condition(self):
        t = self._upsert_trouve(join_sql="target.id = source.id AND target.region = source.region")
        stmts = t.build_sql(RunMode.INCREMENTAL, run_id="x")
        assert "target.id = source.id AND target.region = source.region" in stmts[1]

    def test_join_sql_updates_all_columns(self):
        t = self._upsert_trouve(join_sql="target.id = source.id")
        stmts = t.build_sql(RunMode.INCREMENTAL, run_id="x")
        # With join_sql, all columns appear in UPDATE SET
        assert "id = source.id, name = source.name, value = source.value" in stmts[1]

    def test_empty_columns_raises_value_error(self):
        rc = RunConfig(
            run_mode=RunMode.INCREMENTAL,
            incremental_mode=IncrementalMode.UPSERT,
            primary_key_columns=["id"],
        )
        t = _make_compiled_trouve(run_config=rc, columns=[])
        with pytest.raises(ValueError, match="upsert mode requires columns"):
            t.build_sql(RunMode.INCREMENTAL, run_id="abc")

    def test_multi_column_primary_key_joins_with_and(self):
        t = self._upsert_trouve(primary_key_columns=["id", "region"])
        stmts = t.build_sql(RunMode.INCREMENTAL, run_id="abc")
        assert "target.id = source.id AND target.region = source.region" in stmts[1]

    def test_upsert_config_update_columns_overrides_join_sql_default(self):
        t = self._upsert_trouve(
            join_sql="target.id = source.id",
            upsert_config=UpsertConfig(update_columns=["name"]),
        )
        stmts = t.build_sql(RunMode.INCREMENTAL, run_id="x")
        assert "UPDATE SET name = source.name" in stmts[1]
        assert "id = source.id" not in stmts[1].split("UPDATE SET")[1].split("WHEN")[0]
        assert "value = source.value" not in stmts[1].split("UPDATE SET")[1].split("WHEN")[0]

    def test_upsert_config_update_columns_overrides_primary_key_default(self):
        t = self._upsert_trouve(
            primary_key_columns=["id"],
            upsert_config=UpsertConfig(update_columns=["name"]),
        )
        stmts = t.build_sql(RunMode.INCREMENTAL, run_id="x")
        assert "UPDATE SET name = source.name" in stmts[1]
        assert "value = source.value" not in stmts[1].split("UPDATE SET")[1].split("WHEN")[0]

    def test_upsert_config_insert_columns_limits_insert(self):
        t = self._upsert_trouve(
            primary_key_columns=["id"],
            upsert_config=UpsertConfig(insert_columns=["id", "name"]),
        )
        stmts = t.build_sql(RunMode.INCREMENTAL, run_id="x")
        assert "INSERT (id, name)" in stmts[1]
        assert "VALUES (source.id, source.name)" in stmts[1]
        assert "value" not in stmts[1].split("INSERT")[1].split("VALUES")[0]

    def test_upsert_config_update_and_insert_columns_together(self):
        t = self._upsert_trouve(
            join_sql="target.id = source.id",
            upsert_config=UpsertConfig(update_columns=["name"], insert_columns=["id", "name"]),
        )
        stmts = t.build_sql(RunMode.INCREMENTAL, run_id="x")
        assert "UPDATE SET name = source.name" in stmts[1]
        assert "INSERT (id, name)" in stmts[1]
        assert "VALUES (source.id, source.name)" in stmts[1]
