"""Comprehensive tests for Trouve, RunConfig, tests, refs, and build_sql."""

from pathlib import Path

import pytest

from clair.trouves._refs import TROUVE_PLACEHOLDER_PREFIX, _registry, clear
from clair.trouves.column import Column, ColumnType
from clair.trouves.config import DatabaseDefaults, ResolvedConfig, SchemaDefaults
from clair.trouves.run_config import (
    IncrementalMode,
    RunConfig,
    RunMode,
    UpsertConfig,
)
from clair.trouves.test import (
    TestNotNull,
    TestRowCount,
    TestUnique,
    TestUniqueColumns,
)
from clair.trouves.trouve import CompiledAttributes, ExecutionType, Trouve, TrouveType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_SQL = "SELECT id, name FROM raw.users"


def _compiled_attrs(
    full_name: str = "db.schema.my_table",
    resolved_sql: str = SAMPLE_SQL,
) -> CompiledAttributes:
    """Build a CompiledAttributes with sensible defaults."""
    return CompiledAttributes(
        full_name=full_name,
        logical_name=full_name,
        resolved_sql=resolved_sql,
        file_path=Path("/fake/db/schema/my_table.py"),
        module_name="db.schema.my_table",
        imports=[],
        config=ResolvedConfig(),
        execution_type=ExecutionType.SNOWFLAKE,
    )


def _compiled_trouve(
    trouve_type: TrouveType = TrouveType.TABLE,
    sql: str = SAMPLE_SQL,
    full_name: str = "db.schema.my_table",
    resolved_sql: str = SAMPLE_SQL,
    columns: list[Column] | None = None,
    run_config: RunConfig | None = None,
) -> Trouve:
    """Build a compiled Trouve ready for build_sql()."""
    trouve = Trouve(
        type=trouve_type,
        sql=sql,
        columns=columns or [],
        run_config=run_config or RunConfig(),
    )
    trouve.compiled = _compiled_attrs(
        full_name=full_name,
        resolved_sql=resolved_sql,
    )
    return trouve


# ---------------------------------------------------------------------------
# Trouve construction and validation
# ---------------------------------------------------------------------------


class TestTrouveConstruction:
    def test_source_trouve_valid(self):
        trouve = Trouve(type=TrouveType.SOURCE)
        assert trouve.type == TrouveType.SOURCE
        assert trouve.sql == ""

    def test_source_trouve_with_columns_and_docs(self):
        trouve = Trouve(
            type=TrouveType.SOURCE,
            columns=[Column(name="id", type=ColumnType.STRING)],
            docs="A source table.",
        )
        assert len(trouve.columns) == 1
        assert trouve.columns[0].name == "id"
        assert trouve.docs == "A source table."

    def test_table_trouve_valid(self):
        trouve = Trouve(type=TrouveType.TABLE, sql="SELECT 1 AS id")
        assert trouve.type == TrouveType.TABLE
        assert trouve.sql == "SELECT 1 AS id"

    def test_view_trouve_valid(self):
        trouve = Trouve(type=TrouveType.VIEW, sql="SELECT 1 AS id")
        assert trouve.type == TrouveType.VIEW

    def test_default_type_is_table(self):
        trouve = Trouve(sql="SELECT 1")
        assert trouve.type == TrouveType.TABLE

    def test_default_run_config_is_full_refresh(self):
        trouve = Trouve(sql="SELECT 1")
        assert trouve.run_config.run_mode == RunMode.FULL_REFRESH
        assert trouve.run_config.incremental_mode is None

    def test_table_rejects_empty_sql(self):
        with pytest.raises(ValueError, match="requires non-empty sql"):
            Trouve(type=TrouveType.TABLE, sql="")

    def test_table_rejects_whitespace_only_sql(self):
        with pytest.raises(ValueError, match="requires non-empty sql"):
            Trouve(type=TrouveType.TABLE, sql="   \n\t  ")

    def test_view_rejects_empty_sql(self):
        with pytest.raises(ValueError, match="requires non-empty sql"):
            Trouve(type=TrouveType.VIEW, sql="")

    def test_source_rejects_sql(self):
        with pytest.raises(ValueError, match="SOURCE Trouve must not have sql"):
            Trouve(type=TrouveType.SOURCE, sql="SELECT 1")

    def test_source_rejects_whitespace_sql(self):
        """Source sql is checked with .strip(), so whitespace-only is fine."""
        trouve = Trouve(type=TrouveType.SOURCE, sql="   ")
        assert trouve.type == TrouveType.SOURCE

    def test_sample_returns_top_subquery(self):
        from pathlib import Path
        from clair.trouves.config import ResolvedConfig
        from clair.trouves.trouve import CompiledAttributes, ExecutionType

        t = Trouve(type=TrouveType.TABLE, sql="select 1")
        t.compiled = CompiledAttributes(
            full_name="db.s.orders",
            logical_name="db.s.orders",
            resolved_sql="select 1",
            file_path=Path("/fake/db/s/orders.py"),
            module_name="db.s.orders",
            imports=[],
            config=ResolvedConfig(),
            execution_type=ExecutionType.SNOWFLAKE,
        )
        assert t.sample() == "(SELECT TOP 1000 * FROM db.s.orders)"

    def test_sample_uses_routed_name_not_logical_name(self):
        """sample() uses compiled.full_name (post-routing), not logical_name."""
        from pathlib import Path
        from clair.trouves.config import ResolvedConfig
        from clair.trouves.trouve import CompiledAttributes, ExecutionType

        t = Trouve(type=TrouveType.TABLE, sql="select 1")
        t.compiled = CompiledAttributes(
            full_name="dev_omer.s.orders",   # routed name (e.g. schema isolation)
            logical_name="db.s.orders",       # original filesystem-derived name
            resolved_sql="select 1",
            file_path=Path("/fake/db/s/orders.py"),
            module_name="db.s.orders",
            imports=[],
            config=ResolvedConfig(),
            execution_type=ExecutionType.SNOWFLAKE,
        )
        result = t.sample()
        assert "dev_omer.s.orders" in result
        assert "db.s.orders" not in result

    def test_sample_raises_when_not_compiled(self):
        t = Trouve(type=TrouveType.TABLE, sql="select 1")
        with pytest.raises(AssertionError, match="requires a compiled Trouve"):
            t.sample()

    def test_trouve_with_tests(self):
        trouve = Trouve(
            type=TrouveType.TABLE,
            sql="SELECT 1 AS id",
            tests=[TestUnique(column="id"), TestNotNull(column="id")],
        )
        assert len(trouve.tests) == 2
        assert trouve.tests[0].type == "unique"
        assert trouve.tests[1].type == "not_null"

    def test_is_compiled_false_by_default(self):
        trouve = Trouve(type=TrouveType.SOURCE)
        assert trouve.is_compiled is False

    def test_is_compiled_true_after_setting_compiled(self):
        trouve = Trouve(type=TrouveType.SOURCE)
        trouve.compiled = _compiled_attrs(resolved_sql="")
        assert trouve.is_compiled is True

    def test_full_name_raises_when_not_compiled(self):
        trouve = Trouve(type=TrouveType.SOURCE)
        with pytest.raises(RuntimeError, match="full_name is not set"):
            _ = trouve.full_name

    def test_full_name_returns_compiled_value(self):
        trouve = Trouve(type=TrouveType.SOURCE)
        trouve.compiled = _compiled_attrs(full_name="prod.analytics.orders")
        assert trouve.full_name == "prod.analytics.orders"


class TestTrouveIncrementalValidation:
    """Validate that incremental mode is only allowed on TABLE Trouves."""

    def test_view_with_incremental_raises(self):
        with pytest.raises(ValueError, match="only TABLE Trouves support incremental mode"):
            Trouve(
                type=TrouveType.VIEW,
                sql="SELECT 1",
                run_config=RunConfig(
                    run_mode=RunMode.INCREMENTAL,
                    incremental_mode=IncrementalMode.APPEND,
                ),
            )

    def test_source_with_incremental_raises(self):
        """SOURCE has no sql, but incremental mode validation fires first via RunConfig.
        Actually SOURCE with incremental will fail because SOURCE requires empty sql,
        and incremental only applies to TABLE. The validator checks run_mode == INCREMENTAL
        and type != TABLE."""
        with pytest.raises(ValueError):
            Trouve(
                type=TrouveType.SOURCE,
                run_config=RunConfig(
                    run_mode=RunMode.INCREMENTAL,
                    incremental_mode=IncrementalMode.APPEND,
                ),
            )

    def test_table_with_incremental_append_valid(self):
        trouve = Trouve(
            type=TrouveType.TABLE,
            sql="SELECT 1",
            run_config=RunConfig(
                run_mode=RunMode.INCREMENTAL,
                incremental_mode=IncrementalMode.APPEND,
            ),
        )
        assert trouve.run_config.run_mode == RunMode.INCREMENTAL

    def test_table_with_incremental_upsert_valid(self):
        trouve = Trouve(
            type=TrouveType.TABLE,
            sql="SELECT 1",
            run_config=RunConfig(
                run_mode=RunMode.INCREMENTAL,
                incremental_mode=IncrementalMode.UPSERT,
                primary_key_columns=["id"],
            ),
        )
        assert trouve.run_config.incremental_mode == IncrementalMode.UPSERT


# ---------------------------------------------------------------------------
# RunConfig validation
# ---------------------------------------------------------------------------


class TestRunConfigValidation:
    def test_default_is_full_refresh(self):
        config = RunConfig()
        assert config.run_mode == RunMode.FULL_REFRESH
        assert config.incremental_mode is None

    def test_full_refresh_with_incremental_mode_raises(self):
        with pytest.raises(ValueError, match="incremental_mode is only valid when run_mode is incremental"):
            RunConfig(
                run_mode=RunMode.FULL_REFRESH,
                incremental_mode=IncrementalMode.APPEND,
            )

    def test_incremental_without_incremental_mode_raises(self):
        with pytest.raises(ValueError, match="incremental run_mode requires incremental_mode"):
            RunConfig(run_mode=RunMode.INCREMENTAL)

    def test_append_with_primary_key_columns_raises(self):
        with pytest.raises(ValueError, match="primary_key_columns is only valid for upsert mode"):
            RunConfig(
                run_mode=RunMode.INCREMENTAL,
                incremental_mode=IncrementalMode.APPEND,
                primary_key_columns=["id"],
            )

    def test_append_with_join_sql_raises(self):
        with pytest.raises(ValueError, match="join_sql is only valid for upsert mode"):
            RunConfig(
                run_mode=RunMode.INCREMENTAL,
                incremental_mode=IncrementalMode.APPEND,
                join_sql="target.id = source.id",
            )

    def test_append_with_upsert_config_raises(self):
        with pytest.raises(ValueError, match="upsert_config is only valid for upsert mode"):
            RunConfig(
                run_mode=RunMode.INCREMENTAL,
                incremental_mode=IncrementalMode.APPEND,
                upsert_config=UpsertConfig(update_columns=["name"]),
            )

    def test_upsert_with_both_primary_key_and_join_sql_raises(self):
        with pytest.raises(ValueError, match="specify primary_key_columns or join_sql, not both"):
            RunConfig(
                run_mode=RunMode.INCREMENTAL,
                incremental_mode=IncrementalMode.UPSERT,
                primary_key_columns=["id"],
                join_sql="target.id = source.id",
            )

    def test_upsert_with_neither_primary_key_nor_join_sql_raises(self):
        with pytest.raises(ValueError, match="upsert mode requires primary_key_columns or join_sql"):
            RunConfig(
                run_mode=RunMode.INCREMENTAL,
                incremental_mode=IncrementalMode.UPSERT,
            )

    def test_upsert_with_primary_key_columns_valid(self):
        config = RunConfig(
            run_mode=RunMode.INCREMENTAL,
            incremental_mode=IncrementalMode.UPSERT,
            primary_key_columns=["id"],
        )
        assert config.primary_key_columns == ["id"]

    def test_upsert_with_join_sql_valid(self):
        config = RunConfig(
            run_mode=RunMode.INCREMENTAL,
            incremental_mode=IncrementalMode.UPSERT,
            join_sql="target.id = source.id AND target.region = source.region",
        )
        assert config.join_sql is not None

    def test_upsert_with_multiple_primary_key_columns(self):
        config = RunConfig(
            run_mode=RunMode.INCREMENTAL,
            incremental_mode=IncrementalMode.UPSERT,
            primary_key_columns=["id", "region"],
        )
        assert config.primary_key_columns == ["id", "region"]

    def test_append_valid(self):
        config = RunConfig(
            run_mode=RunMode.INCREMENTAL,
            incremental_mode=IncrementalMode.APPEND,
        )
        assert config.incremental_mode == IncrementalMode.APPEND


# ---------------------------------------------------------------------------
# Trouve.__format__() and _refs registration
# ---------------------------------------------------------------------------


class TestRefsRegistration:
    def setup_method(self):
        clear()

    def teardown_method(self):
        clear()

    def test_format_returns_placeholder_token(self):
        trouve = Trouve(type=TrouveType.SOURCE)
        token = f"{trouve}"
        assert token.startswith(TROUVE_PLACEHOLDER_PREFIX)

    def test_format_registers_trouve_in_registry(self):
        trouve = Trouve(type=TrouveType.SOURCE)
        f"{trouve}"
        assert id(trouve) in _registry
        assert _registry[id(trouve)] is trouve

    def test_two_trouves_produce_different_tokens(self):
        trouve_a = Trouve(type=TrouveType.SOURCE)
        trouve_b = Trouve(type=TrouveType.SOURCE)
        token_a = f"{trouve_a}"
        token_b = f"{trouve_b}"
        assert token_a != token_b
        assert len(_registry) == 2

    def test_same_trouve_produces_same_token(self):
        trouve = Trouve(type=TrouveType.SOURCE)
        token_first = f"{trouve}"
        token_second = f"{trouve}"
        assert token_first == token_second

    def test_format_in_fstring_sql(self):
        """Simulates actual usage: referencing a Trouve in SQL via f-string."""
        source = Trouve(type=TrouveType.SOURCE)
        sql = f"SELECT * FROM {source}"
        assert sql.startswith("SELECT * FROM " + TROUVE_PLACEHOLDER_PREFIX)

    def test_clear_empties_registry(self):
        trouve = Trouve(type=TrouveType.SOURCE)
        f"{trouve}"
        assert len(_registry) == 1
        clear()
        assert len(_registry) == 0

    def test_after_clear_new_format_repopulates(self):
        trouve = Trouve(type=TrouveType.SOURCE)
        f"{trouve}"
        clear()
        token_after_clear = f"{trouve}"
        assert token_after_clear.startswith(TROUVE_PLACEHOLDER_PREFIX)
        assert len(_registry) == 1

    def test_format_spec_is_ignored(self):
        """__format__ receives _spec but ignores it; any spec should still work."""
        trouve = Trouve(type=TrouveType.SOURCE)
        token = format(trouve, "some_spec")
        assert token.startswith(TROUVE_PLACEHOLDER_PREFIX)


# ---------------------------------------------------------------------------
# Trouve.build_sql()
# ---------------------------------------------------------------------------


class TestBuildSqlNotCompiled:
    def test_raises_runtime_error_when_not_compiled(self):
        trouve = Trouve(type=TrouveType.TABLE, sql="SELECT 1")
        with pytest.raises(RuntimeError, match="build_sql\\(\\) requires a compiled Trouve"):
            trouve.build_sql(RunMode.FULL_REFRESH, "run_001")


class TestBuildSqlSource:
    def test_source_returns_empty_list(self):
        trouve = _compiled_trouve(
            trouve_type=TrouveType.SOURCE,
            sql="",
            resolved_sql="",
        )
        result = trouve.build_sql(RunMode.FULL_REFRESH, "run_001")
        assert result == []


class TestBuildSqlFullRefresh:
    def test_table_creates_table(self):
        trouve = _compiled_trouve(
            full_name="prod.analytics.orders",
            resolved_sql="SELECT * FROM raw.orders",
        )
        statements = trouve.build_sql(RunMode.FULL_REFRESH, "run_001")

        assert len(statements) == 1
        assert statements[0] == (
            "CREATE OR REPLACE TABLE prod.analytics.orders AS (\n"
            "SELECT * FROM raw.orders\n)"
        )

    def test_view_creates_view(self):
        trouve = _compiled_trouve(
            trouve_type=TrouveType.VIEW,
            full_name="prod.analytics.orders_view",
            resolved_sql="SELECT * FROM raw.orders",
        )
        statements = trouve.build_sql(RunMode.FULL_REFRESH, "run_001")

        assert len(statements) == 1
        assert statements[0] == (
            "CREATE OR REPLACE VIEW prod.analytics.orders_view AS (\n"
            "SELECT * FROM raw.orders\n)"
        )

    def test_resolved_sql_is_stripped(self):
        """Leading/trailing whitespace in resolved_sql should be trimmed."""
        trouve = _compiled_trouve(resolved_sql="  \n  SELECT 1  \n  ")
        statements = trouve.build_sql(RunMode.FULL_REFRESH, "run_001")
        assert "SELECT 1" in statements[0]
        assert "  \n  SELECT 1" not in statements[0]


class TestBuildSqlAppend:
    def test_append_produces_insert_into(self):
        trouve = _compiled_trouve(
            full_name="prod.analytics.events",
            resolved_sql="SELECT * FROM staging.events WHERE ts > '2024-01-01'",
            run_config=RunConfig(
                run_mode=RunMode.INCREMENTAL,
                incremental_mode=IncrementalMode.APPEND,
            ),
        )
        statements = trouve.build_sql(RunMode.INCREMENTAL, "run_002")

        assert len(statements) == 1
        assert statements[0].startswith("INSERT INTO prod.analytics.events")
        assert "SELECT * FROM (" in statements[0]
        assert "SELECT * FROM staging.events WHERE ts > '2024-01-01'" in statements[0]


class TestBuildSqlUpsertWithPrimaryKey:
    """UPSERT with primary_key_columns: 3 statements, auto-generated join condition."""

    def setup_method(self):
        self.columns = [
            Column(name="id", type="INTEGER"),
            Column(name="name", type="STRING"),
            Column(name="amount", type="NUMBER"),
        ]
        self.run_config = RunConfig(
            run_mode=RunMode.INCREMENTAL,
            incremental_mode=IncrementalMode.UPSERT,
            primary_key_columns=["id"],
        )
        self.trouve = _compiled_trouve(
            full_name="db.schema.orders",
            resolved_sql="SELECT id, name, amount FROM staging.orders",
            columns=self.columns,
            run_config=self.run_config,
        )
        self.statements = self.trouve.build_sql(RunMode.INCREMENTAL, "abc123")

    def test_produces_three_statements(self):
        assert len(self.statements) == 3

    def test_stmt1_creates_staging_table_with_run_id(self):
        staging_stmt = self.statements[0]
        assert "CREATE OR REPLACE TABLE db.schema.orders__clair_staging_abc123" in staging_stmt
        assert "SELECT id, name, amount FROM staging.orders" in staging_stmt
        assert "-- [1/3]" in staging_stmt

    def test_stmt2_merge_uses_primary_key_in_on_clause(self):
        merge_stmt = self.statements[1]
        assert "MERGE INTO db.schema.orders AS target" in merge_stmt
        assert "USING db.schema.orders__clair_staging_abc123 AS source" in merge_stmt
        assert "ON target.id = source.id" in merge_stmt

    def test_stmt2_update_set_excludes_primary_key(self):
        merge_stmt = self.statements[1]
        assert "UPDATE SET name = source.name, amount = source.amount" in merge_stmt
        # primary key should NOT appear in UPDATE SET
        assert "id = source.id" not in merge_stmt.split("UPDATE SET")[1]

    def test_stmt2_insert_uses_all_columns(self):
        merge_stmt = self.statements[1]
        assert "INSERT (id, name, amount)" in merge_stmt
        assert "VALUES (source.id, source.name, source.amount)" in merge_stmt

    def test_stmt3_drops_staging_table(self):
        drop_stmt = self.statements[2]
        assert "DROP TABLE IF EXISTS db.schema.orders__clair_staging_abc123" in drop_stmt
        assert "-- [3/3]" in drop_stmt


class TestBuildSqlUpsertWithCompositePrimaryKey:
    """UPSERT with multiple primary_key_columns."""

    def test_composite_key_join_uses_and(self):
        columns = [
            Column(name="region", type="STRING"),
            Column(name="product_id", type="INTEGER"),
            Column(name="quantity", type="NUMBER"),
        ]
        run_config = RunConfig(
            run_mode=RunMode.INCREMENTAL,
            incremental_mode=IncrementalMode.UPSERT,
            primary_key_columns=["region", "product_id"],
        )
        trouve = _compiled_trouve(
            full_name="db.schema.inventory",
            resolved_sql="SELECT region, product_id, quantity FROM raw.inv",
            columns=columns,
            run_config=run_config,
        )
        statements = trouve.build_sql(RunMode.INCREMENTAL, "run_99")
        merge_stmt = statements[1]

        assert "ON target.region = source.region AND target.product_id = source.product_id" in merge_stmt

    def test_composite_key_update_excludes_all_keys(self):
        columns = [
            Column(name="region", type="STRING"),
            Column(name="product_id", type="INTEGER"),
            Column(name="quantity", type="NUMBER"),
        ]
        run_config = RunConfig(
            run_mode=RunMode.INCREMENTAL,
            incremental_mode=IncrementalMode.UPSERT,
            primary_key_columns=["region", "product_id"],
        )
        trouve = _compiled_trouve(
            full_name="db.schema.inventory",
            resolved_sql="SELECT region, product_id, quantity FROM raw.inv",
            columns=columns,
            run_config=run_config,
        )
        statements = trouve.build_sql(RunMode.INCREMENTAL, "run_99")
        merge_stmt = statements[1]

        update_part = merge_stmt.split("UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assert "quantity = source.quantity" in update_part
        assert "region = source.region" not in update_part
        assert "product_id = source.product_id" not in update_part


class TestBuildSqlUpsertWithJoinSql:
    """UPSERT with join_sql: join condition from user, UPDATE includes all columns."""

    def test_join_sql_used_as_on_clause(self):
        columns = [
            Column(name="id", type="INTEGER"),
            Column(name="name", type="STRING"),
        ]
        run_config = RunConfig(
            run_mode=RunMode.INCREMENTAL,
            incremental_mode=IncrementalMode.UPSERT,
            join_sql="target.id = source.id AND target.name IS NOT NULL",
        )
        trouve = _compiled_trouve(
            full_name="db.schema.users",
            resolved_sql="SELECT id, name FROM raw.users",
            columns=columns,
            run_config=run_config,
        )
        statements = trouve.build_sql(RunMode.INCREMENTAL, "run_j1")
        merge_stmt = statements[1]

        assert "ON target.id = source.id AND target.name IS NOT NULL" in merge_stmt

    def test_join_sql_update_includes_all_columns(self):
        """When join_sql is used (no primary_key_columns), UPDATE SET includes all columns."""
        columns = [
            Column(name="id", type="INTEGER"),
            Column(name="name", type="STRING"),
            Column(name="score", type="NUMBER"),
        ]
        run_config = RunConfig(
            run_mode=RunMode.INCREMENTAL,
            incremental_mode=IncrementalMode.UPSERT,
            join_sql="target.id = source.id",
        )
        trouve = _compiled_trouve(
            full_name="db.schema.scores",
            resolved_sql="SELECT id, name, score FROM raw.scores",
            columns=columns,
            run_config=run_config,
        )
        statements = trouve.build_sql(RunMode.INCREMENTAL, "run_j2")
        merge_stmt = statements[1]

        update_part = merge_stmt.split("UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assert "id = source.id" in update_part
        assert "name = source.name" in update_part
        assert "score = source.score" in update_part


class TestBuildSqlUpsertWithUpsertConfig:
    """UPSERT with upsert_config overrides for update_columns and insert_columns."""

    def test_custom_update_columns(self):
        columns = [
            Column(name="id", type="INTEGER"),
            Column(name="name", type="STRING"),
            Column(name="email", type="STRING"),
        ]
        run_config = RunConfig(
            run_mode=RunMode.INCREMENTAL,
            incremental_mode=IncrementalMode.UPSERT,
            primary_key_columns=["id"],
            upsert_config=UpsertConfig(update_columns=["email"]),
        )
        trouve = _compiled_trouve(
            full_name="db.schema.users",
            resolved_sql="SELECT id, name, email FROM raw.users",
            columns=columns,
            run_config=run_config,
        )
        statements = trouve.build_sql(RunMode.INCREMENTAL, "run_uc1")
        merge_stmt = statements[1]

        update_part = merge_stmt.split("UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        # Only email should be in UPDATE SET
        assert "email = source.email" in update_part
        assert "name = source.name" not in update_part

    def test_custom_insert_columns(self):
        columns = [
            Column(name="id", type="INTEGER"),
            Column(name="name", type="STRING"),
            Column(name="email", type="STRING"),
        ]
        run_config = RunConfig(
            run_mode=RunMode.INCREMENTAL,
            incremental_mode=IncrementalMode.UPSERT,
            primary_key_columns=["id"],
            upsert_config=UpsertConfig(insert_columns=["id", "name"]),
        )
        trouve = _compiled_trouve(
            full_name="db.schema.users",
            resolved_sql="SELECT id, name, email FROM raw.users",
            columns=columns,
            run_config=run_config,
        )
        statements = trouve.build_sql(RunMode.INCREMENTAL, "run_uc2")
        merge_stmt = statements[1]

        assert "INSERT (id, name)" in merge_stmt
        assert "VALUES (source.id, source.name)" in merge_stmt
        # email should NOT be in INSERT
        assert "source.email" not in merge_stmt.split("VALUES")[1]

    def test_custom_update_columns_with_join_sql(self):
        """upsert_config.update_columns takes precedence over join_sql default (all cols)."""
        columns = [
            Column(name="id", type="INTEGER"),
            Column(name="name", type="STRING"),
            Column(name="score", type="NUMBER"),
        ]
        run_config = RunConfig(
            run_mode=RunMode.INCREMENTAL,
            incremental_mode=IncrementalMode.UPSERT,
            join_sql="target.id = source.id",
            upsert_config=UpsertConfig(update_columns=["score"]),
        )
        trouve = _compiled_trouve(
            full_name="db.schema.scores",
            resolved_sql="SELECT id, name, score FROM raw.scores",
            columns=columns,
            run_config=run_config,
        )
        statements = trouve.build_sql(RunMode.INCREMENTAL, "run_uc3")
        merge_stmt = statements[1]

        update_part = merge_stmt.split("UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assert "score = source.score" in update_part
        assert "name = source.name" not in update_part
        assert "id = source.id" not in update_part


class TestBuildSqlUpsertErrors:
    def test_upsert_without_columns_raises(self):
        run_config = RunConfig(
            run_mode=RunMode.INCREMENTAL,
            incremental_mode=IncrementalMode.UPSERT,
            primary_key_columns=["id"],
        )
        trouve = _compiled_trouve(
            columns=[],
            run_config=run_config,
        )
        with pytest.raises(ValueError, match="upsert mode requires columns to be defined"):
            trouve.build_sql(RunMode.INCREMENTAL, "run_err")


class TestBuildSqlStagingTableName:
    def test_staging_name_includes_run_id(self):
        columns = [Column(name="id", type="INTEGER"), Column(name="val", type="STRING")]
        run_config = RunConfig(
            run_mode=RunMode.INCREMENTAL,
            incremental_mode=IncrementalMode.UPSERT,
            primary_key_columns=["id"],
        )
        trouve = _compiled_trouve(
            full_name="db.schema.target_table",
            resolved_sql="SELECT id, val FROM raw.data",
            columns=columns,
            run_config=run_config,
        )
        statements = trouve.build_sql(RunMode.INCREMENTAL, "unique_run_42")

        staging_name = "db.schema.target_table__clair_staging_unique_run_42"
        assert staging_name in statements[0]
        assert staging_name in statements[1]
        assert staging_name in statements[2]


# ---------------------------------------------------------------------------
# Test models: TestUnique
# ---------------------------------------------------------------------------


class TestUniqueValidation:
    def test_valid_construction(self):
        test = TestUnique(column="id")
        assert test.column == "id"
        assert test.type == "unique"
        assert test.label == "unique"

    def test_sql_generation(self):
        test = TestUnique(column="user_id")
        sql = test.to_sql("db.schema.orders")
        assert sql == (
            "SELECT user_id, COUNT(*) "
            "FROM db.schema.orders "
            "GROUP BY user_id "
            "HAVING COUNT(*) > 1"
        )


# ---------------------------------------------------------------------------
# Test models: TestNotNull
# ---------------------------------------------------------------------------


class TestNotNullValidation:
    def test_valid_construction(self):
        test = TestNotNull(column="email")
        assert test.column == "email"
        assert test.type == "not_null"
        assert test.label == "not_null"

    def test_sql_generation(self):
        test = TestNotNull(column="email")
        sql = test.to_sql("db.schema.users")
        assert sql == (
            "SELECT email "
            "FROM db.schema.users "
            "WHERE email IS NULL"
        )


# ---------------------------------------------------------------------------
# Test models: TestRowCount
# ---------------------------------------------------------------------------


class TestRowCountValidation:
    def test_valid_min_only(self):
        test = TestRowCount(min_rows=1)
        assert test.min_rows == 1
        assert test.max_rows is None

    def test_valid_max_only(self):
        test = TestRowCount(max_rows=1000)
        assert test.min_rows is None
        assert test.max_rows == 1000

    def test_valid_min_and_max(self):
        test = TestRowCount(min_rows=5, max_rows=100)
        assert test.min_rows == 5
        assert test.max_rows == 100

    def test_min_rows_zero_is_valid(self):
        test = TestRowCount(min_rows=0)
        assert test.min_rows == 0

    def test_min_and_max_both_zero_is_valid(self):
        test = TestRowCount(min_rows=0, max_rows=0)
        assert test.min_rows == 0
        assert test.max_rows == 0

    def test_equal_min_and_max_is_valid(self):
        test = TestRowCount(min_rows=50, max_rows=50)
        assert test.min_rows == 50
        assert test.max_rows == 50

    def test_neither_raises(self):
        with pytest.raises(ValueError, match="at least one of min_rows or max_rows must be set"):
            TestRowCount()

    def test_max_less_than_min_raises(self):
        with pytest.raises(ValueError, match="max_rows must be >= min_rows"):
            TestRowCount(min_rows=10, max_rows=5)

    def test_negative_min_raises(self):
        with pytest.raises(ValueError, match="min_rows must be >= 0"):
            TestRowCount(min_rows=-1)

    def test_negative_max_with_no_min_is_allowed(self):
        """max_rows has no >= 0 validation when min_rows is not set."""
        test = TestRowCount(max_rows=-5)
        assert test.max_rows == -5


class TestRowCountSqlGeneration:
    def test_sql_min_only(self):
        test = TestRowCount(min_rows=5)
        sql = test.to_sql("db.s.orders")
        assert sql == "SELECT 1 FROM db.s.orders HAVING COUNT(*) < 5"

    def test_sql_max_only(self):
        test = TestRowCount(max_rows=500)
        sql = test.to_sql("db.s.orders")
        assert sql == "SELECT 1 FROM db.s.orders HAVING COUNT(*) > 500"

    def test_sql_min_and_max(self):
        test = TestRowCount(min_rows=1, max_rows=100)
        sql = test.to_sql("db.s.orders")
        assert " UNION ALL " in sql
        assert "HAVING COUNT(*) < 1" in sql
        assert "HAVING COUNT(*) > 100" in sql

    def test_sql_min_rows_zero(self):
        """min_rows=0 generates HAVING COUNT(*) < 0 which is always false (good: no failure)."""
        test = TestRowCount(min_rows=0)
        sql = test.to_sql("db.s.t")
        assert "HAVING COUNT(*) < 0" in sql

    def test_sql_max_only_no_union(self):
        test = TestRowCount(max_rows=10)
        sql = test.to_sql("db.s.t")
        assert "UNION ALL" not in sql

    def test_is_run_with_sample_false(self):
        test = TestRowCount(min_rows=1)
        assert test.is_run_with_sample is False


class TestIsRunWithSample:
    def test_unique_is_run_with_sample_true(self):
        test = TestUnique(column="id")
        assert test.is_run_with_sample is True

    def test_not_null_is_run_with_sample_true(self):
        test = TestNotNull(column="email")
        assert test.is_run_with_sample is True

    def test_unique_columns_is_run_with_sample_true(self):
        test = TestUniqueColumns(columns=["a", "b"])
        assert test.is_run_with_sample is True

    def test_row_count_is_run_with_sample_false(self):
        test = TestRowCount(min_rows=1)
        assert test.is_run_with_sample is False


# ---------------------------------------------------------------------------
# Test models: TestUniqueColumns
# ---------------------------------------------------------------------------


class TestUniqueColumnsValidation:
    def test_valid_two_columns(self):
        test = TestUniqueColumns(columns=["a", "b"])
        assert test.columns == ["a", "b"]
        assert test.type == "unique_columns"
        assert test.label == "unique_columns"

    def test_valid_three_columns(self):
        test = TestUniqueColumns(columns=["a", "b", "c"])
        assert test.columns == ["a", "b", "c"]

    def test_valid_many_columns(self):
        test = TestUniqueColumns(columns=["a", "b", "c", "d", "e"])
        assert len(test.columns) == 5

    def test_one_column_raises(self):
        with pytest.raises(ValueError, match="at least 2"):
            TestUniqueColumns(columns=["a"])

    def test_empty_columns_raises(self):
        with pytest.raises(ValueError, match="at least 2"):
            TestUniqueColumns(columns=[])


class TestUniqueColumnsSqlGeneration:
    def test_sql_two_columns(self):
        test = TestUniqueColumns(columns=["col_a", "col_b"])
        sql = test.to_sql("db.s.orders")
        assert sql == (
            "SELECT col_a, col_b, COUNT(*) "
            "FROM db.s.orders "
            "GROUP BY col_a, col_b "
            "HAVING COUNT(*) > 1"
        )

    def test_sql_three_columns(self):
        test = TestUniqueColumns(columns=["x", "y", "z"])
        sql = test.to_sql("db.s.t")
        assert "SELECT x, y, z, COUNT(*)" in sql
        assert "GROUP BY x, y, z" in sql

    def test_sql_five_columns(self):
        test = TestUniqueColumns(columns=["a", "b", "c", "d", "e"])
        sql = test.to_sql("db.s.wide")
        assert "SELECT a, b, c, d, e, COUNT(*)" in sql
        assert "GROUP BY a, b, c, d, e" in sql


# ---------------------------------------------------------------------------
# Column
# ---------------------------------------------------------------------------


class TestColumn:
    def test_basic_column(self):
        column = Column(name="id", type=ColumnType.STRING)
        assert column.name == "id"
        assert column.type == "STRING"
        assert column.nullable is True
        assert column.docs == ""

    def test_column_with_all_fields(self):
        column = Column(name="amount", type="NUMBER(18,2)", docs="Order amount", nullable=False)
        assert column.type == "NUMBER(18,2)"
        assert column.docs == "Order amount"
        assert column.nullable is False

    def test_column_type_constants(self):
        assert ColumnType.STRING == "STRING"
        assert ColumnType.FLOAT == "FLOAT"
        assert ColumnType.TIMESTAMP_NTZ == "TIMESTAMP_NTZ"


# ---------------------------------------------------------------------------
# Config models
# ---------------------------------------------------------------------------


class TestConfig:
    def test_database_defaults(self):
        defaults = DatabaseDefaults(warehouse="wh", role="admin")
        assert defaults.warehouse == "wh"
        assert defaults.role == "admin"

    def test_database_defaults_optional(self):
        defaults = DatabaseDefaults()
        assert defaults.warehouse is None
        assert defaults.role is None

    def test_schema_defaults(self):
        schema = SchemaDefaults(warehouse="reporting_wh")
        assert schema.warehouse == "reporting_wh"
        assert schema.role is None


# ---------------------------------------------------------------------------
# Trouve with df_fn
# ---------------------------------------------------------------------------


class TestTrouveWithDfFn:
    """Tests for the df_fn field on Trouve."""

    def _make_upstream(self) -> Trouve:
        """Create a compiled SOURCE Trouve for use as an upstream dependency."""
        source = Trouve(type=TrouveType.SOURCE)
        source.compiled = _compiled_attrs(full_name="db.schema.upstream", resolved_sql="")
        return source

    def test_valid_construction_with_df_fn(self):
        upstream = self._make_upstream()

        def my_fn(events=upstream):
            return events

        trouve = Trouve(df_fn=my_fn)
        assert trouve.df_fn is my_fn
        assert trouve.type == TrouveType.TABLE
        assert trouve.sql == ""

    def test_df_fn_with_sql_raises_value_error(self):
        upstream = self._make_upstream()

        def my_fn(events=upstream):
            return events

        with pytest.raises(ValueError, match="cannot have both sql and df_fn"):
            Trouve(df_fn=my_fn, sql="SELECT 1")

    def test_df_fn_with_view_type_raises_value_error(self):
        upstream = self._make_upstream()

        def my_fn(events=upstream):
            return events

        with pytest.raises(ValueError, match="df_fn Trouves must be TABLE type"):
            Trouve(df_fn=my_fn, type=TrouveType.VIEW)

    def test_df_fn_with_source_type_raises_value_error(self):
        upstream = self._make_upstream()

        def my_fn(events=upstream):
            return events

        with pytest.raises(ValueError, match="df_fn Trouves must be TABLE type"):
            Trouve(df_fn=my_fn, type=TrouveType.SOURCE)

    def test_df_fn_not_callable_raises_value_error(self):
        with pytest.raises(ValueError, match="df_fn must be callable"):
            Trouve(df_fn="not_a_function")

    def test_df_fn_with_incremental_raises_value_error(self):
        upstream = self._make_upstream()

        def my_fn(events=upstream):
            return events

        with pytest.raises(ValueError, match="df_fn Trouves do not support incremental mode"):
            Trouve(
                df_fn=my_fn,
                run_config=RunConfig(
                    run_mode=RunMode.INCREMENTAL,
                    incremental_mode=IncrementalMode.APPEND,
                ),
            )

