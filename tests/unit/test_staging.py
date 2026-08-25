"""The tests of staging -- write to a staging address, test, then promote."""

from __future__ import annotations

from pathlib import Path
from typing import TypeVar

import pytest

from clair.core.compiler import build_statements
from clair.core.staging import (
    STAGING_SUFFIX,
    StagingAddressError,
    build_clone_statement,
    build_drop_staging_statement,
    build_promote_statement,
    make_staging_address,
)
from clair.environments.routing import TrouveAddress
from clair.trouves.config import ResolvedConfig
from clair.trouves.run_config import IncrementalMode, RunConfig, RunMode
from clair.trouves.trouve import (
    CompiledAttributes,
    ExecutionType,
    Trouve,
    TrouveAbc,
    TrouveType,
)

RUN_ID = "0195aabbccddeeff0011223344556677"

# Snowflake permits 255 characters for each object name.
MAX_IDENTIFIER_LENGTH = 255


def _address(physical_address: str) -> TrouveAddress:
    return TrouveAddress.parse(physical_address)


def _staging_name(physical_address: str, run_id: str = RUN_ID) -> str:
    """Give the staging address of a dotted name, as a string."""
    return str(make_staging_address(_address(physical_address), run_id))


AnyTrouve = TypeVar("AnyTrouve", bound=TrouveAbc)


def _compile(
    trouve: AnyTrouve,
    address: str,
    imports: list[str] | None = None,
    execution_type: ExecutionType = ExecutionType.SNOWFLAKE,
) -> AnyTrouve:
    trouve.compiled = CompiledAttributes(
        physical_address=TrouveAddress.parse(address),
        logical_address=TrouveAddress.parse(address),
        resolved_sql=getattr(trouve, "sql", ""),
        file_path=Path(f"/fake/{address.replace('.', '/')}.py"),
        module_name=address,
        imports=imports or [],
        config=ResolvedConfig(),
        execution_type=execution_type,
    )
    return trouve


class TestStagingAddress:
    def test_suffix_applied_to_table_component_only(self):
        staging = _staging_name("db.schema.orders", RUN_ID)
        assert staging == f"db.schema.orders{STAGING_SUFFIX}{RUN_ID}"

    def test_staging_shares_database_and_schema_with_target(self):
        """A rejected candidate should sit next to the table it was meant to become."""
        staging = _staging_name("analytics.revenue.daily", RUN_ID)
        assert staging.split(".")[:2] == ["analytics", "revenue"]

    def test_run_id_makes_concurrent_runs_disjoint(self):
        first = _staging_name("db.schema.orders", "aaaa")
        second = _staging_name("db.schema.orders", "bbbb")
        assert first != second

    def test_rejects_identifier_over_snowflake_limit(self):
        long_table = "x" * MAX_IDENTIFIER_LENGTH
        with pytest.raises(StagingAddressError, match="shorter name"):
            _staging_name(f"db.schema.{long_table}", RUN_ID)

    def test_accepts_identifier_at_the_limit(self):
        budget = MAX_IDENTIFIER_LENGTH - len(STAGING_SUFFIX) - len(RUN_ID)
        staging = _staging_name(f"db.schema.{'x' * budget}", RUN_ID)
        assert len(staging.split(".")[2]) == MAX_IDENTIFIER_LENGTH


class TestPromoteStatements:
    def test_table_is_cloned_into_place_carrying_grants(self):
        statement = build_promote_statement(
            TrouveType.TABLE,
            staging_address=_address("db.s.t__staging"),
            physical_address=_address("db.s.t"),
        )
        assert "CREATE OR REPLACE TABLE db.s.t CLONE db.s.t__staging COPY GRANTS" in statement

    def test_table_promotion_does_not_depend_on_the_target_existing(self):
        """COPY GRANTS copies from the replaced table, or the clone source if there is none."""
        statement = build_promote_statement(
            TrouveType.TABLE,
            staging_address=_address("db.s.t__staging"),
            physical_address=_address("db.s.t"),
        )
        assert "IF NOT EXISTS" not in statement
        assert "SWAP WITH" not in statement
        assert "RENAME TO" not in statement

    def test_view_is_recreated_carrying_grants(self):
        statement = build_promote_statement(
            TrouveType.VIEW,
            staging_address=_address("db.s.v__staging"),
            physical_address=_address("db.s.v"),
            resolved_sql="SELECT 1 AS id",
        )
        assert "CREATE OR REPLACE VIEW db.s.v COPY GRANTS AS" in statement
        assert "SELECT 1 AS id" in statement

    def test_drop_staging_uses_matching_object_type(self):
        assert "DROP TABLE IF EXISTS db.s.t" in build_drop_staging_statement(
            TrouveType.TABLE, _address("db.s.t")
        )
        assert "DROP VIEW IF EXISTS db.s.v" in build_drop_staging_statement(
            TrouveType.VIEW, _address("db.s.v")
        )

    def test_clone_is_zero_copy(self):
        statement = build_clone_statement(_address("db.s.t"), _address("db.s.t__staging"))
        assert "CREATE OR REPLACE TABLE db.s.t__staging CLONE db.s.t" in statement


class TestBuildSqlStagingAddress:
    def test_full_refresh_writes_into_override(self):
        trouve = _compile(Trouve(sql="SELECT 1 AS id"), "db.s.orders")
        statements = trouve.build_sql(RunMode.FULL_REFRESH, RUN_ID, staging_address=_address("db.s.staging"))
        assert "CREATE OR REPLACE TABLE db.s.staging" in statements[0]
        assert "db.s.orders" not in statements[0]

    def test_append_inserts_into_override(self):
        trouve = _compile(
            Trouve(
                sql="SELECT 1 AS id",
                run_config=RunConfig(
                    run_mode=RunMode.INCREMENTAL, incremental_mode=IncrementalMode.APPEND
                ),
            ),
            "db.s.orders",
        )
        statements = trouve.build_sql(RunMode.INCREMENTAL, RUN_ID, staging_address=_address("db.s.staging"))
        assert statements[0].startswith("INSERT INTO db.s.staging")

    def test_upsert_merges_into_override_without_stacking_staging_suffixes(self):
        from clair.trouves.column import Column, ColumnType

        trouve = _compile(
            Trouve(
                sql="SELECT 1 AS id, 2 AS amount",
                columns=[
                    Column(name="id", type=ColumnType.NUMBER),
                    Column(name="amount", type=ColumnType.NUMBER),
                ],
                run_config=RunConfig(
                    run_mode=RunMode.INCREMENTAL,
                    incremental_mode=IncrementalMode.UPSERT,
                    primary_key_columns=["id"],
                ),
            ),
            "db.s.orders",
        )
        statements = trouve.build_sql(RunMode.INCREMENTAL, RUN_ID, staging_address=_address("db.s.candidate"))
        assert "MERGE INTO db.s.candidate" in statements[1]
        # The merge source table derives from the real name, not the override,
        # so the two suffixes never stack.
        assert f"db.s.orders__clair_merge_{RUN_ID}" in statements[0]

    def test_omitting_the_address_writes_to_the_physical_address(self):
        trouve = _compile(Trouve(sql="SELECT 1 AS id"), "db.s.orders")
        assert trouve.build_sql(RunMode.FULL_REFRESH, RUN_ID) == trouve.build_sql(
            RunMode.FULL_REFRESH, RUN_ID, staging_address=None
        )


class TestStagingIsAlwaysOn:
    def test_the_strict_flag_is_gone(self):
        """Staging is unconditional, so no flag selects it."""
        import structlog
        from click.testing import CliRunner

        from clair.cli.main import cli

        try:
            for command in ("run", "compile"):
                result = CliRunner().invoke(cli, [command, "--strict"])
                # Click exits 2 on an unrecognized option.
                assert result.exit_code == 2, command
                assert "--strict" in result.output
        finally:
            # The CLI binds structlog to the runner's stdout/stderr, which are
            # closed on exit; reset so later tests log to real streams.
            structlog.reset_defaults()


class TestStagingCompilePlan:
    def test_plan_shows_staging_build_test_checkpoint_and_promotion(self):
        trouve = _compile(Trouve(sql="SELECT 1 AS id"), "db.s.orders")
        statements = build_statements(trouve, RunMode.FULL_REFRESH, RUN_ID, use_staging=True)

        staging = _staging_name("db.s.orders", RUN_ID)
        assert f"CREATE OR REPLACE TABLE {staging}" in statements[0]
        assert "the data quality tests run here" in statements[1]
        assert f"CREATE OR REPLACE TABLE db.s.orders CLONE {staging} COPY GRANTS" in statements[2]
        assert f"DROP TABLE IF EXISTS {staging}" in statements[3]

    def test_incremental_plan_starts_with_a_clone(self):
        trouve = _compile(
            Trouve(
                sql="SELECT 1 AS id",
                run_config=RunConfig(
                    run_mode=RunMode.INCREMENTAL, incremental_mode=IncrementalMode.APPEND
                ),
            ),
            "db.s.orders",
        )
        statements = build_statements(trouve, RunMode.INCREMENTAL, RUN_ID, use_staging=True)
        assert "CLONE db.s.orders" in statements[0]

    def test_plan_without_staging_is_the_plain_build(self):
        trouve = _compile(Trouve(sql="SELECT 1 AS id"), "db.s.orders")
        statements = build_statements(
            trouve, RunMode.FULL_REFRESH, RUN_ID, use_staging=False
        )
        assert statements == trouve.build_sql(RunMode.FULL_REFRESH, RUN_ID)
