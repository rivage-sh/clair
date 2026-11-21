"""The tests of staging -- write to a staging address, test, then promote."""

from __future__ import annotations

from typing import TypeVar

import pandas as pd
import pytest

from clair.core.compiler import build_statements
from clair.core.staging import STAGING_SUFFIX, StagingAddressError, make_staging_address
from clair.environments.routing import TrouveAddress
from clair.trouves.pandas_table import PandasTable
from clair.trouves.plan import PlanContext
from clair.trouves.run_config import Append, Incremental, RunMode, Upsert
from clair.trouves.statements import view_promote_statement
from clair.trouves.trouve import Table, TrouveAbc, View
from tests.helpers import (
    TEST_ADDRESS,
    RecordingAdapter,
    build_and_cleanup,
    compile_trouve,
    executed_sql,
    run_one_trouve,
    staged_plan,
    unstaged_plan,
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


def _compile(trouve: AnyTrouve, address: str, imports: list[str] | None = None) -> AnyTrouve:
    return compile_trouve(trouve, address, imports=imports)


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
    def test_table_is_cloned_into_place(self):
        statement = "\n".join(
            _compile(Table(sql="SELECT 1 AS id"), "db.s.t").clone_promote_statements(
                _address("db.s.t__staging"), _address("db.s.t")
            )
        )
        assert "CREATE OR REPLACE TABLE db.s.t CLONE db.s.t__staging" in statement

    def test_table_promotion_does_not_depend_on_the_target_existing(self):
        statement = "\n".join(
            _compile(Table(sql="SELECT 1 AS id"), "db.s.t").clone_promote_statements(
                _address("db.s.t__staging"), _address("db.s.t")
            )
        )
        assert "IF NOT EXISTS" not in statement
        assert "SWAP WITH" not in statement
        assert "RENAME TO" not in statement

    def test_view_is_recreated(self):
        statement = view_promote_statement(_address("db.s.v"), "SELECT 1 AS id")
        assert "CREATE OR REPLACE VIEW db.s.v AS" in statement
        assert "SELECT 1 AS id" in statement

    def test_drop_staging_uses_matching_object_type(self):
        table = _compile(Table(sql="SELECT 1 AS id"), "db.s.t")
        view = _compile(View(sql="SELECT 1 AS id"), "db.s.v")
        assert "DROP TABLE IF EXISTS db.s.t" in table.drop_statement(_address("db.s.t"))
        assert "DROP VIEW IF EXISTS db.s.v" in view.drop_statement(_address("db.s.v"))

    def test_clone_is_zero_copy(self):
        statement = _compile(Table(sql="SELECT 1 AS id"), "db.s.t").clone_statement(
            _address("db.s.t"), _address("db.s.t__staging")
        )
        assert "CREATE OR REPLACE TABLE db.s.t__staging CLONE db.s.t" in statement


class TestTheBuildWritesTheTargetAddress:
    def test_full_refresh_writes_into_override(self):
        trouve = _compile(Table(sql="SELECT 1 AS id"), "db.s.orders")
        statements = build_and_cleanup(staged_plan(trouve, RunMode.FULL_REFRESH, RUN_ID, _address("db.s.staging")))
        assert "CREATE OR REPLACE TABLE db.s.staging" in statements[0]
        assert "db.s.orders" not in statements[0]

    def test_append_inserts_into_override(self):
        trouve = _compile(
            Table(
                sql="SELECT 1 AS id",
                run_config=Incremental(strategy=Append()),
            ),
            "db.s.orders",
        )
        statements = build_and_cleanup(staged_plan(trouve, RunMode.INCREMENTAL, RUN_ID, _address("db.s.staging")))
        assert statements[0].startswith("INSERT INTO db.s.staging")

    def test_upsert_merges_into_override_without_stacking_staging_suffixes(self):
        from clair.trouves.column import Column, ColumnType

        trouve = _compile(
            Table(
                sql="SELECT 1 AS id, 2 AS amount",
                columns=[
                    Column(name="id", type=ColumnType.NUMBER),
                    Column(name="amount", type=ColumnType.NUMBER),
                ],
                run_config=Incremental(strategy=Upsert(primary_key_columns=["id"])),
            ),
            "db.s.orders",
        )
        statements = build_and_cleanup(staged_plan(trouve, RunMode.INCREMENTAL, RUN_ID, _address("db.s.candidate")))
        assert "MERGE INTO db.s.candidate" in statements[1]
        # The merge source table derives from the real name, not the override,
        # so the two suffixes never stack.
        assert f"db.s.orders__clair_new_rows_{RUN_ID}" in statements[0]


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
        trouve = _compile(Table(sql="SELECT 1 AS id"), "db.s.orders")
        statements = build_statements(trouve, RunMode.FULL_REFRESH, RUN_ID, use_staging=True)

        staging = _staging_name("db.s.orders", RUN_ID)
        assert statements[0].startswith("-- ── build")
        assert f"CREATE OR REPLACE TABLE {staging}" in statements[0]
        # The promotion clones the tested table into position.
        assert statements[1].startswith("-- ── promote · when should_promote() gives True")
        assert f"CREATE OR REPLACE TABLE db.s.orders CLONE {staging}" in statements[1]
        assert statements[2].startswith("-- ── retire · after the promotion")
        assert f"DROP TABLE IF EXISTS {staging}" in statements[2]

    def test_incremental_plan_starts_with_a_clone(self):
        trouve = _compile(
            Table(
                sql="SELECT 1 AS id",
                run_config=Incremental(strategy=Append()),
            ),
            "db.s.orders",
        )
        statements = build_statements(trouve, RunMode.INCREMENTAL, RUN_ID, use_staging=True)
        assert "CLONE db.s.orders" in statements[0]

    def test_plan_without_staging_is_the_plain_build(self):
        # The plan holds the build, after the header of its phase.
        trouve = _compile(Table(sql="SELECT 1 AS id"), "db.s.orders")
        statements = build_statements(
            trouve, RunMode.FULL_REFRESH, RUN_ID, use_staging=False
        )
        (build,) = build_and_cleanup(unstaged_plan(trouve, RunMode.FULL_REFRESH, RUN_ID))
        assert len(statements) == 1
        assert statements[0].endswith(build)



AUDITOR_GRANT = f"GRANT SELECT ON TABLE {TEST_ADDRESS} TO ROLE auditor"


class AuditedTable(Table):
    """The override of staging.md#grants: give a grant on one object again after each write."""

    def document(self, context: PlanContext) -> list[str]:
        return [
            *super().document(context),
            f"GRANT SELECT ON TABLE {context.physical_address} TO ROLE auditor",
        ]


class AuditedPandasTable(PandasTable):
    """The same override on a DataFrame Trouve, whose write_pandas drops the table."""

    def document(self, context: PlanContext) -> list[str]:
        return [
            *super().document(context),
            f"GRANT SELECT ON TABLE {context.physical_address} TO ROLE auditor",
        ]


def _audit_rows() -> pd.DataFrame:
    return pd.DataFrame({"id": [1]})


@pytest.mark.parametrize(
    "trouve",
    [AuditedTable(sql="SELECT 1 AS id"), AuditedPandasTable(transform=_audit_rows)],
    ids=["table", "pandas_table"],
)
@pytest.mark.parametrize("use_staging", [True, False], ids=["staged", "no_test"])
def test_a_document_override_grants_again_after_the_last_write_of_production(trouve, use_staging):
    # Clair keeps a grant on one object only where can. The
    # document phase runs on production after the promotion, or after the
    # write of a run with no staging, thus an override there gives the grant
    # back on each path, also after a write_pandas that dropped the table.
    result = run_one_trouve(trouve, RecordingAdapter(), use_staging=use_staging)
    statements = executed_sql(result)

    production_writes = [
        index
        for index, sql in enumerate(statements)
        if sql != AUDITOR_GRANT
        and (f"TABLE {TEST_ADDRESS} " in sql or sql == f"-- write_dataframe {TEST_ADDRESS}")
    ]
    assert production_writes, "the run wrote nothing to production"
    assert statements.index(AUDITOR_GRANT) > max(production_writes)
