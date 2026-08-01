"""Tests for strict mode -- build into staging, test, then swap."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from clair.adapters.base import QueryResult, WarehouseAdapter
from clair.core.compiler import build_statements
from clair.core.dag import build_dag, get_executable_nodes
from clair.core.runner import RunStatus, run_project
from clair.core.strict import (
    MAX_IDENTIFIER_LENGTH,
    STRICT_SUFFIX,
    StrictNamingError,
    build_cleanup_statement,
    build_clone_statement,
    build_promote_statements,
    strict_staging_name,
)
from clair.exceptions import RunError
from clair.trouves.config import ResolvedConfig
from clair.trouves.run_config import IncrementalMode, RunConfig, RunMode
from clair.trouves.trouve import CompiledAttributes, ExecutionType, Trouve, TrouveType


RUN_ID = "0195aabbccddeeff0011223344556677"


def _compile(
    trouve: Trouve,
    full_name: str,
    imports: list[str] | None = None,
    execution_type: ExecutionType = ExecutionType.SNOWFLAKE,
) -> Trouve:
    trouve.compiled = CompiledAttributes(
        full_name=full_name,
        logical_name=full_name,
        resolved_sql=trouve.sql,
        file_path=Path(f"/fake/{full_name.replace('.', '/')}.py"),
        module_name=full_name,
        imports=imports or [],
        config=ResolvedConfig(),
        execution_type=execution_type,
    )
    return trouve


def _make_adapter(
    fail_on: str | None = None,
    target_exists: bool = True,
) -> tuple[WarehouseAdapter, list[str]]:
    """Return a mock adapter plus the list of SQL it was asked to execute."""
    executed: list[str] = []
    adapter = MagicMock(spec=WarehouseAdapter)
    counter = 0

    def mock_execute(sql: str) -> QueryResult:
        nonlocal counter
        counter += 1
        executed.append(sql)
        query_id = f"qid-{counter:04d}"
        success = fail_on is None or fail_on not in sql
        return QueryResult(
            query_id=query_id,
            query_url=f"https://test/#/query/{query_id}",
            success=success,
            error=None if success else f"Simulated failure for {fail_on}",
        )

    adapter.execute.side_effect = mock_execute
    adapter.table_exists.return_value = target_exists
    return adapter, executed


class TestStrictStagingName:
    def test_suffix_applied_to_table_component_only(self):
        staging = strict_staging_name("db.schema.orders", RUN_ID)
        assert staging == f"db.schema.orders{STRICT_SUFFIX}{RUN_ID}"

    def test_staging_shares_database_and_schema_with_target(self):
        """SWAP WITH requires both objects to live in the same schema."""
        staging = strict_staging_name("analytics.revenue.daily", RUN_ID)
        assert staging.split(".")[:2] == ["analytics", "revenue"]

    def test_run_id_makes_concurrent_runs_disjoint(self):
        first = strict_staging_name("db.schema.orders", "aaaa")
        second = strict_staging_name("db.schema.orders", "bbbb")
        assert first != second

    def test_rejects_name_that_is_not_three_parts(self):
        with pytest.raises(StrictNamingError, match="database.schema.table"):
            strict_staging_name("db.schema", RUN_ID)

    def test_rejects_identifier_over_snowflake_limit(self):
        long_table = "x" * MAX_IDENTIFIER_LENGTH
        with pytest.raises(StrictNamingError, match="max 255"):
            strict_staging_name(f"db.schema.{long_table}", RUN_ID)

    def test_accepts_identifier_at_the_limit(self):
        budget = MAX_IDENTIFIER_LENGTH - len(STRICT_SUFFIX) - len(RUN_ID)
        staging = strict_staging_name(f"db.schema.{'x' * budget}", RUN_ID)
        assert len(staging.split(".")[2]) == MAX_IDENTIFIER_LENGTH


class TestPromoteStatements:
    def test_existing_table_is_swapped_then_old_copy_dropped(self):
        statements = build_promote_statements(
            TrouveType.TABLE,
            staging_name="db.s.t__staging",
            target_name="db.s.t",
            target_exists=True,
        )
        assert "ALTER TABLE db.s.t__staging SWAP WITH db.s.t" in statements[0]
        assert "DROP TABLE IF EXISTS db.s.t__staging" in statements[1]

    def test_missing_table_is_renamed_into_place(self):
        statements = build_promote_statements(
            TrouveType.TABLE,
            staging_name="db.s.t__staging",
            target_name="db.s.t",
            target_exists=False,
        )
        assert len(statements) == 1
        assert "ALTER TABLE db.s.t__staging RENAME TO db.s.t" in statements[0]

    def test_view_is_recreated_and_staging_dropped(self):
        statements = build_promote_statements(
            TrouveType.VIEW,
            staging_name="db.s.v__staging",
            target_name="db.s.v",
            target_exists=True,
            resolved_sql="SELECT 1 AS id",
        )
        assert "CREATE OR REPLACE VIEW db.s.v" in statements[0]
        assert "SELECT 1 AS id" in statements[0]
        assert "DROP VIEW IF EXISTS db.s.v__staging" in statements[1]

    def test_cleanup_uses_matching_object_type(self):
        assert "DROP TABLE IF EXISTS db.s.t" in build_cleanup_statement(
            TrouveType.TABLE, "db.s.t"
        )
        assert "DROP VIEW IF EXISTS db.s.v" in build_cleanup_statement(
            TrouveType.VIEW, "db.s.v"
        )

    def test_clone_is_zero_copy(self):
        statement = build_clone_statement("db.s.t", "db.s.t__staging")
        assert "CREATE OR REPLACE TABLE db.s.t__staging CLONE db.s.t" in statement


class TestBuildSqlTargetOverride:
    def test_full_refresh_writes_into_override(self):
        trouve = _compile(Trouve(sql="SELECT 1 AS id"), "db.s.orders")
        statements = trouve.build_sql(RunMode.FULL_REFRESH, RUN_ID, target_name="db.s.staging")
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
        statements = trouve.build_sql(RunMode.INCREMENTAL, RUN_ID, target_name="db.s.staging")
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
        statements = trouve.build_sql(RunMode.INCREMENTAL, RUN_ID, target_name="db.s.strict")
        assert "MERGE INTO db.s.strict" in statements[1]
        # The merge staging table derives from the real name, not the override,
        # so the two suffixes never stack.
        assert f"db.s.orders__clair_staging_{RUN_ID}" in statements[0]

    def test_omitting_override_preserves_previous_behaviour(self):
        trouve = _compile(Trouve(sql="SELECT 1 AS id"), "db.s.orders")
        assert trouve.build_sql(RunMode.FULL_REFRESH, RUN_ID) == trouve.build_sql(
            RunMode.FULL_REFRESH, RUN_ID, target_name=None
        )


def _single_table_dag(
    trouve_type: TrouveType = TrouveType.TABLE,
    run_config: RunConfig | None = None,
):
    kwargs = {"sql": "SELECT 1 AS id", "type": trouve_type}
    if run_config is not None:
        kwargs["run_config"] = run_config
    trouve = _compile(Trouve(**kwargs), "db.s.orders")
    dag = build_dag([trouve])
    return dag, get_executable_nodes(dag)


class TestStrictRunner:
    def test_build_targets_staging_and_promotes_after_passing_tests(self):
        dag, selected = _single_table_dag()
        adapter, executed = _make_adapter()
        tested: list[str] = []

        def on_success(node_name: str, physical_name: str) -> bool:
            tested.append(physical_name)
            return True

        results = list(run_project(
            dag, selected, adapter,
            run_id=RUN_ID, after_node_success=on_success, strict=True,
        ))

        staging = strict_staging_name("db.s.orders", RUN_ID)
        assert results[0].status == RunStatus.SUCCESS
        # Tests ran against the staging object, not the target.
        assert tested == [staging]
        assert any(f"CREATE OR REPLACE TABLE {staging}" in sql for sql in executed)
        assert any(f"ALTER TABLE {staging} SWAP WITH db.s.orders" in sql for sql in executed)

    def test_target_is_never_written_before_tests_pass(self):
        dag, selected = _single_table_dag()
        adapter, executed = _make_adapter()
        sql_at_test_time: list[list[str]] = []

        def on_success(node_name: str, physical_name: str) -> bool:
            sql_at_test_time.append(list(executed))
            return True

        list(run_project(
            dag, selected, adapter,
            run_id=RUN_ID, after_node_success=on_success, strict=True,
        ))

        assert not any(
            "CREATE OR REPLACE TABLE db.s.orders AS" in sql for sql in sql_at_test_time[0]
        )

    def test_failing_tests_drop_staging_and_leave_target_untouched(self):
        dag, selected = _single_table_dag()
        adapter, executed = _make_adapter()

        results = list(run_project(
            dag, selected, adapter,
            run_id=RUN_ID, after_node_success=lambda _n, _p: False, strict=True,
        ))

        staging = strict_staging_name("db.s.orders", RUN_ID)
        assert results[0].status == RunStatus.FAILURE
        assert "tests failed" in results[0].error
        assert any(f"DROP TABLE IF EXISTS {staging}" in sql for sql in executed)
        assert not any("SWAP WITH" in sql for sql in executed)

    def test_missing_target_is_renamed_rather_than_swapped(self):
        dag, selected = _single_table_dag()
        adapter, executed = _make_adapter(target_exists=False)

        list(run_project(
            dag, selected, adapter,
            run_id=RUN_ID, after_node_success=lambda _n, _p: True, strict=True,
        ))

        assert any("RENAME TO db.s.orders" in sql for sql in executed)
        assert not any("SWAP WITH" in sql for sql in executed)

    def test_failed_materialization_cleans_up_staging(self):
        dag, selected = _single_table_dag()
        adapter, executed = _make_adapter(fail_on="CREATE OR REPLACE TABLE")

        results = list(run_project(
            dag, selected, adapter,
            run_id=RUN_ID, after_node_success=lambda _n, _p: True, strict=True,
        ))

        staging = strict_staging_name("db.s.orders", RUN_ID)
        assert results[0].status == RunStatus.FAILURE
        assert any(f"DROP TABLE IF EXISTS {staging}" in sql for sql in executed)

    def test_failed_promotion_retains_staging_and_reports_failure(self):
        dag, selected = _single_table_dag()
        adapter, executed = _make_adapter(fail_on="SWAP WITH")

        results = list(run_project(
            dag, selected, adapter,
            run_id=RUN_ID, after_node_success=lambda _n, _p: True, strict=True,
        ))

        assert results[0].status == RunStatus.FAILURE
        assert "promotion failed" in results[0].error
        assert "retained" in results[0].error

    def test_incremental_clones_target_into_staging_first(self):
        dag, selected = _single_table_dag(
            run_config=RunConfig(
                run_mode=RunMode.INCREMENTAL, incremental_mode=IncrementalMode.APPEND
            )
        )
        adapter, executed = _make_adapter()

        list(run_project(
            dag, selected, adapter,
            run_mode=RunMode.INCREMENTAL, run_id=RUN_ID,
            after_node_success=lambda _n, _p: True, strict=True,
        ))

        staging = strict_staging_name("db.s.orders", RUN_ID)
        clone_index = next(i for i, sql in enumerate(executed) if "CLONE db.s.orders" in sql)
        insert_index = next(i for i, sql in enumerate(executed) if sql.startswith(f"INSERT INTO {staging}"))
        assert clone_index < insert_index

    def test_incremental_fallback_to_full_refresh_skips_the_clone(self):
        dag, selected = _single_table_dag(
            run_config=RunConfig(
                run_mode=RunMode.INCREMENTAL, incremental_mode=IncrementalMode.APPEND
            )
        )
        adapter, executed = _make_adapter(target_exists=False)

        list(run_project(
            dag, selected, adapter,
            run_mode=RunMode.INCREMENTAL, run_id=RUN_ID,
            after_node_success=lambda _n, _p: True, strict=True,
        ))

        assert not any("CLONE" in sql for sql in executed)

    def test_view_is_created_in_staging_then_replaced_at_target(self):
        dag, selected = _single_table_dag(trouve_type=TrouveType.VIEW)
        adapter, executed = _make_adapter()

        list(run_project(
            dag, selected, adapter,
            run_id=RUN_ID, after_node_success=lambda _n, _p: True, strict=True,
        ))

        staging = strict_staging_name("db.s.orders", RUN_ID)
        assert any(f"CREATE OR REPLACE VIEW {staging}" in sql for sql in executed)
        assert any("CREATE OR REPLACE VIEW db.s.orders" in sql for sql in executed)
        assert any(f"DROP VIEW IF EXISTS {staging}" in sql for sql in executed)

    def test_downstream_is_skipped_when_upstream_tests_fail(self):
        upstream = _compile(Trouve(sql="SELECT 1 AS id"), "db.s.upstream")
        downstream = _compile(
            Trouve(sql="SELECT * FROM db.s.upstream"),
            "db.s.downstream",
            imports=["db.s.upstream"],
        )
        dag = build_dag([upstream, downstream])
        adapter, _ = _make_adapter()

        results = list(run_project(
            dag, get_executable_nodes(dag), adapter,
            run_id=RUN_ID,
            after_node_success=lambda node_name, _p: node_name != "db.s.upstream",
            strict=True,
        ))

        by_name = {r.full_name: r for r in results}
        assert by_name["db.s.upstream"].status == RunStatus.FAILURE
        assert by_name["db.s.downstream"].status == RunStatus.SKIPPED
        assert by_name["db.s.downstream"].skipped_by == "db.s.upstream"

    def test_downstream_reads_the_promoted_name_of_its_upstream(self):
        """Promotion happens per node, so dependents never see a staging name."""
        upstream = _compile(Trouve(sql="SELECT 1 AS id"), "db.s.upstream")
        downstream = _compile(
            Trouve(sql="SELECT * FROM db.s.upstream"),
            "db.s.downstream",
            imports=["db.s.upstream"],
        )
        dag = build_dag([upstream, downstream])
        adapter, executed = _make_adapter()

        list(run_project(
            dag, get_executable_nodes(dag), adapter,
            run_id=RUN_ID, after_node_success=lambda _n, _p: True, strict=True,
        ))

        downstream_staging = strict_staging_name("db.s.downstream", RUN_ID)
        build_sql = next(sql for sql in executed if f"CREATE OR REPLACE TABLE {downstream_staging}" in sql)
        assert "FROM db.s.upstream" in build_sql
        assert STRICT_SUFFIX not in build_sql.split("FROM")[1]

    def test_strict_without_tests_is_rejected(self):
        dag, selected = _single_table_dag()
        adapter, _ = _make_adapter()

        with pytest.raises(RunError, match="strict mode requires tests"):
            list(run_project(dag, selected, adapter, run_id=RUN_ID, strict=True))

    def test_naming_failure_fails_the_node_rather_than_the_run(self):
        long_name = "x" * MAX_IDENTIFIER_LENGTH
        trouve = _compile(Trouve(sql="SELECT 1 AS id"), f"db.s.{long_name}")
        dag = build_dag([trouve])
        adapter, executed = _make_adapter()

        results = list(run_project(
            dag, get_executable_nodes(dag), adapter,
            run_id=RUN_ID, after_node_success=lambda _n, _p: True, strict=True,
        ))

        assert results[0].status == RunStatus.FAILURE
        assert "max 255" in results[0].error
        assert not any("CREATE OR REPLACE TABLE" in sql for sql in executed)


class TestStrictRunnerPandas:
    def test_dataframe_is_written_to_staging_then_swapped(self):
        def transform() -> pd.DataFrame:
            return pd.DataFrame({"id": [1, 2]})

        trouve = _compile(
            Trouve(df_fn=transform),
            "db.s.orders",
            execution_type=ExecutionType.PANDAS,
        )
        dag = build_dag([trouve])
        adapter, executed = _make_adapter()
        adapter.write_dataframe = MagicMock(
            return_value=QueryResult(query_id="w1", query_url="u1", success=True)
        )

        results = list(run_project(
            dag, get_executable_nodes(dag), adapter,
            run_id=RUN_ID, after_node_success=lambda _n, _p: True, strict=True,
        ))

        staging = strict_staging_name("db.s.orders", RUN_ID)
        assert results[0].status == RunStatus.SUCCESS
        # Reported under the real name even though the write went to staging.
        assert results[0].full_name == "db.s.orders"
        assert adapter.write_dataframe.call_args.kwargs["full_name"] == staging
        assert adapter.write_dataframe.call_args.kwargs["table_name"] == staging.split(".")[2]
        assert any(f"ALTER TABLE {staging} SWAP WITH db.s.orders" in sql for sql in executed)

    def test_failing_tests_discard_the_staging_table(self):
        def transform() -> pd.DataFrame:
            return pd.DataFrame({"id": [1, 2]})

        trouve = _compile(
            Trouve(df_fn=transform),
            "db.s.orders",
            execution_type=ExecutionType.PANDAS,
        )
        dag = build_dag([trouve])
        adapter, executed = _make_adapter()
        adapter.write_dataframe = MagicMock(
            return_value=QueryResult(query_id="w1", query_url="u1", success=True)
        )

        results = list(run_project(
            dag, get_executable_nodes(dag), adapter,
            run_id=RUN_ID, after_node_success=lambda _n, _p: False, strict=True,
        ))

        staging = strict_staging_name("db.s.orders", RUN_ID)
        assert results[0].status == RunStatus.FAILURE
        assert any(f"DROP TABLE IF EXISTS {staging}" in sql for sql in executed)


class TestNonStrictUnchanged:
    def test_target_is_written_directly_and_tested_afterwards(self):
        dag, selected = _single_table_dag()
        adapter, executed = _make_adapter()
        tested: list[str] = []

        results = list(run_project(
            dag, selected, adapter,
            run_id=RUN_ID,
            after_node_success=lambda _n, physical: (tested.append(physical) or True),
        ))

        assert results[0].status == RunStatus.SUCCESS
        assert tested == ["db.s.orders"]
        assert any("CREATE OR REPLACE TABLE db.s.orders" in sql for sql in executed)
        assert not any(STRICT_SUFFIX in sql for sql in executed)

    def test_failing_tests_do_not_fail_the_node_itself(self):
        """Without strict mode the write already happened; only downstream is cut off."""
        dag, selected = _single_table_dag()
        adapter, _ = _make_adapter()

        results = list(run_project(
            dag, selected, adapter,
            run_id=RUN_ID, after_node_success=lambda _n, _p: False,
        ))

        assert results[0].status == RunStatus.SUCCESS


class TestPhysicalNameOverride:
    """run_tests can be pointed at a staging object while still reporting the real name."""

    def _dag_with_test(self):
        from clair.trouves.test import TestNotNull

        trouve = _compile(
            Trouve(sql="SELECT 1 AS id", tests=[TestNotNull(column="id")]),
            "db.s.orders",
        )
        return build_dag([trouve])

    def test_sql_queries_the_override(self):
        from clair.core.test_runner import run_tests

        dag = self._dag_with_test()
        adapter, executed = _make_adapter()

        run_tests(dag, ["db.s.orders"], adapter, physical_names={"db.s.orders": "db.s.staging"})

        assert any("db.s.staging" in sql for sql in executed)
        assert not any("FROM db.s.orders" in sql for sql in executed)

    def test_results_still_report_the_routed_name(self):
        from clair.core.test_runner import run_tests

        dag = self._dag_with_test()
        adapter, _ = _make_adapter()

        results = run_tests(
            dag, ["db.s.orders"], adapter, physical_names={"db.s.orders": "db.s.staging"}
        )

        assert [r.full_name for r in results] == ["db.s.orders"]

    def test_sampling_applies_to_the_override(self):
        from clair.core.test_runner import run_tests

        dag = self._dag_with_test()
        adapter, executed = _make_adapter()

        run_tests(
            dag, ["db.s.orders"], adapter,
            use_sample=True,
            physical_names={"db.s.orders": "db.s.staging"},
        )

        assert any("SELECT TOP 1000 * FROM db.s.staging" in sql for sql in executed)
        assert not any("db.s.orders" in sql for sql in executed)


class TestStrictCliGuard:
    def test_strict_with_no_test_is_rejected(self):
        import structlog
        from click.testing import CliRunner

        from clair.cli.main import cli

        try:
            result = CliRunner().invoke(cli, ["run", "--strict", "--no-test"])
        finally:
            # The CLI binds structlog to the runner's stdout/stderr, which are
            # closed on exit; reset so later tests log to real streams.
            structlog.reset_defaults()

        assert result.exit_code == 1


class TestStrictCompilePlan:
    def test_plan_shows_staging_build_test_checkpoint_and_swap(self):
        trouve = _compile(Trouve(sql="SELECT 1 AS id"), "db.s.orders")
        statements = build_statements(trouve, RunMode.FULL_REFRESH, RUN_ID, strict=True)

        staging = strict_staging_name("db.s.orders", RUN_ID)
        assert f"CREATE OR REPLACE TABLE {staging}" in statements[0]
        assert "tests run against the staging object" in statements[1]
        assert f"ALTER TABLE {staging} SWAP WITH db.s.orders" in statements[2]

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
        statements = build_statements(trouve, RunMode.INCREMENTAL, RUN_ID, strict=True)
        assert "CLONE db.s.orders" in statements[0]

    def test_non_strict_plan_is_the_plain_build(self):
        trouve = _compile(Trouve(sql="SELECT 1 AS id"), "db.s.orders")
        assert build_statements(trouve, RunMode.FULL_REFRESH, RUN_ID) == trouve.build_sql(
            RunMode.FULL_REFRESH, RUN_ID
        )
