"""A Table with ``iceberg`` makes a Snowflake-managed Iceberg table in each CREATE and each ALTER.

The staging flow stays as it is: Snowflake builds, clones and promotes an
Iceberg table in the same way as a standard table. Snowflake cannot replace a
standard table with an Iceberg table, or the reverse, thus a change of kind
stops before the promotion, as a change between a TABLE and a VIEW does.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pydantic import ValidationError

from clair.adapters.base import WarehouseColumn
from clair.core.compiler import build_statements
from clair.core.runner import RunStatus
from clair.core.staging import make_staging_address
from clair.trouves.address import TrouveAddress
from clair.trouves.pandas_table import PandasTable
from clair.trouves.run_config import Append, FullRefresh, Incremental, OnSchemaChange, RunMode
from clair.trouves.seed_table import SeedTable
from clair.trouves.trouve import Iceberg, Table, View
from tests.helpers import (
    TEST_ADDRESS,
    TEST_RUN_ID,
    RecordingAdapter,
    compile_trouve,
    executed_sql,
    run_one_trouve,
)

PHYSICAL_ADDRESS = TrouveAddress.parse(TEST_ADDRESS)
STAGING_ADDRESS = make_staging_address(PHYSICAL_ADDRESS, TEST_RUN_ID)
ID = WarehouseColumn(name="ID", data_type="NUMBER(38,0)")
LABEL = WarehouseColumn(name="LABEL", data_type="VARCHAR(16777216)")

PROMOTION = f"CREATE OR REPLACE ICEBERG TABLE {PHYSICAL_ADDRESS} CLONE {STAGING_ADDRESS} COPY GRANTS"
CLONE = f"CREATE OR REPLACE ICEBERG TABLE {STAGING_ADDRESS} CLONE {PHYSICAL_ADDRESS}"


def iceberg_table(
    iceberg: Iceberg | None = None,
    incremental: bool = False,
    cluster_by: list[str] | None = None,
) -> Table:
    run_config = FullRefresh()
    if incremental:
        run_config = Incremental(strategy=Append(), on_schema_change=OnSchemaChange.ADD_NEW_COLUMNS)
    return Table(
        sql="SELECT 1 AS id",
        iceberg=iceberg or Iceberg(),
        cluster_by=cluster_by or [],
        run_config=run_config,
    )


def contains_statement(sql_texts: list[str], expected: str) -> bool:
    return any(expected in sql for sql in sql_texts)


class TestTheOption:
    def test_a_table_accepts_it(self):
        table = Table(sql="SELECT 1 AS id", iceberg=Iceberg(external_volume="lake"))
        assert table.iceberg == Iceberg(external_volume="lake")

    def test_a_table_is_standard_by_default(self):
        assert Table(sql="SELECT 1 AS id").iceberg is None

    @pytest.mark.parametrize(
        "make_trouve",
        [
            lambda: PandasTable(transform=lambda: pd.DataFrame(), iceberg=Iceberg()),  # ty: ignore[unknown-argument]
            lambda: SeedTable(dataframe=pd.DataFrame({"id": [1]}), iceberg=Iceberg()),  # ty: ignore[unknown-argument]
            lambda: View(sql="SELECT 1 AS id", iceberg=Iceberg()),  # ty: ignore[unknown-argument]
        ],
        ids=["PandasTable", "SeedTable", "View"],
    )
    def test_each_other_class_rejects_it(self, make_trouve):
        with pytest.raises(ValidationError, match="iceberg"):
            make_trouve()

    def test_a_transient_iceberg_table_is_rejected(self):
        with pytest.raises(ValidationError, match="transient and Iceberg"):
            Table(sql="SELECT 1 AS id", iceberg=Iceberg(), transient=True)

    @pytest.mark.parametrize("external_volume", ["", "  "])
    def test_an_empty_external_volume_is_rejected(self, external_volume: str):
        with pytest.raises(ValidationError, match="external_volume"):
            Iceberg(external_volume=external_volume)


class TestThePlan:
    def test_a_full_refresh_builds_an_iceberg_table_that_snowflake_catalogs(self):
        statements = build_statements(
            compile_trouve(iceberg_table()), RunMode.FULL_REFRESH, TEST_RUN_ID
        )

        assert contains_statement(
            statements,
            f"CREATE OR REPLACE ICEBERG TABLE {STAGING_ADDRESS}\nCATALOG = 'SNOWFLAKE' AS (",
        )
        assert not contains_statement(statements, "EXTERNAL_VOLUME")
        assert contains_statement(statements, PROMOTION)

    def test_the_build_names_the_external_volume(self):
        statements = build_statements(
            compile_trouve(iceberg_table(Iceberg(external_volume="it's_lake"))),
            RunMode.FULL_REFRESH,
            TEST_RUN_ID,
        )

        assert contains_statement(
            statements,
            f"{STAGING_ADDRESS}\nEXTERNAL_VOLUME = 'it''s_lake'\nCATALOG = 'SNOWFLAKE' AS (",
        )

    def test_an_incremental_run_clones_an_iceberg_table(self):
        statements = build_statements(
            compile_trouve(iceberg_table(incremental=True)), RunMode.INCREMENTAL, TEST_RUN_ID
        )

        assert contains_statement(statements, CLONE)
        assert contains_statement(statements, PROMOTION)

    def test_the_clustering_statements_alter_an_iceberg_table(self):
        statements = build_statements(
            compile_trouve(iceberg_table(cluster_by=["id"])), RunMode.FULL_REFRESH, TEST_RUN_ID
        )

        assert contains_statement(statements, f"ALTER ICEBERG TABLE {STAGING_ADDRESS} CLUSTER BY (id)")
        assert contains_statement(statements, f"ALTER ICEBERG TABLE {PHYSICAL_ADDRESS} RESUME RECLUSTER")
        assert not contains_statement(statements, "ALTER TABLE")

    def test_the_comment_names_the_iceberg_type(self):
        statements = build_statements(
            compile_trouve(iceberg_table()), RunMode.FULL_REFRESH, TEST_RUN_ID
        )

        assert contains_statement(statements, "of a type other than ICEBERG TABLE")


class TestTheRun:
    def test_an_incremental_run_on_an_iceberg_table_stays_incremental(self):
        adapter = RecordingAdapter(
            table_columns=[ID], query_columns=[ID], existing_iceberg_tables=[TEST_ADDRESS]
        )

        result = run_one_trouve(
            iceberg_table(incremental=True), adapter, run_mode=RunMode.INCREMENTAL
        )

        assert result.status == RunStatus.SUCCESS, result.error
        assert result.effective_run_mode == RunMode.INCREMENTAL
        assert contains_statement(executed_sql(result), CLONE)
        assert contains_statement(executed_sql(result), PROMOTION)

    def test_a_new_column_is_added_with_an_iceberg_alter(self):
        adapter = RecordingAdapter(
            table_columns=[ID], query_columns=[ID, LABEL], existing_iceberg_tables=[TEST_ADDRESS]
        )

        result = run_one_trouve(
            iceberg_table(incremental=True), adapter, run_mode=RunMode.INCREMENTAL
        )

        assert result.status == RunStatus.SUCCESS, result.error
        assert contains_statement(
            executed_sql(result), f'ALTER ICEBERG TABLE {STAGING_ADDRESS} ADD COLUMN "LABEL"'
        )

    @pytest.mark.parametrize(
        ("trouve", "adapter", "old_type", "new_type"),
        [
            (
                iceberg_table(),
                RecordingAdapter(existing_tables=[TEST_ADDRESS]),
                "TABLE",
                "ICEBERG TABLE",
            ),
            (
                Table(sql="SELECT 1 AS id"),
                RecordingAdapter(existing_iceberg_tables=[TEST_ADDRESS]),
                "ICEBERG TABLE",
                "TABLE",
            ),
        ],
        ids=["standard to iceberg", "iceberg to standard"],
    )
    def test_a_change_of_kind_stops_before_the_promotion_and_names_the_drop(
        self, trouve: Table, adapter: RecordingAdapter, old_type: str, new_type: str
    ):
        result = run_one_trouve(trouve, adapter)

        assert result.status == RunStatus.FAILURE
        assert f"holds the type {old_type}, and the Trouve now makes the type {new_type}" in result.error
        assert f"Run DROP {old_type} {TEST_ADDRESS}" in result.error
        assert not contains_statement(executed_sql(result), f"{PHYSICAL_ADDRESS} CLONE")

    def test_an_incremental_change_of_kind_builds_from_the_sql_then_stops(self):
        # The clone of a standard table cannot make an Iceberg table. The run
        # thus builds a full refresh candidate, and the promotion stops.
        adapter = RecordingAdapter(table_columns=[ID], query_columns=[ID], existing_tables=[TEST_ADDRESS])

        result = run_one_trouve(
            iceberg_table(incremental=True), adapter, run_mode=RunMode.INCREMENTAL
        )

        assert result.status == RunStatus.FAILURE
        assert result.effective_run_mode == RunMode.FULL_REFRESH
        assert not contains_statement(executed_sql(result), CLONE)
        assert "Run DROP TABLE" in result.error
