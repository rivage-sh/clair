"""A Trouve with ``cluster_by`` keeps its clustering key, and Automatic Clustering, on the physical table.

Snowflake copies the clustering key into a clone, but it suspends Automatic
Clustering on the clone. Each promotion is a clone, thus a promotion with no
``RESUME RECLUSTER`` turns reclustering off on the production table.
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
from clair.trouves.run_config import Append, Incremental, RunMode
from clair.trouves.seed_table import SeedTable
from clair.trouves.trouve import Source, SqlTrouve, Table, TrouveAbc, View, table_options_of
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
CLUSTER_BY = ["id", "to_date(created_at)"]

CLUSTER_STAGING = f"ALTER TABLE {STAGING_ADDRESS} CLUSTER BY (id, to_date(created_at))"
CLUSTER_PHYSICAL = f"ALTER TABLE {PHYSICAL_ADDRESS} CLUSTER BY (id, to_date(created_at))"
RESUME_PHYSICAL = f"ALTER TABLE {PHYSICAL_ADDRESS} RESUME RECLUSTER"
PROMOTION = f"CREATE OR REPLACE TABLE {PHYSICAL_ADDRESS} CLONE {STAGING_ADDRESS} COPY GRANTS"


def full_refresh_table(cluster_by: list[str]) -> Table:
    return Table(sql="SELECT 1 AS id", cluster_by=cluster_by)


def incremental_table(cluster_by: list[str]) -> Table:
    return Table(
        sql="SELECT 1 AS id",
        run_config=Incremental(strategy=Append()),
        cluster_by=cluster_by,
    )


def index_of(sql_texts: list[str], expected: str) -> int:
    """Give the position of the first statement that holds the text, or -1."""
    return next(
        (position for position, sql in enumerate(sql_texts) if expected in sql), -1
    )


class TestTheOption:
    @pytest.mark.parametrize(
        "trouve",
        [
            Table(sql="SELECT 1 AS id", cluster_by=CLUSTER_BY),
            PandasTable(transform=lambda: pd.DataFrame({"id": [1]}), cluster_by=CLUSTER_BY),
            SeedTable(dataframe=pd.DataFrame({"id": [1]}), cluster_by=CLUSTER_BY),
        ],
        ids=["Table", "PandasTable", "SeedTable"],
    )
    def test_each_class_that_makes_a_table_accepts_it(self, trouve: TrouveAbc):
        table_options = table_options_of(trouve)
        assert table_options is not None
        assert table_options.cluster_by == CLUSTER_BY

    def test_a_table_has_no_key_by_default(self):
        assert Table(sql="SELECT 1 AS id").cluster_by == []

    @pytest.mark.parametrize("expression", ["", "   "])
    def test_an_empty_expression_is_rejected(self, expression: str):
        with pytest.raises(ValidationError, match="cluster_by"):
            Table(sql="SELECT 1 AS id", cluster_by=["id", expression])

    def test_a_view_rejects_it(self):
        with pytest.raises(ValidationError, match="cluster_by"):
            View(sql="SELECT 1 AS id", cluster_by=CLUSTER_BY)  # ty: ignore[unknown-argument]

    def test_a_source_rejects_it(self):
        with pytest.raises(ValidationError, match="cluster_by"):
            Source(cluster_by=CLUSTER_BY)  # ty: ignore[unknown-argument]


class TestThePlan:
    @pytest.mark.parametrize(
        ("trouve", "run_mode"),
        [
            (full_refresh_table(CLUSTER_BY), RunMode.FULL_REFRESH),
            (incremental_table(CLUSTER_BY), RunMode.INCREMENTAL),
        ],
        ids=["full refresh", "incremental"],
    )
    def test_a_staged_plan_clusters_the_staging_table_then_resumes_after_the_promotion(
        self, trouve: Table, run_mode: RunMode
    ):
        statements = build_statements(compile_trouve(trouve), run_mode, TEST_RUN_ID)

        build_position = index_of(statements, f"{STAGING_ADDRESS}")
        cluster_position = index_of(statements, CLUSTER_STAGING)
        promotion_position = index_of(statements, PROMOTION)
        resume_position = index_of(statements, RESUME_PHYSICAL)
        assert -1 < build_position < cluster_position < promotion_position < resume_position

    def test_an_incremental_plan_clusters_after_the_clone(self):
        statements = build_statements(
            compile_trouve(incremental_table(CLUSTER_BY)), RunMode.INCREMENTAL, TEST_RUN_ID
        )

        clone_position = index_of(statements, f"CLONE {PHYSICAL_ADDRESS}")
        assert -1 < clone_position < index_of(statements, CLUSTER_STAGING)

    def test_a_plan_with_no_staging_clusters_the_physical_table_and_resumes_nothing(self):
        statements = build_statements(
            compile_trouve(full_refresh_table(CLUSTER_BY)),
            RunMode.FULL_REFRESH,
            TEST_RUN_ID,
            use_staging=False,
        )

        assert index_of(statements, CLUSTER_PHYSICAL) > index_of(
            statements, f"CREATE OR REPLACE TABLE {PHYSICAL_ADDRESS} AS"
        )
        assert index_of(statements, "RESUME RECLUSTER") == -1

    @pytest.mark.parametrize("use_staging", [True, False])
    @pytest.mark.parametrize(
        "trouve",
        [full_refresh_table([]), View(sql="SELECT 1 AS id")],
        ids=["table", "view"],
    )
    def test_a_trouve_with_no_key_gets_no_clustering_statement(
        self, trouve: SqlTrouve, use_staging: bool
    ):
        statements = build_statements(
            compile_trouve(trouve), RunMode.FULL_REFRESH, TEST_RUN_ID, use_staging=use_staging
        )

        assert index_of(statements, "CLUSTER") == -1


class TestTheRun:
    def test_an_incremental_run_clusters_the_clone_and_resumes_the_promoted_table(self):
        adapter = RecordingAdapter(table_columns=[ID], query_columns=[ID])

        result = run_one_trouve(
            incremental_table(CLUSTER_BY), adapter, run_mode=RunMode.INCREMENTAL
        )

        assert result.status == RunStatus.SUCCESS
        sql_texts = executed_sql(result)
        insert_position = index_of(sql_texts, f"INSERT INTO {STAGING_ADDRESS}")
        assert -1 < insert_position < index_of(sql_texts, CLUSTER_STAGING)
        assert index_of(sql_texts, PROMOTION) < index_of(sql_texts, RESUME_PHYSICAL)

    def test_a_seed_clusters_the_written_table_and_resumes_the_promoted_table(self):
        adapter = RecordingAdapter()
        seed = SeedTable(dataframe=pd.DataFrame({"id": [1]}), cluster_by=CLUSTER_BY)

        result = run_one_trouve(seed, adapter)

        assert result.status == RunStatus.SUCCESS
        assert adapter.written_addresses == [str(STAGING_ADDRESS)]
        sql_texts = executed_sql(result)
        assert -1 < index_of(sql_texts, CLUSTER_STAGING) < index_of(sql_texts, PROMOTION)
        assert index_of(sql_texts, PROMOTION) < index_of(sql_texts, RESUME_PHYSICAL)

    @pytest.mark.parametrize(
        "trouve",
        [
            full_refresh_table(CLUSTER_BY),
            SeedTable(dataframe=pd.DataFrame({"id": [1]}), cluster_by=CLUSTER_BY),
        ],
        ids=["Table", "SeedTable"],
    )
    def test_a_key_that_snowflake_rejects_stops_the_node_before_the_promotion(
        self, trouve: Table | SeedTable
    ):
        adapter = RecordingAdapter(fail_on=["CLUSTER BY"])

        result = run_one_trouve(trouve, adapter)

        assert result.status == RunStatus.FAILURE
        assert index_of(executed_sql(result), PROMOTION) == -1

    def test_a_failed_resume_fails_the_node_after_the_promotion(self):
        adapter = RecordingAdapter(fail_on=["RESUME RECLUSTER"])

        result = run_one_trouve(full_refresh_table(CLUSTER_BY), adapter)

        assert result.status == RunStatus.FAILURE
        assert index_of(executed_sql(result), PROMOTION) > -1
        assert RESUME_PHYSICAL in result.error
