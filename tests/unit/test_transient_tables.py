"""A transient Trouve makes a TRANSIENT table in each CREATE: the build, the staging clone and the promotion.

Snowflake cannot clone a transient table into a permanent one. Thus one
statement with the wrong kind breaks the run, or changes the kind of the
production table.
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
from clair.trouves.trouve import Source, Table, TrouveAbc, View, is_transient
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


def table_keyword_of(transient: bool) -> str:
    return "TRANSIENT TABLE" if transient else "TABLE"


def incremental_table(transient: bool) -> Table:
    return Table(
        sql="SELECT 1 AS id",
        run_config=Incremental(strategy=Append()),
        transient=transient,
    )


def contains_statement(sql_texts: list[str], expected: str) -> bool:
    return any(expected in sql for sql in sql_texts)


class TestTheOption:
    @pytest.mark.parametrize(
        "trouve",
        [
            Table(sql="SELECT 1 AS id", transient=True),
            PandasTable(transform=lambda: pd.DataFrame({"id": [1]}), transient=True),
            SeedTable(dataframe=pd.DataFrame({"id": [1]}), transient=True),
        ],
        ids=["Table", "PandasTable", "SeedTable"],
    )
    def test_each_class_that_makes_a_table_accepts_it(self, trouve: TrouveAbc):
        assert is_transient(trouve)

    def test_a_table_is_permanent_by_default(self):
        assert not is_transient(Table(sql="SELECT 1 AS id"))

    def test_a_view_rejects_it(self):
        with pytest.raises(ValidationError, match="transient"):
            View(sql="SELECT 1 AS id", transient=True)  # ty: ignore[unknown-argument]

    def test_a_source_rejects_it(self):
        with pytest.raises(ValidationError, match="transient"):
            Source(transient=True)  # ty: ignore[unknown-argument]


@pytest.mark.parametrize("transient", [False, True])
class TestThePlan:
    def test_a_staged_full_refresh_builds_and_promotes_the_kind(self, transient: bool):
        trouve = compile_trouve(Table(sql="SELECT 1 AS id", transient=transient))
        keyword = table_keyword_of(transient)

        statements = build_statements(trouve, RunMode.FULL_REFRESH, TEST_RUN_ID, use_staging=True)

        assert contains_statement(statements, f"CREATE OR REPLACE {keyword} {STAGING_ADDRESS} AS")
        assert contains_statement(
            statements,
            f"CREATE OR REPLACE {keyword} {PHYSICAL_ADDRESS} CLONE {STAGING_ADDRESS} COPY GRANTS",
        )

    def test_a_staged_incremental_run_clones_into_the_kind(self, transient: bool):
        trouve = compile_trouve(incremental_table(transient))
        keyword = table_keyword_of(transient)

        statements = build_statements(trouve, RunMode.INCREMENTAL, TEST_RUN_ID, use_staging=True)

        assert contains_statement(
            statements, f"CREATE OR REPLACE {keyword} {STAGING_ADDRESS} CLONE {PHYSICAL_ADDRESS}"
        )
        assert contains_statement(
            statements,
            f"CREATE OR REPLACE {keyword} {PHYSICAL_ADDRESS} CLONE {STAGING_ADDRESS} COPY GRANTS",
        )

    def test_a_full_refresh_with_no_staging_makes_the_kind(self, transient: bool):
        trouve = compile_trouve(Table(sql="SELECT 1 AS id", transient=transient))
        keyword = table_keyword_of(transient)

        statements = build_statements(trouve, RunMode.FULL_REFRESH, TEST_RUN_ID, use_staging=False)

        assert contains_statement(statements, f"CREATE OR REPLACE {keyword} {PHYSICAL_ADDRESS} AS")


@pytest.mark.parametrize("transient", [False, True])
class TestTheRun:
    def test_an_incremental_run_clones_and_promotes_the_kind(self, transient: bool):
        adapter = RecordingAdapter(table_columns=[ID], query_columns=[ID])
        keyword = table_keyword_of(transient)

        result = run_one_trouve(incremental_table(transient), adapter, run_mode=RunMode.INCREMENTAL)

        assert result.status == RunStatus.SUCCESS
        sql_texts = executed_sql(result)
        assert contains_statement(
            sql_texts, f"CREATE OR REPLACE {keyword} {STAGING_ADDRESS} CLONE {PHYSICAL_ADDRESS}"
        )
        assert contains_statement(
            sql_texts,
            f"CREATE OR REPLACE {keyword} {PHYSICAL_ADDRESS} CLONE {STAGING_ADDRESS} COPY GRANTS",
        )

    def test_a_seed_writes_and_promotes_the_kind(self, transient: bool):
        adapter = RecordingAdapter()
        seed = SeedTable(dataframe=pd.DataFrame({"id": [1]}), transient=transient)

        result = run_one_trouve(seed, adapter)

        assert result.status == RunStatus.SUCCESS
        assert adapter.written_addresses == [str(STAGING_ADDRESS)]
        assert adapter.transient_addresses == ([str(STAGING_ADDRESS)] if transient else [])
        assert contains_statement(
            executed_sql(result),
            f"CREATE OR REPLACE {table_keyword_of(transient)} {PHYSICAL_ADDRESS} "
            f"CLONE {STAGING_ADDRESS} COPY GRANTS",
        )
