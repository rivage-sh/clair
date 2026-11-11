"""A transient Trouve makes a TRANSIENT table in each CREATE: the build, the staging clone and the promotion.

Snowflake cannot clone a transient table into a permanent one. Thus one
statement with the wrong kind breaks the run, or changes the kind of the
production table. ``__database_config__.py`` and ``__schema_config__.py`` can
set the kind for each table below them.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from clair.adapters.base import WarehouseColumn
from clair.core.compiler import build_statements
from clair.core.discovery import discover_project
from clair.core.runner import RunSuccess
from clair.core.staging import make_staging_address
from clair.exceptions import TrouveNotCompiledError
from clair.trouves.address import TrouveAddress
from clair.trouves.config import SchemaDefaults
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
    write_layered_project,
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
        table_options = table_options_of(trouve)
        assert table_options is not None
        assert table_options.transient

    def test_a_table_takes_the_directory_value_by_default(self):
        assert Table(sql="SELECT 1 AS id").transient is None

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

        assert isinstance(result, RunSuccess)
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

        assert isinstance(result, RunSuccess)
        assert adapter.written_addresses == [str(STAGING_ADDRESS)]
        assert adapter.transient_addresses == ([str(STAGING_ADDRESS)] if transient else [])
        assert contains_statement(
            executed_sql(result),
            f"CREATE OR REPLACE {table_keyword_of(transient)} {PHYSICAL_ADDRESS} "
            f"CLONE {STAGING_ADDRESS} COPY GRANTS",
        )


def discovered_trouve(root: Path) -> SqlTrouve:
    (trouve,) = discover_project(root)
    assert isinstance(trouve, SqlTrouve)
    return trouve


class TestTheDirectoryDefault:
    @pytest.mark.parametrize(
        ("database_text", "schema_text", "trouve_text", "expected_transient"),
        [
            ("", "", "", False),
            ("transient=True", "", "", True),
            ("", "transient=True", "", True),
            ("transient=True", "transient=False", "", False),
            ("", "transient=True", "transient=False", False),
            ("transient=False", "", "transient=True", True),
        ],
        ids=[
            "no-level",
            "database",
            "schema",
            "schema-replaces-database",
            "trouve-replaces-schema",
            "trouve-replaces-database",
        ],
    )
    def test_the_lowest_level_that_sets_it_wins(
        self, tmp_path, database_text, schema_text, trouve_text, expected_transient
    ):
        root = write_layered_project(
            tmp_path, database_text, schema_text, f'Table(sql="select 1 as n", {trouve_text})'
        )
        trouve = discovered_trouve(root)

        assert isinstance(trouve, Table)
        assert trouve.is_transient == expected_transient

    def test_a_directory_default_makes_each_create_transient(self, tmp_path):
        root = write_layered_project(tmp_path, "", "transient=True", 'Table(sql="select 1 as n")')
        trouve = discovered_trouve(root)
        assert trouve.compiled is not None
        physical_address = trouve.compiled.physical_address
        staging_address = make_staging_address(physical_address, TEST_RUN_ID)

        statements = build_statements(trouve, RunMode.FULL_REFRESH, TEST_RUN_ID, use_staging=True)

        assert contains_statement(
            statements, f"CREATE OR REPLACE TRANSIENT TABLE {staging_address} AS"
        )
        assert contains_statement(
            statements,
            f"CREATE OR REPLACE TRANSIENT TABLE {physical_address} CLONE {staging_address} COPY GRANTS",
        )

    @pytest.mark.parametrize(
        ("trouve_expression", "expected_keyword"),
        [
            ('View(sql="select 1 as n")', "VIEW"),
            ('Table(sql="select 1 as n", iceberg=Iceberg())', "ICEBERG TABLE"),
        ],
        ids=["View", "Iceberg"],
    )
    def test_an_object_that_cannot_be_transient_ignores_it(
        self, tmp_path, trouve_expression, expected_keyword
    ):
        root = write_layered_project(tmp_path, "transient=True", "", trouve_expression)
        trouve = discovered_trouve(root)
        assert trouve.compiled is not None

        statements = build_statements(trouve, RunMode.FULL_REFRESH, TEST_RUN_ID, use_staging=False)

        assert not trouve.compiled.config.transient
        assert contains_statement(
            statements, f"CREATE OR REPLACE {expected_keyword} {trouve.compiled.physical_address}"
        )

    def test_a_seed_writes_a_transient_table(self):
        adapter = RecordingAdapter()
        seed = SeedTable(dataframe=pd.DataFrame({"id": [1]}))

        result = run_one_trouve(seed, adapter, defaults=SchemaDefaults(transient=True))

        assert isinstance(result, RunSuccess)
        assert adapter.transient_addresses == [str(STAGING_ADDRESS)]

    def test_the_resolved_value_needs_discovery(self):
        with pytest.raises(TrouveNotCompiledError):
            _ = Table(sql="SELECT 1 AS id").is_transient
