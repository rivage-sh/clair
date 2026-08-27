"""Prove SeedTrouve against a real Snowflake account.

A seed holds its rows in the Python file, and the dtype of each column gives the
Snowflake type. Only the warehouse can prove that claim: clair sends no DDL for
a seed. It writes a Parquet file to a stage, and Snowflake infers the schema of
that file. A mock adapter shows the DataFrame that clair gives to the adapter.
It cannot show which column type Snowflake made, nor that an integer column with
a null stays an integer.

These tests write their own project. Each test class gives its own database
name, which becomes the first part of each logical address, thus two tests never
write one table in the schema of the run.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

import clair
from clair import TrouveAddress
from clair.adapters.snowflake import SnowflakeAdapter
from clair.core.runner import RunStatus, RunSummary
from tests.integration.config import IntegrationConfig
from tests.integration.projects import (
    CI_DATABASE_CONFIG_FILE,
    CI_ROUTING_FILE,
    physical_address,
)
from tests.integration.warehouse import query_rows, row_count, staging_objects

pytestmark = pytest.mark.integration

# The seed of the project below. Each column carries a dtype on purpose:
#
#   code        string          -> a text column
#   quantity    Int64, one null -> a number column, and 10 keeps no ".0"
#   rate        float64         -> a floating point column
#   is_active   bool            -> BOOLEAN
#   valid_from  datetime64[ns]  -> a timestamp column
#
# The nullable Int64 dtype is the interesting one. Plain pandas turns
# [10, 20, None] into float64, and the values reach Snowflake as 10.0 and 20.0.
SEED_FILE = '''\
import pandas as pd

from clair import Column, ColumnType, SeedTrouve

labels = pd.DataFrame(
    {
        "code": [%(codes)s],
        "quantity": [%(quantities)s],
        "rate": [%(rates)s],
        "is_active": [%(flags)s],
        "valid_from": [%(dates)s],
    }
)
labels["code"] = labels["code"].astype("string")
labels["quantity"] = labels["quantity"].astype("Int64")
labels["valid_from"] = pd.to_datetime(labels["valid_from"])

trouve = SeedTrouve(
    dataframe=labels,
    docs="The tax rate of each country.",
    columns=[Column(name="code", type=ColumnType.STRING)],
)
'''

THREE_ROWS = {
    "codes": '"US", "FR", "JP"',
    "quantities": "10, 20, None",
    "rates": "0.0, 0.20, 0.10",
    "flags": "True, False, True",
    "dates": '"2024-01-01", "2024-01-01", "2025-04-01"',
}

FOUR_ROWS = {
    "codes": '"US", "FR", "JP", "DE"',
    "quantities": "10, 20, None, 40",
    "rates": "0.0, 0.20, 0.10, 0.19",
    "flags": "True, False, True, True",
    "dates": '"2024-01-01", "2024-01-01", "2025-04-01", "2026-01-01"',
}


def _downstream_file(database_name: str) -> str:
    """Give a SQL Trouve that reads the seed.

    The inner f-string belongs to the file that this function writes, thus the
    outer string gives it two braces.
    """
    return (
        f"from {database_name}.reference.labels import trouve as labels\n"
        "\n"
        "from clair import Trouve\n"
        "\n"
        "trouve = Trouve(\n"
        '    sql=f"SELECT code, quantity FROM {labels} WHERE is_active",\n'
        ")\n"
    )


def seed_address(database_name: str, schema_name: str) -> TrouveAddress:
    """Give the address that the test routing entry makes for the seed."""
    return physical_address(f"{database_name}.reference.labels", schema_name)


def downstream_address(database_name: str, schema_name: str) -> TrouveAddress:
    """Give the address of the Trouve that reads the seed."""
    return physical_address(f"{database_name}.derived.active_codes", schema_name)


def write_seed_project(
    destination: Path,
    database_name: str,
    rows: dict[str, str] | None = None,
) -> Path:
    """Write a project with one seed and one Trouve that reads it.

    The structure is:
        <database_name>/reference/labels.py        [a SeedTrouve]
        <database_name>/derived/active_codes.py    [a Trouve] reads the seed

    Args:
        destination: The directory that receives the project.
        database_name: The first part of each logical address. Give a different
            name to each test, thus the tables of the tests stay apart.
        rows: The rows of the seed. THREE_ROWS by default.

    Returns:
        The path of the project root.
    """
    project_path = destination / database_name
    database_path = project_path / database_name
    for schema_name in ("reference", "derived"):
        (database_path / schema_name).mkdir(parents=True, exist_ok=True)

    (project_path / "__routing__.py").write_text(CI_ROUTING_FILE)
    (database_path / "__database_config__.py").write_text(CI_DATABASE_CONFIG_FILE)
    write_seed_file(project_path, database_name, rows or THREE_ROWS)
    (database_path / "derived" / "active_codes.py").write_text(
        _downstream_file(database_name)
    )

    return project_path


def write_seed_file(
    project_path: Path, database_name: str, rows: dict[str, str]
) -> None:
    """Write the seed file of one project, with the given rows."""
    seed_path = project_path / database_name / "reference" / "labels.py"
    seed_path.write_text(SEED_FILE % rows)


def column_types(adapter: SnowflakeAdapter, address: TrouveAddress) -> dict[str, str]:
    """Give the Snowflake type of each column of one table.

    The names in INFORMATION_SCHEMA are upper case, thus this helper gives the
    lower case name of each column back.
    """
    rows = query_rows(
        adapter,
        "SELECT COLUMN_NAME, DATA_TYPE FROM "
        f"{address.database_name}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{address.schema_name.upper()}' "
        f"AND TABLE_NAME = '{address.table_name.upper()}'",
    )
    return {str(name).lower(): str(data_type).upper() for name, data_type in rows}


class TestASeedReachesSnowflake:
    """One run writes the rows of the file, and a Trouve reads them."""

    DATABASE_NAME = "seed_basic_database"

    @pytest.fixture(scope="class")
    def completed_run(
        self,
        clair_environment: IntegrationConfig,
        adapter: SnowflakeAdapter,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> RunSummary:
        """Run the seed project one time."""
        destination = tmp_path_factory.mktemp(self.DATABASE_NAME)
        return clair.run(write_seed_project(destination, self.DATABASE_NAME))

    def test_the_run_succeeds(self, completed_run: RunSummary) -> None:
        assert completed_run.failed == []
        assert completed_run.succeeded_count == 2

    def test_the_seed_is_a_root_that_clair_builds(
        self, completed_run: RunSummary
    ) -> None:
        """A seed reads no upstream, and clair still builds it."""
        seed_result = completed_run.result(f"{self.DATABASE_NAME}.reference.labels")

        assert seed_result is not None
        assert seed_result.status == RunStatus.SUCCESS

    def test_the_table_holds_each_row_of_the_file(
        self,
        completed_run: RunSummary,
        adapter: SnowflakeAdapter,
        clair_environment: IntegrationConfig,
    ) -> None:
        address = seed_address(self.DATABASE_NAME, clair_environment.schema_name)

        assert row_count(adapter, address) == 3

    def test_the_downstream_trouve_reads_the_seed(
        self,
        completed_run: RunSummary,
        adapter: SnowflakeAdapter,
        clair_environment: IntegrationConfig,
    ) -> None:
        """The SQL Trouve keeps the two rows whose is_active column is true."""
        address = downstream_address(self.DATABASE_NAME, clair_environment.schema_name)

        assert row_count(adapter, address) == 2

    def test_the_seed_leaves_no_staging_object(
        self,
        completed_run: RunSummary,
        adapter: SnowflakeAdapter,
        clair_environment: IntegrationConfig,
    ) -> None:
        """Clair promotes the staging table of a seed, then drops it."""
        remaining = staging_objects(
            adapter,
            clair_environment.schema_name,
            name_prefix=f"{self.DATABASE_NAME}__",
        )

        assert remaining == []


class TestTheDtypeGivesTheSnowflakeType:
    """The central claim of a seed: the dtype decides, and `columns` does not.

    The seed declares `columns` for the `code` column only. Each other column
    still reaches Snowflake with the type of its dtype, which shows that
    `columns` stays documentation.
    """

    DATABASE_NAME = "seed_types_database"

    @pytest.fixture(scope="class")
    def seed_table(
        self,
        clair_environment: IntegrationConfig,
        adapter: SnowflakeAdapter,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> TrouveAddress:
        """Run the project, and give the address of the seed table."""
        destination = tmp_path_factory.mktemp(self.DATABASE_NAME)
        summary = clair.run(write_seed_project(destination, self.DATABASE_NAME))
        assert summary.failed == []
        return seed_address(self.DATABASE_NAME, clair_environment.schema_name)

    def test_a_string_dtype_gives_a_text_column(
        self, adapter: SnowflakeAdapter, seed_table: TrouveAddress
    ) -> None:
        assert column_types(adapter, seed_table)["code"] == "TEXT"

    def test_an_int64_dtype_gives_a_number_column(
        self, adapter: SnowflakeAdapter, seed_table: TrouveAddress
    ) -> None:
        """The nullable Int64 dtype must not become a float or a text column."""
        assert column_types(adapter, seed_table)["quantity"] == "NUMBER"

    def test_a_float_dtype_gives_a_floating_point_column(
        self, adapter: SnowflakeAdapter, seed_table: TrouveAddress
    ) -> None:
        assert column_types(adapter, seed_table)["rate"] == "FLOAT"

    def test_a_bool_dtype_gives_a_boolean_column(
        self, adapter: SnowflakeAdapter, seed_table: TrouveAddress
    ) -> None:
        assert column_types(adapter, seed_table)["is_active"] == "BOOLEAN"

    def test_a_datetime_dtype_gives_a_timestamp_column(
        self, adapter: SnowflakeAdapter, seed_table: TrouveAddress
    ) -> None:
        assert column_types(adapter, seed_table)["valid_from"].startswith("TIMESTAMP")

    def test_an_integer_keeps_no_decimal_part(
        self, adapter: SnowflakeAdapter, seed_table: TrouveAddress
    ) -> None:
        """Plain pandas gives float64 here, and 10 becomes 10.0."""
        rows = query_rows(
            adapter, f"SELECT quantity FROM {seed_table} WHERE code = 'US'"
        )

        assert rows == [(10,)]

    def test_a_null_stays_null(
        self, adapter: SnowflakeAdapter, seed_table: TrouveAddress
    ) -> None:
        rows = query_rows(
            adapter, f"SELECT quantity FROM {seed_table} WHERE code = 'JP'"
        )

        assert rows == [(None,)]


class TestTheFileIsTheData:
    """A seed is a full refresh, and an edit of the file reaches the table."""

    DATABASE_NAME = "seed_refresh_database"

    def test_two_runs_give_the_same_row_count(
        self,
        clair_environment: IntegrationConfig,
        adapter: SnowflakeAdapter,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> None:
        """A second run replaces the table. It must not add the rows again."""
        destination = tmp_path_factory.mktemp(self.DATABASE_NAME)
        project_path = write_seed_project(destination, self.DATABASE_NAME)
        address = seed_address(self.DATABASE_NAME, clair_environment.schema_name)

        clair.run(project_path)
        after_the_first_run = row_count(adapter, address)

        clair.run(project_path)
        after_the_second_run = row_count(adapter, address)

        assert after_the_first_run == 3
        assert after_the_second_run == 3

    def test_a_new_row_in_the_file_reaches_the_table(
        self,
        clair_environment: IntegrationConfig,
        adapter: SnowflakeAdapter,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> None:
        database_name = "seed_edit_database"
        destination = tmp_path_factory.mktemp(database_name)
        project_path = write_seed_project(destination, database_name)
        address = seed_address(database_name, clair_environment.schema_name)

        clair.run(project_path)
        assert row_count(adapter, address) == 3

        write_seed_file(project_path, database_name, FOUR_ROWS)
        clair.run(project_path)

        assert row_count(adapter, address) == 4


class TestASeedWithNoRow:
    """A seed with no row makes a table that holds no row."""

    DATABASE_NAME = "seed_empty_database"

    def test_the_table_exists_and_holds_no_row(
        self,
        clair_environment: IntegrationConfig,
        adapter: SnowflakeAdapter,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> None:
        destination = tmp_path_factory.mktemp(self.DATABASE_NAME)
        project_path = destination / self.DATABASE_NAME
        database_path = project_path / self.DATABASE_NAME
        (database_path / "reference").mkdir(parents=True)

        (project_path / "__routing__.py").write_text(CI_ROUTING_FILE)
        (database_path / "__database_config__.py").write_text(CI_DATABASE_CONFIG_FILE)
        (database_path / "reference" / "labels.py").write_text(textwrap.dedent("""\
            import pandas as pd

            from clair import SeedTrouve

            frame = pd.DataFrame({"code": pd.Series(dtype="string")})

            trouve = SeedTrouve(dataframe=frame)
        """))

        summary = clair.run(project_path)

        assert summary.failed == []
        address = seed_address(self.DATABASE_NAME, clair_environment.schema_name)
        assert row_count(adapter, address) == 0
