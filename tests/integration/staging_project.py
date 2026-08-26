"""Make a small project that shows what staging does.

The example projects pass their data quality tests, which is correct for a
fixture. Staging is about the run that **fails**, so these tests need a project
whose test fails on demand.

The project holds three Trouves:

    <database_name>.source.rows        A SOURCE. The test makes this table.
    <database_name>.refined.checked    The candidate, with one TestRowCount.
    <database_name>.derived.downstream A TABLE that reads refined.checked.

``minimum_rows`` decides the result. A low number passes, and a number above the
row count of the source fails. The downstream Trouve shows that clair skips each
dependent of a Trouve that failed.

``incremental_mode`` gives the candidate an incremental run mode. An
incremental run adds two steps that a full refresh does not have: a zero copy
clone that seeds the staging table, and, for an UPSERT, a second table that
holds the new rows for the MERGE.

``execution_type`` selects the engine of the candidate. The two engines write in
different ways, and staging must hold for both:

* SNOWFLAKE runs ``CREATE OR REPLACE TABLE`` at the staging address.
* PANDAS calls ``write_pandas``, and that function makes the staging table
  itself. Clair then promotes that table with a clone.

Each test gives its own ``database_name``, thus two tests never write one table
in the schema of the run.

Each function below gives the text of one file. The file that the function
writes holds an f-string of its own, and ``{{source_rows}}`` is that inner
f-string. The outer f-string gives one pair of braces to the file.
"""

from __future__ import annotations

from pathlib import Path

from clair import TrouveAddress
from clair.adapters.snowflake import SnowflakeAdapter
from clair.trouves.run_config import IncrementalMode
from clair.trouves.trouve import MERGE_SUFFIX, ExecutionType
from tests.integration.projects import (
    CI_DATABASE_CONFIG_FILE,
    CI_ROUTING_FILE,
    physical_address,
)
from tests.integration.warehouse import execute


def source_address(database_name: str, schema_name: str) -> TrouveAddress:
    """Give the address of the SOURCE table of one probe project."""
    return physical_address(f"{database_name}.source.rows", schema_name)


def checked_address(database_name: str, schema_name: str) -> TrouveAddress:
    """Give the address of the candidate, which holds the data quality test."""
    return physical_address(f"{database_name}.refined.checked", schema_name)


def downstream_address(database_name: str, schema_name: str) -> TrouveAddress:
    """Give the address of the dependent Trouve."""
    return physical_address(f"{database_name}.derived.downstream", schema_name)


def merge_source_address(
    physical: TrouveAddress, run_id: str, schema_name: str
) -> TrouveAddress:
    """Give the address of the table that the MERGE of an UPSERT reads.

    This table holds the new rows. It is not the staging address, and an
    incremental UPSERT run makes both.
    """
    return TrouveAddress(
        database_name=physical.database_name,
        schema_name=schema_name,
        table_name=f"{physical.table_name}{MERGE_SUFFIX}{run_id}",
    )


def make_source_rows(
    adapter: SnowflakeAdapter,
    database_name: str,
    schema_name: str,
    amount_of: dict[str, int],
) -> None:
    """Make the SOURCE table of one probe project again, with these rows.

    Args:
        adapter: An open connection.
        database_name: The database name of the probe project.
        schema_name: The schema of the run.
        amount_of: The amount of each row id. A row id that repeats needs a
            list, so a caller that wants a duplicate calls insert_source_row.
    """
    address = source_address(database_name, schema_name)
    execute(adapter, f"create or replace table {address} (id string, amount number)")
    values = ", ".join(
        f"('{row_id}', {amount})" for row_id, amount in amount_of.items()
    )
    execute(adapter, f"insert into {address} values {values}")


def insert_source_row(
    adapter: SnowflakeAdapter,
    database_name: str,
    schema_name: str,
    row_id: str,
    amount: int,
) -> None:
    """Add one row to the SOURCE table. The row id may exist already."""
    address = source_address(database_name, schema_name)
    execute(adapter, f"insert into {address} values ('{row_id}', {amount})")


def source_file() -> str:
    """Give the SOURCE Trouve. The test makes the table that it names.

    ``amount`` gives an UPSERT something to change. A full refresh candidate
    reads ``id`` only.
    """
    return '''\
from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.SOURCE,
    docs="The rows that the test inserts before it runs clair.",
    columns=[
        Column(name="id", type=ColumnType.STRING),
        Column(name="amount", type=ColumnType.NUMBER),
    ],
)
'''


def incremental_checked_file(
    database_name: str, minimum_rows: int, incremental_mode: IncrementalMode
) -> str:
    """Give the candidate Trouve that clair materializes incrementally.

    An UPSERT matches on ``id``. An APPEND takes no primary key column, and
    RunConfig rejects one.
    """
    if incremental_mode == IncrementalMode.UPSERT:
        primary_key_line = '        primary_key_columns=["id"],\n'
    elif incremental_mode == IncrementalMode.APPEND:
        primary_key_line = ""
    else:
        raise ValueError(f"The probe project has no Trouve for {incremental_mode}.")

    return f'''\
from {database_name}.source.rows import trouve as source_rows

from clair import (
    Column,
    ColumnType,
    IncrementalMode,
    RunConfig,
    RunMode,
    TestRowCount,
    Trouve,
    TrouveType,
)

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="The candidate of an incremental run. TestRowCount decides it.",
    sql=f"""
        select id, amount from {{source_rows}}
    """,
    columns=[
        Column(name="id", type=ColumnType.STRING),
        Column(name="amount", type=ColumnType.NUMBER),
    ],
    run_config=RunConfig(
        run_mode=RunMode.INCREMENTAL,
        incremental_mode=IncrementalMode.{incremental_mode.name},
{primary_key_line}    ),
    tests=[TestRowCount(min_rows={minimum_rows})],
)
'''


def sql_checked_file(database_name: str, minimum_rows: int) -> str:
    """Give the candidate Trouve that Snowflake materializes from SQL."""
    return f'''\
from {database_name}.source.rows import trouve as source_rows

from clair import Column, ColumnType, TestRowCount, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="The candidate. TestRowCount decides if clair promotes it.",
    sql=f"""
        select id from {{source_rows}}
    """,
    columns=[
        Column(name="id", type=ColumnType.STRING),
    ],
    tests=[TestRowCount(min_rows={minimum_rows})],
)
'''


def pandas_checked_file(database_name: str, minimum_rows: int) -> str:
    """Give the candidate Trouve that a transform function gives."""
    return f'''\
import pandas as pd
from {database_name}.source.rows import trouve as source_rows

from clair import Column, ColumnType, PandasTrouve, TestRowCount


def keep_each_row(rows: pd.DataFrame) -> pd.DataFrame:
    return rows


trouve = PandasTrouve(
    transform=keep_each_row,
    inputs=[source_rows],
    docs="The candidate that write_pandas makes. TestRowCount decides it.",
    columns=[
        Column(name="id", type=ColumnType.STRING),
    ],
    tests=[TestRowCount(min_rows={minimum_rows})],
)
'''


def downstream_file(database_name: str) -> str:
    """Give the dependent Trouve. Clair skips it after the candidate fails."""
    return f'''\
from {database_name}.refined.checked import trouve as refined_checked

from clair import Column, ColumnType, Trouve, TrouveType

trouve = Trouve(
    type=TrouveType.TABLE,
    docs="A dependent. Clair skips it when the test of refined.checked fails.",
    sql=f"""
        select id from {{refined_checked}}
    """,
    columns=[
        Column(name="id", type=ColumnType.STRING),
    ],
)
'''


def write_probe_project(
    destination: Path,
    database_name: str,
    minimum_rows: int,
    execution_type: ExecutionType = ExecutionType.SNOWFLAKE,
    incremental_mode: IncrementalMode | None = None,
) -> Path:
    """Write the project, and give the path of its root.

    Args:
        destination: The directory that receives the project.
        database_name: The first part of each logical address. Give a different
            name to each test, thus the tables of the tests stay apart.
        minimum_rows: The limit of TestRowCount on the refined Trouve.
        execution_type: SNOWFLAKE for a SQL candidate, PANDAS for a candidate
            that a transform function gives.
        incremental_mode: APPEND or UPSERT for an incremental candidate. None
            gives a full refresh candidate. A PandasTrouve always makes the
            table again, so the two arguments do not go together.

    Returns:
        The path of the project root.

    Raises:
        ValueError: If the execution type has no Trouve here, or if a pandas
            candidate asks for an incremental mode.
    """
    if incremental_mode is not None and execution_type == ExecutionType.PANDAS:
        raise ValueError("A pandas candidate has no incremental mode.")

    project_path = destination / database_name
    database_path = project_path / database_name
    for schema_name in ("source", "refined", "derived"):
        (database_path / schema_name).mkdir(parents=True, exist_ok=True)

    (project_path / "__routing__.py").write_text(CI_ROUTING_FILE)
    (database_path / "__database_config__.py").write_text(CI_DATABASE_CONFIG_FILE)

    checked_file = None
    if incremental_mode is not None:
        checked_file = incremental_checked_file(
            database_name, minimum_rows, incremental_mode
        )
    elif execution_type == ExecutionType.PANDAS:
        checked_file = pandas_checked_file(database_name, minimum_rows)
    elif execution_type == ExecutionType.SNOWFLAKE:
        checked_file = sql_checked_file(database_name, minimum_rows)
    else:
        raise ValueError(f"The probe project has no Trouve for {execution_type}.")

    (database_path / "source" / "rows.py").write_text(source_file())
    (database_path / "refined" / "checked.py").write_text(checked_file)
    (database_path / "derived" / "downstream.py").write_text(
        downstream_file(database_name)
    )
    return project_path
